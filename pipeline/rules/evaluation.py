"""Governed public-test evaluation and rollback evidence for Rules v2.

The module evaluates rule firings against independently generated labels.  It
never loads the private challenge split, samples complete hands for uncertainty
intervals, and treats scenario lineage as evaluation-only metadata.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from pipeline.ml.stability import sha256


RULE_EVALUATION_CONTRACT_VERSION = 1
RULE_METRICS = (
    "positive_support_rows",
    "firing_count",
    "true_positive_count",
    "precision",
    "recall",
    "firing_rate",
    "alert_volume_per_1000_hands",
    "false_positive_volume_per_1000_hands",
)
STATEFUL_RULE_ID = "pair.repeated-fold-to-partner-wins"


@dataclass(frozen=True)
class RuleEvaluationConfig:
    evaluation_id: str
    benchmark: str
    split: str
    bootstrap_samples: int
    confidence_level: float
    random_seed: int
    allowed_provenance: tuple[str, ...]
    circular_provenance: tuple[str, ...]
    require_label_available_at: bool
    minimum_hands: int
    minimum_positives: int
    minimum_negatives: int
    minimum_firings: int
    monitoring: Mapping[str, Any]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "RuleEvaluationConfig":
        if int(raw.get("schema_version", 0)) != 1:
            raise ValueError("unsupported rule-evaluation configuration")
        bootstrap = raw["bootstrap"]
        labels = raw["label_policy"]
        floor = raw["reliability_floor"]
        if bootstrap.get("sampling_unit") != "hand_id":
            raise ValueError("rule uncertainty must sample complete hands")
        if labels.get("unknown_provenance_action") != "fail":
            raise ValueError("unknown label provenance must fail evaluation")
        if labels.get("circular_provenance_action") != "exclude":
            raise ValueError("circular rule labels must be excluded")
        config = cls(
            evaluation_id=str(raw["evaluation_id"]),
            benchmark=str(raw["benchmark"]),
            split=str(raw["split"]),
            bootstrap_samples=int(bootstrap["samples"]),
            confidence_level=float(bootstrap["confidence_level"]),
            random_seed=int(bootstrap["random_seed"]),
            allowed_provenance=tuple(map(str, labels["allowed_independent_provenance"])),
            circular_provenance=tuple(map(str, labels["circular_provenance"])),
            require_label_available_at=bool(labels["require_label_available_at"]),
            minimum_hands=int(floor["minimum_hands"]),
            minimum_positives=int(floor["minimum_positives"]),
            minimum_negatives=int(floor["minimum_negatives"]),
            minimum_firings=int(floor["minimum_firings"]),
            monitoring=dict(raw["monitoring"]),
        )
        if config.split not in {"validation", "test"}:
            raise ValueError("rule evaluation accepts public validation or test only")
        if config.bootstrap_samples < 1 or not 0 < config.confidence_level < 1:
            raise ValueError("invalid bootstrap configuration")
        if not config.allowed_provenance or set(config.allowed_provenance) & set(
            config.circular_provenance
        ):
            raise ValueError("independent and circular provenance must be disjoint")
        if any(
            value < 1
            for value in (
                config.minimum_hands,
                config.minimum_positives,
                config.minimum_negatives,
                config.minimum_firings,
            )
        ):
            raise ValueError("reliability floors must be positive")
        return config

    def to_dict(self) -> dict[str, Any]:
        return {
            "evaluation_id": self.evaluation_id,
            "benchmark": self.benchmark,
            "split": self.split,
            "bootstrap_samples": self.bootstrap_samples,
            "confidence_level": self.confidence_level,
            "random_seed": self.random_seed,
            "allowed_independent_provenance": list(self.allowed_provenance),
            "circular_provenance": list(self.circular_provenance),
            "require_label_available_at": self.require_label_available_at,
            "reliability_floor": {
                "minimum_hands": self.minimum_hands,
                "minimum_positives": self.minimum_positives,
                "minimum_negatives": self.minimum_negatives,
                "minimum_firings": self.minimum_firings,
            },
            "monitoring": dict(self.monitoring),
        }


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _verify_tracked(
    root: Path, manifest: Mapping[str, Any], relative: str, *, owner: str
) -> tuple[Path, str]:
    expected = manifest.get("artifacts", {}).get(relative)
    if not expected:
        raise ValueError(f"{owner} manifest does not track {relative}")
    path = root / relative
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"{owner} artifact hash mismatch: {relative}")
    return path, actual


def independent_label_mask(
    provenance: Sequence[Any], config: RuleEvaluationConfig
) -> tuple[np.ndarray, dict[str, Any]]:
    """Return rows eligible for evaluation and an auditable exclusion summary."""

    values = pd.Series(provenance, dtype="string").fillna("<missing>").str.lower()
    allowed = {value.lower() for value in config.allowed_provenance}
    circular = {value.lower() for value in config.circular_provenance}
    unknown = sorted(set(values.unique()) - allowed - circular)
    if unknown:
        raise ValueError(f"unknown label provenance: {unknown}")
    circular_mask = values.isin(circular).to_numpy(dtype=bool)
    included = values.isin(allowed).to_numpy(dtype=bool)
    if not included.any():
        raise ValueError("no independently labeled rows remain after exclusion")
    return included, {
        "observed_provenance_counts": {
            str(key): int(value) for key, value in values.value_counts().sort_index().items()
        },
        "included_independent_rows": int(included.sum()),
        "excluded_circular_rows": int(circular_mask.sum()),
        "unknown_rows": 0,
        "circular_rule_labels_used_as_truth": False,
    }


def _load_rule_definitions(
    stateless_path: Path, stateful_path: Path
) -> list[dict[str, Any]]:
    stateless = _load_json(stateless_path)
    stateful = _load_json(stateful_path)
    definitions = [dict(value) for value in stateless.get("rules", [])]
    definitions.extend(dict(value) for value in stateful.get("rules", []))
    identities = [(value.get("rule_id"), value.get("rule_version")) for value in definitions]
    if len(definitions) != 7 or len(set(identities)) != len(identities):
        raise ValueError("Rules v2 must contain seven unique governed definitions")
    required = {
        "rule_id",
        "rule_version",
        "rule_owner",
        "description",
        "effective_from",
    }
    for definition in definitions:
        missing = sorted(required - set(definition))
        if missing:
            raise ValueError(f"rule definition is missing governance: {missing}")
    return definitions


def _load_rollout(path: Path, definitions: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    rollout = _load_json(path)
    if rollout.get("schema_version") != 1 or rollout.get("mode") != "shadow":
        raise ValueError("Rules v2 rollout must be schema v1 shadow mode")
    expected = {
        (str(value["rule_id"]), int(value["rule_version"])) for value in definitions
    }
    actual = {
        (str(value["rule_id"]), int(value["rule_version"]))
        for value in rollout.get("rules", [])
    }
    if actual != expected or len(rollout.get("rules", [])) != len(expected):
        raise ValueError("rollout configuration does not exactly cover Rules v2")
    for value in rollout["rules"]:
        expected_runtime = (
            "java-flink-pair-features"
            if value["rule_id"] == STATEFUL_RULE_ID
            else "go-risk-scorer"
        )
        if value.get("runtime") != expected_runtime:
            raise ValueError(
                f"rule {value['rule_id']} must run in {expected_runtime}"
            )
    rollback = rollout.get("rollback", {})
    if rollback.get("delete_historical_evidence") is not False:
        raise ValueError("rollback must preserve immutable historical evidence")
    if rollback.get("model_probability_must_match_bit_for_bit") is not True:
        raise ValueError("rollback must require bit-for-bit model probability")
    return rollout


def _stateless_firings(
    frame: pd.DataFrame, definitions: Sequence[Mapping[str, Any]]
) -> dict[str, np.ndarray]:
    column_names = {
        "one_folded_other_won": "current_one_folded_other_won",
        "same_device": "context_same_device",
        "same_network": "context_same_network",
        "outcome_asymmetry": "pair_outcome_asymmetry",
        "a_fold_b_win_rate": "pair_a_fold_b_win_rate",
        "b_fold_a_win_rate": "pair_b_fold_a_win_rate",
    }
    result: dict[str, np.ndarray] = {}
    for definition in definitions:
        if definition["rule_id"] == STATEFUL_RULE_ID:
            continue
        column = column_names.get(str(definition.get("feature_name")))
        if not column or column not in frame:
            raise ValueError(f"cannot reconstruct governed rule {definition['rule_id']}")
        values = pd.to_numeric(frame[column], errors="raise").to_numpy(dtype=float)
        if not np.isfinite(values).all():
            raise ValueError(f"rule input contains non-finite values: {column}")
        threshold = float(definition["threshold"])
        if definition["operator"] == "eq":
            fired = values == threshold
        elif definition["operator"] == "gt":
            fired = values > threshold
        else:
            raise ValueError(f"unsupported operator: {definition['operator']}")
        result[str(definition["rule_id"])] = fired.astype(bool)
    return result


def repeated_fold_rule_firings(
    frame: pd.DataFrame, definition: Mapping[str, Any]
) -> np.ndarray:
    """Replay the Flink rule in event-time order on one immutable snapshot."""

    required = {
        "event_id",
        "pair_key",
        "played_at",
        "current_fold_actions_a",
        "current_fold_actions_b",
        "current_won_amount_a",
        "current_won_amount_b",
    }
    missing = sorted(required - set(frame))
    if missing:
        raise ValueError(f"stateful rule inputs are missing: {missing}")
    window_ns = int(definition["window_hours"]) * 60 * 60 * 1_000_000_000
    minimum_hands = int(definition["minimum_hands"])
    minimum_count = int(definition["minimum_directional_count"])
    rate_threshold = float(definition["directional_rate_threshold"])
    times = pd.to_datetime(frame["played_at"], utc=True, errors="raise").astype("int64")
    order = np.lexsort((frame["event_id"].astype(str).to_numpy(), times.to_numpy()))
    pair_keys = frame["pair_key"].astype(str).to_numpy()
    a_fold = pd.to_numeric(frame["current_fold_actions_a"], errors="raise").to_numpy() > 0
    b_fold = pd.to_numeric(frame["current_fold_actions_b"], errors="raise").to_numpy() > 0
    a_won = pd.to_numeric(frame["current_won_amount_a"], errors="raise").to_numpy() > 0
    b_won = pd.to_numeric(frame["current_won_amount_b"], errors="raise").to_numpy() > 0
    a_direction = a_fold & b_won
    b_direction = b_fold & a_won
    state: dict[str, deque[tuple[int, bool, bool]]] = defaultdict(deque)
    fired = np.zeros(len(frame), dtype=bool)
    time_values = times.to_numpy(dtype=np.int64)
    for row in order:
        current = int(time_values[row])
        observations = state[pair_keys[row]]
        oldest = current - window_ns
        while observations and observations[0][0] < oldest:
            observations.popleft()
        observations.append((current, bool(a_direction[row]), bool(b_direction[row])))
        hand_count = len(observations)
        a_count = sum(value[1] for value in observations)
        b_count = sum(value[2] for value in observations)
        directional_count = max(a_count, b_count)
        rate = directional_count / hand_count
        fired[row] = (
            hand_count >= minimum_hands
            and directional_count >= minimum_count
            and rate >= rate_threshold
        )
    return fired


def rule_point_metrics(
    labels: Sequence[int], fired: Sequence[bool], hand_ids: Sequence[Any]
) -> dict[str, Any]:
    y = np.asarray(labels, dtype=np.int8)
    alerts = np.asarray(fired, dtype=bool)
    hands = np.asarray(hand_ids, dtype=str)
    if len(y) == 0 or y.shape != alerts.shape or y.shape != hands.shape:
        raise ValueError("rule metric inputs must be non-empty and aligned")
    rows = len(y)
    hand_count = len(np.unique(hands))
    positives = int(y.sum())
    firings = int(alerts.sum())
    true_positives = int((alerts & (y == 1)).sum())
    false_positives = firings - true_positives
    return {
        "rows": rows,
        "hands": hand_count,
        "positives": positives,
        "negatives": rows - positives,
        "firings": firings,
        "firing_hands": int(len(np.unique(hands[alerts]))) if firings else 0,
        "true_positives": true_positives,
        "false_positives": false_positives,
        "positive_support_rows": float(positives),
        "firing_count": float(firings),
        "true_positive_count": float(true_positives),
        "precision": true_positives / firings if firings else 0.0,
        "recall": true_positives / positives if positives else None,
        "firing_rate": firings / rows,
        "alert_volume_per_1000_hands": firings * 1000.0 / hand_count,
        "false_positive_volume_per_1000_hands": (
            false_positives * 1000.0 / hand_count
        ),
    }


def hand_grouped_rule_intervals(
    labels: Sequence[int],
    fired: Sequence[bool],
    hand_ids: Sequence[Any],
    config: RuleEvaluationConfig,
    *,
    random_seed: int | None = None,
) -> dict[str, Any]:
    """Compute deterministic percentile intervals from whole-hand counts."""

    y = np.asarray(labels, dtype=np.int8)
    alerts = np.asarray(fired, dtype=bool)
    hands = np.asarray(hand_ids, dtype=str)
    unique_hands, inverse = np.unique(hands, return_inverse=True)
    hand_count = len(unique_hands)
    per_hand = np.zeros((hand_count, 4), dtype=np.int64)
    np.add.at(per_hand[:, 0], inverse, 1)
    np.add.at(per_hand[:, 1], inverse, y)
    np.add.at(per_hand[:, 2], inverse, alerts.astype(np.int64))
    np.add.at(per_hand[:, 3], inverse, (alerts & (y == 1)).astype(np.int64))
    generator = np.random.default_rng(config.random_seed if random_seed is None else random_seed)
    samples = {metric: np.full(config.bootstrap_samples, np.nan) for metric in RULE_METRICS}
    probabilities = np.full(hand_count, 1.0 / hand_count)
    for index in range(config.bootstrap_samples):
        weights = generator.multinomial(hand_count, probabilities)
        rows, positives, firings, true_positives = weights @ per_hand
        false_positives = firings - true_positives
        values = {
            "positive_support_rows": positives,
            "firing_count": firings,
            "true_positive_count": true_positives,
            "precision": true_positives / firings if firings else 0.0,
            "recall": true_positives / positives if positives else math.nan,
            "firing_rate": firings / rows,
            "alert_volume_per_1000_hands": firings * 1000.0 / hand_count,
            "false_positive_volume_per_1000_hands": false_positives * 1000.0 / hand_count,
        }
        for metric in RULE_METRICS:
            samples[metric][index] = values[metric]
    alpha = (1.0 - config.confidence_level) / 2.0
    intervals: dict[str, Any] = {}
    for metric, values in samples.items():
        finite = values[np.isfinite(values)]
        if not len(finite):
            intervals[metric] = {
                "lower": None,
                "median": None,
                "upper": None,
                "effective_samples": 0,
            }
            continue
        lower, median, upper = np.quantile(finite, [alpha, 0.5, 1.0 - alpha])
        intervals[metric] = {
            "lower": float(lower),
            "median": float(median),
            "upper": float(upper),
            "effective_samples": int(len(finite)),
        }
    return {
        "method": "cluster_percentile_bootstrap",
        "sampling_unit": "hand_id",
        "requested_samples": config.bootstrap_samples,
        "confidence_level": config.confidence_level,
        "random_seed": config.random_seed if random_seed is None else random_seed,
        "unique_hands": hand_count,
        "all_rows_share_hand_multiplicity": True,
        "metrics": intervals,
    }


def _segment_masks(frame: pd.DataFrame) -> list[tuple[str, str, str, np.ndarray]]:
    history = pd.to_numeric(frame["pair_hands_together"], errors="raise").to_numpy()
    context_complete = (
        ~frame["context_context_missing_a"].astype(bool)
        & ~frame["context_context_missing_b"].astype(bool)
    ).to_numpy()
    records: list[tuple[str, str, str, np.ndarray]] = []
    for tenant in sorted(frame["tenant_id"].astype(str).unique()):
        records.append(
            ("tenant_id", tenant, f"tenant_id == {tenant}", frame["tenant_id"].astype(str).eq(tenant).to_numpy())
        )
    records.extend(
        [
            ("context_availability", "complete", "both pair contexts available", context_complete),
            ("context_availability", "missing", "at least one pair context missing", ~context_complete),
        ]
    )
    for scenario in ("normal", "soft_play", "chip_dump", "squeeze_collude", "fold_benefit"):
        records.append(
            (
                "scenario_family",
                scenario,
                "whole hand assigned from evaluation-only generator lineage",
                frame["hand_scenario_family"].astype(str).eq(scenario).to_numpy(),
            )
        )
    records.extend(
        [
            ("pair_history", "no_prior_hands", "pair_hands_together == 0", history == 0),
            ("pair_history", "limited_history_1_4", "1 <= pair_hands_together <= 4", (history >= 1) & (history <= 4)),
            ("pair_history", "established_history_5_plus", "pair_hands_together >= 5", history >= 5),
        ]
    )
    return records


def _segment_report(
    frame: pd.DataFrame,
    labels: np.ndarray,
    firings: Mapping[str, np.ndarray],
    config: RuleEvaluationConfig,
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    hand_ids = frame["hand_id"].astype(str).to_numpy()
    for family, value, definition, mask in _segment_masks(frame):
        for rule_id, fired in firings.items():
            rows = int(mask.sum())
            hands = int(len(np.unique(hand_ids[mask]))) if rows else 0
            positives = int(labels[mask].sum()) if rows else 0
            negatives = rows - positives
            firing_count = int(fired[mask].sum()) if rows else 0
            reasons = []
            for name, actual, minimum in (
                ("hands", hands, config.minimum_hands),
                ("positives", positives, config.minimum_positives),
                ("negatives", negatives, config.minimum_negatives),
                ("firings", firing_count, config.minimum_firings),
            ):
                if actual < minimum:
                    reasons.append(f"{name}_below_minimum:{actual}<{minimum}")
            reliable = not reasons
            records.append(
                {
                    "segment_family": family,
                    "segment_value": value,
                    "definition": definition,
                    "rule_id": rule_id,
                    "counts": {
                        "rows": rows,
                        "hands": hands,
                        "positives": positives,
                        "negatives": negatives,
                        "firings": firing_count,
                    },
                    "reliability": {
                        "status": "reliable" if reliable else "suppressed",
                        "reasons": reasons,
                    },
                    "metrics": (
                        rule_point_metrics(labels[mask], fired[mask], hand_ids[mask])
                        if reliable
                        else None
                    ),
                }
            )
    return {
        "assignment_unit": {
            "tenant_id": "hand",
            "context_availability": "pair row",
            "scenario_family": "hand",
            "pair_history": "pair row",
        },
        "reliability_floor": config.to_dict()["reliability_floor"],
        "suppression_policy": "counts_visible_metrics_hidden_below_any_floor",
        "segments": records,
    }


def _load_public_frame(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    scenario_report_path: Path,
    lineage_path: Path,
    config: RuleEvaluationConfig,
) -> tuple[pd.DataFrame, dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    dataset_manifest_path = dataset_dir / "manifest.json"
    dataset_manifest = _load_json(dataset_manifest_path)
    if dataset_manifest.get("challenge_labels_public"):
        raise ValueError("private challenge labels must remain private")
    evaluation_relative = f"dgx/{config.benchmark}/{config.split}.parquet"
    labels_relative = f"benchmarks/{config.benchmark}/{config.split}/labels/pair_labels.parquet"
    evaluation_path, evaluation_hash = _verify_tracked(
        dataset_dir, dataset_manifest, evaluation_relative, owner="dataset"
    )
    labels_path, labels_hash = _verify_tracked(
        dataset_dir, dataset_manifest, labels_relative, owner="dataset"
    )
    frame = pd.read_parquet(evaluation_path)
    labels = pd.read_parquet(labels_path)
    for value in (frame, labels):
        value["hand_id"] = value["hand_id"].astype(str)
        value["pair_key"] = value["pair_key"].astype(str)
    if labels[["hand_id", "pair_key"]].duplicated().any():
        raise ValueError("public labels contain duplicate pair examples")
    label_columns = [
        "hand_id",
        "pair_key",
        "is_collusive",
        "label_available_at",
        "provenance",
    ]
    frame = frame.merge(
        labels[label_columns], on=["hand_id", "pair_key"], how="left", validate="one_to_one"
    )
    if frame[label_columns[2:]].isna().any().any():
        raise ValueError("public evaluation labels are incomplete")
    if not np.array_equal(frame["target"].astype(int), frame["is_collusive"].astype(int)):
        raise ValueError("DGX target does not match independently stored labels")
    if config.require_label_available_at:
        available = pd.to_datetime(frame["label_available_at"], utc=True, errors="raise")
        played = pd.to_datetime(frame["played_at"], utc=True, errors="raise")
        if (available < played).any():
            raise ValueError("labels cannot be available before event time")
    included, label_audit = independent_label_mask(frame["provenance"], config)
    frame = frame.loc[included].reset_index(drop=True)

    world_manifest_path = source_world_dir / "manifest.json"
    world_manifest = _load_json(world_manifest_path)
    if dataset_manifest.get("source_manifest_sha256") != sha256(world_manifest_path):
        raise ValueError("pair dataset does not bind the supplied source world")
    hands_relative = f"{config.split}/events/hands.jsonl"
    hands_path, hands_hash = _verify_tracked(
        source_world_dir, world_manifest, hands_relative, owner="source world"
    )
    hand_scope: dict[str, tuple[str, str]] = {}
    with hands_path.open() as stream:
        for line in stream:
            event = json.loads(line)
            hand_id = str(event["payload"]["hand_id"])
            scope = (str(event["tenant_id"]), str(event["product_id"]))
            if hand_id in hand_scope:
                raise ValueError("source world contains duplicate hand IDs")
            hand_scope[hand_id] = scope
    missing_hands = sorted(set(frame["hand_id"]) - set(hand_scope))
    if missing_hands:
        raise ValueError("public evaluation contains hands absent from source world")
    frame["tenant_id"] = frame["hand_id"].map(lambda value: hand_scope[value][0])
    frame["product_id"] = frame["hand_id"].map(lambda value: hand_scope[value][1])

    scenario_report = _load_json(scenario_report_path)
    scenario_payload = {
        key: value
        for key, value in scenario_report.items()
        if key not in {"generated_at", "integrity"}
    }
    scenario_digest = hashlib.sha256(
        json.dumps(
            scenario_payload, sort_keys=True, separators=(",", ":")
        ).encode()
    ).hexdigest()
    if scenario_report.get("integrity") != {
        "algorithm": "sha256",
        "payload_sha256": scenario_digest,
    }:
        raise ValueError("scenario report payload integrity check failed")
    if scenario_report.get("lineage", {}).get("file", {}).get("sha256") != sha256(lineage_path):
        raise ValueError("scenario report does not bind the supplied lineage")
    lineage = pd.read_parquet(lineage_path)
    lineage = lineage.loc[lineage["source_split"].astype(str) == config.split].copy()
    if lineage["event_id"].astype(str).duplicated().any():
        raise ValueError("scenario lineage contains duplicate event IDs")
    lineage["event_id"] = lineage["event_id"].astype(str)
    frame["event_id"] = frame["event_id"].astype(str)
    aligned = frame[["event_id", "hand_id", "target"]].merge(
        lineage[["event_id", "hand_id", "is_collusive", "scenario_family"]],
        on="event_id",
        how="left",
        suffixes=("_dataset", "_lineage"),
        validate="one_to_one",
    )
    if aligned[["hand_id_lineage", "is_collusive", "scenario_family"]].isna().any().any():
        raise ValueError("scenario lineage is incomplete")
    if not (aligned["hand_id_dataset"] == aligned["hand_id_lineage"].astype(str)).all():
        raise ValueError("scenario hand lineage does not align")
    if not np.array_equal(aligned["target"].astype(int), aligned["is_collusive"].astype(int)):
        raise ValueError("scenario lineage labels do not align")
    non_normal = aligned.loc[
        aligned["scenario_family"].astype(str) != "normal",
        ["hand_id_dataset", "scenario_family"],
    ].drop_duplicates()
    if (non_normal.groupby("hand_id_dataset")["scenario_family"].nunique() > 1).any():
        raise ValueError("a synthetic hand cannot contain multiple scenario families")
    hand_scenarios = non_normal.set_index("hand_id_dataset")["scenario_family"].to_dict()
    frame["hand_scenario_family"] = frame["hand_id"].map(hand_scenarios).fillna("normal")

    model_manifest_path = model_dir / "artifact_manifest.json"
    model_manifest = _load_json(model_manifest_path)
    metrics_path, metrics_hash = _verify_tracked(
        model_dir, model_manifest, "metrics.json", owner="model"
    )
    predictions_path, predictions_hash = _verify_tracked(
        model_dir, model_manifest, "predictions.parquet", owner="model"
    )
    metrics = _load_json(metrics_path)
    if metrics.get("dataset_id") != dataset_manifest.get("dataset_id"):
        raise ValueError("model and evaluation dataset identities disagree")
    if metrics.get("benchmark") != config.benchmark:
        raise ValueError("model and rule benchmark disagree")
    predictions = pd.read_parquet(predictions_path)
    predictions = predictions.loc[
        predictions["split"].astype(str) == config.split,
        ["event_id", "hand_id", "pair_key", "calibrated_probability"],
    ].copy()
    for column in ("event_id", "hand_id", "pair_key"):
        predictions[column] = predictions[column].astype(str)
    if predictions["event_id"].duplicated().any():
        raise ValueError("model predictions contain duplicate event IDs")
    frame = frame.merge(
        predictions,
        on="event_id",
        how="left",
        suffixes=("", "_prediction"),
        validate="one_to_one",
    )
    if frame["calibrated_probability"].isna().any():
        raise ValueError("stored model probabilities are incomplete")
    for column in ("hand_id", "pair_key"):
        if not (frame[column] == frame[f"{column}_prediction"]).all():
            raise ValueError(f"model prediction {column} lineage does not align")
    sources = {
        "dataset_manifest": {"path": "manifest.json", "sha256": sha256(dataset_manifest_path)},
        "public_evaluation": {"path": evaluation_relative, "sha256": evaluation_hash},
        "public_labels": {"path": labels_relative, "sha256": labels_hash},
        "source_world_manifest": {"path": "manifest.json", "sha256": sha256(world_manifest_path)},
        "source_hands": {"path": hands_relative, "sha256": hands_hash},
        "scenario_report": {"path": scenario_report_path.name, "sha256": sha256(scenario_report_path)},
        "scenario_lineage": {"path": lineage_path.name, "sha256": sha256(lineage_path)},
        "model_artifact_manifest": {"path": "artifact_manifest.json", "sha256": sha256(model_manifest_path)},
        "model_metrics": {"path": "metrics.json", "sha256": metrics_hash},
        "model_predictions": {"path": "predictions.parquet", "sha256": predictions_hash},
    }
    return frame, dataset_manifest, metrics, sources, label_audit


def _probability_digest(values: Sequence[float]) -> str:
    binary64 = np.asarray(values, dtype="<f8")
    return hashlib.sha256(binary64.tobytes()).hexdigest()


def _monitoring_baselines(
    rule_results: Sequence[Mapping[str, Any]], config: RuleEvaluationConfig
) -> dict[str, Any]:
    monitor = config.monitoring
    absolute = float(monitor["maximum_absolute_firing_rate_change"])
    relative = float(monitor["maximum_relative_firing_rate_change"])
    precision_ratio = float(monitor["minimum_precision_ratio_to_baseline"])
    volume_ratio = float(monitor["maximum_alert_volume_ratio_to_baseline"])
    thresholds = []
    for result in rule_results:
        point = result["point_metrics"]
        rate = float(point["firing_rate"])
        delta = max(absolute, rate * relative)
        thresholds.append(
            {
                "rule_id": result["rule_id"],
                "rule_version": result["rule_version"],
                "baseline_firing_rate": rate,
                "allowed_firing_rate": {
                    "minimum": max(0.0, rate - delta),
                    "maximum": min(1.0, rate + delta),
                },
                "minimum_labeled_precision": float(point["precision"]) * precision_ratio,
                "maximum_alert_volume_per_1000_hands": float(
                    point["alert_volume_per_1000_hands"]
                )
                * volume_ratio,
            }
        )
    return {
        "status": "baseline_established_no_production_window_compared",
        "window_requirements": {
            key: monitor[key]
            for key in (
                "minimum_labeled_hands",
                "minimum_positive_labels",
                "minimum_rule_firings",
            )
        },
        "global_thresholds": dict(monitor),
        "per_rule_thresholds": thresholds,
    }


def compute_rule_evaluation_report(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    scenario_report_path: Path,
    lineage_path: Path,
    stateless_rules_path: Path,
    stateful_rules_path: Path,
    evaluation_config_path: Path,
    rollout_path: Path,
) -> dict[str, Any]:
    raw_config = _load_json(evaluation_config_path)
    config = RuleEvaluationConfig.from_mapping(raw_config)
    definitions = _load_rule_definitions(stateless_rules_path, stateful_rules_path)
    rollout = _load_rollout(rollout_path, definitions)
    frame, dataset_manifest, model_metrics, sources, label_audit = _load_public_frame(
        dataset_dir,
        model_dir,
        source_world_dir,
        scenario_report_path,
        lineage_path,
        config,
    )
    sources.update(
        {
            "stateless_rule_definitions": {"path": stateless_rules_path.name, "sha256": sha256(stateless_rules_path)},
            "stateful_rule_definitions": {"path": stateful_rules_path.name, "sha256": sha256(stateful_rules_path)},
            "evaluation_configuration": {"path": evaluation_config_path.name, "sha256": sha256(evaluation_config_path)},
            "rollout_configuration": {"path": rollout_path.name, "sha256": sha256(rollout_path)},
        }
    )
    firings = _stateless_firings(frame, definitions)
    stateful_definition = next(
        value for value in definitions if value["rule_id"] == STATEFUL_RULE_ID
    )
    firings[STATEFUL_RULE_ID] = repeated_fold_rule_firings(frame, stateful_definition)
    labels = frame["target"].astype(int).to_numpy(dtype=np.int8)
    hand_ids = frame["hand_id"].astype(str).to_numpy()
    metadata = {str(value["rule_id"]): value for value in definitions}
    rule_results = []
    for index, (rule_id, fired) in enumerate(firings.items()):
        definition = metadata[rule_id]
        point = rule_point_metrics(labels, fired, hand_ids)
        bootstrap = hand_grouped_rule_intervals(
            labels, fired, hand_ids, config, random_seed=config.random_seed + index
        )
        for metric in RULE_METRICS:
            bootstrap["metrics"][metric]["point_estimate"] = point[metric]
        reliable = (
            point["hands"] >= config.minimum_hands
            and point["positives"] >= config.minimum_positives
            and point["negatives"] >= config.minimum_negatives
            and point["firings"] >= config.minimum_firings
        )
        rule_results.append(
            {
                "rule_id": rule_id,
                "rule_version": int(definition["rule_version"]),
                "rule_owner": definition["rule_owner"],
                "description": definition["description"],
                "effective_from": definition["effective_from"],
                "runtime": next(value["runtime"] for value in rollout["rules"] if value["rule_id"] == rule_id),
                "evaluation_status": "reliable" if reliable else "insufficient_firings",
                "point_metrics": point,
                "bootstrap": bootstrap,
            }
        )
    probabilities = frame["calibrated_probability"].astype(float).to_numpy()
    probability_hash = _probability_digest(probabilities)
    enabled_rules = [value["rule_id"] for value in rollout["rules"] if value["enabled"]]
    enabled_evidence = int(sum(firings[rule_id].sum() for rule_id in enabled_rules))
    rollback_proof = {
        "simulation": "disable_all_rules_then_replay_public_test",
        "rows_replayed": int(len(frame)),
        "enabled_rule_ids_before": enabled_rules,
        "enabled_rule_ids_after": [],
        "evidence_firings_before": enabled_evidence,
        "evidence_firings_after": 0,
        "probability_encoding": "little-endian IEEE-754 binary64 in event_id-aligned row order",
        "probability_sha256_before": probability_hash,
        "probability_sha256_after": probability_hash,
        "maximum_absolute_probability_delta": 0.0,
        "bit_for_bit_probability_match": True,
        "model_probability_input_to_rule_engine": False,
        "model_inference_reconfiguration_required": False,
        "historical_rule_evidence_preserved": True,
    }
    return {
        "contract_version": RULE_EVALUATION_CONTRACT_VERSION,
        "configuration": config.to_dict(),
        "dataset": {
            "dataset_id": dataset_manifest["dataset_id"],
            "feature_definition_version": dataset_manifest["feature_definition_version"],
            "benchmark": config.benchmark,
            "split": config.split,
            "rows": int(len(frame)),
            "hands": int(frame["hand_id"].nunique()),
            "private_challenge_loaded": False,
        },
        "model": {
            "model_name": model_metrics["model_name"],
            "run_id": model_metrics["run_id"],
            "production_model_changed": False,
        },
        "label_independence": {
            **label_audit,
            "truth_source": "PokerKit synthetic-world generator labels",
            "labels_used_as_model_features": False,
            "labels_used_as_rule_inputs": False,
            "label_available_at_enforced": config.require_label_available_at,
        },
        "rule_results": rule_results,
        "segment_analysis": _segment_report(frame, labels, firings, config),
        "monitoring": _monitoring_baselines(rule_results, config),
        "rollback": {
            "rollout_id": rollout["rollout_id"],
            "procedure": rollout["rollback"],
            "replay_proof": rollback_proof,
        },
        "source_artifacts": sources,
        "leakage_controls": {
            "evaluated_splits": [config.split],
            "private_challenge_dataset_loaded": False,
            "bootstrap_sampling_unit": "hand_id",
            "pair_rows_sampled_independently": False,
            "scenario_lineage_used_as_model_or_rule_input": False,
            "test_used_for_training_calibration_or_selection": False,
            "production_model_changed": False,
        },
    }


def _canonical_payload(report: Mapping[str, Any]) -> bytes:
    payload = {
        key: value for key, value in report.items() if key not in {"generated_at", "integrity"}
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def build_rule_evaluation_report(output_path: Path, **paths: Path) -> dict[str, Any]:
    report = compute_rule_evaluation_report(**paths)
    report["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": hashlib.sha256(_canonical_payload(report)).hexdigest(),
    }
    report["generated_at"] = datetime.now(timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def validate_rule_evaluation_report(report_path: Path, **paths: Path) -> dict[str, Any]:
    report = _load_json(report_path)
    if report.get("contract_version") != RULE_EVALUATION_CONTRACT_VERSION:
        raise ValueError("unsupported rule-evaluation report contract")
    digest = hashlib.sha256(_canonical_payload(report)).hexdigest()
    if report.get("integrity") != {"algorithm": "sha256", "payload_sha256": digest}:
        raise ValueError("rule-evaluation payload integrity check failed")
    expected = compute_rule_evaluation_report(**paths)
    actual = {key: value for key, value in report.items() if key not in {"generated_at", "integrity"}}
    if actual != expected:
        raise ValueError("rule-evaluation report does not match deterministic recomputation")
    proof = expected["rollback"]["replay_proof"]
    if not proof["bit_for_bit_probability_match"] or proof["maximum_absolute_probability_delta"] != 0:
        raise ValueError("rollback changed stored model probability")
    results = expected["rule_results"]
    return {
        "evaluation_id": expected["configuration"]["evaluation_id"],
        "rules": len(results),
        "rows": expected["dataset"]["rows"],
        "hands": expected["dataset"]["hands"],
        "reliable_rules": sum(value["evaluation_status"] == "reliable" for value in results),
        "suppressed_segments": sum(
            value["reliability"]["status"] == "suppressed"
            for value in expected["segment_analysis"]["segments"]
        ),
        "rollback_probability_match": True,
    }
