"""Generator-seed and leave-one-scenario-family-out CatBoost evidence."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool

from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)
from pipeline.ml.stability import (
    StabilityConfig,
    hand_grouped_bootstrap_intervals,
    sha256,
)


SCENARIO_HOLDOUT_CONTRACT_VERSION = 1
SCENARIO_LINEAGE_VERSION = 1
SCENARIO_FAMILIES = (
    "soft_play",
    "chip_dump",
    "squeeze_collude",
    "fold_benefit",
)
_PAIR_ID_PATTERN = re.compile(
    r"^(?P<dataset>.+)_(?P<split>train|validation|test|challenge)_pair_"
    r"(?P<index>\d{3})$"
)


@dataclass(frozen=True)
class ScenarioHoldoutConfig:
    benchmark: str = "cold_start"
    random_seed: int = 42
    bootstrap_samples: int = 300
    bootstrap_seed: int = 7300
    confidence_level: float = 0.95
    minimum_scenario_positives: int = 10

    def __post_init__(self) -> None:
        if self.benchmark != "cold_start":
            raise ValueError(
                "scenario holdouts currently require the disjoint cold_start benchmark"
            )
        if self.random_seed < 0 or self.bootstrap_seed < 0:
            raise ValueError("random seeds must be non-negative")
        if self.bootstrap_samples < 1:
            raise ValueError("bootstrap_samples must be positive")
        if not 0 < self.confidence_level < 1:
            raise ValueError("confidence_level must be in (0, 1)")
        if self.minimum_scenario_positives < 1:
            raise ValueError("minimum_scenario_positives must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "random_seed": self.random_seed,
            "bootstrap_samples": self.bootstrap_samples,
            "bootstrap_seed": self.bootstrap_seed,
            "confidence_level": self.confidence_level,
            "minimum_scenario_positives": self.minimum_scenario_positives,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ScenarioHoldoutConfig":
        return cls(
            benchmark=str(raw["benchmark"]),
            random_seed=int(raw["random_seed"]),
            bootstrap_samples=int(raw["bootstrap_samples"]),
            bootstrap_seed=int(raw["bootstrap_seed"]),
            confidence_level=float(raw["confidence_level"]),
            minimum_scenario_positives=int(raw["minimum_scenario_positives"]),
        )


def scenario_family_from_pair_id(
    pair_id: str, *, dataset_id: str, split: str
) -> tuple[str, int]:
    """Decode the generator's fixed round-robin pair-pattern assignment."""

    match = _PAIR_ID_PATTERN.fullmatch(str(pair_id))
    if not match:
        raise ValueError(f"unrecognized synthetic collusion pair ID: {pair_id}")
    if match.group("dataset") != dataset_id or match.group("split") != split:
        raise ValueError("collusion pair ID does not match dataset/split lineage")
    pair_index = int(match.group("index"))
    return SCENARIO_FAMILIES[pair_index % len(SCENARIO_FAMILIES)], pair_index


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def _verify_tracked(
    root: Path, manifest: Mapping[str, Any], relative: str, *, owner: str
) -> tuple[Path, str]:
    expected = manifest.get("artifacts", {}).get(relative)
    if expected is None:
        raise ValueError(f"{owner} manifest does not track {relative}")
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"{owner} artifact hash mismatch: {relative}")
    return path, actual


def _verified_sources(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    config: ScenarioHoldoutConfig,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, dict[str, Path]],
]:
    dataset_dir = dataset_dir.resolve()
    model_dir = model_dir.resolve()
    source_world_dir = source_world_dir.resolve()
    dataset_manifest_path = dataset_dir / "manifest.json"
    schema_path = dataset_dir / "schema.json"
    model_manifest_path = model_dir / "artifact_manifest.json"
    world_manifest_path = source_world_dir / "manifest.json"
    dataset_manifest = _load_json(dataset_manifest_path)
    schema = _load_json(schema_path)
    model_manifest = _load_json(model_manifest_path)
    world_manifest = _load_json(world_manifest_path)
    if dataset_manifest.get("challenge_labels_public") or schema.get(
        "challenge_labels_public"
    ):
        raise ValueError("challenge labels must remain private")
    if dataset_manifest.get("source_manifest_sha256") != sha256(world_manifest_path):
        raise ValueError("pair dataset does not bind the supplied source world")
    if dataset_manifest.get("dataset_id") != world_manifest.get("dataset_id"):
        raise ValueError("pair dataset and source world IDs disagree")
    schema_file, schema_hash = _verify_tracked(
        dataset_dir, dataset_manifest, "schema.json", owner="dataset"
    )
    paths: dict[str, dict[str, Path]] = {}
    sources: dict[str, Any] = {
        "dataset_manifest": {
            "path": "manifest.json",
            "sha256": sha256(dataset_manifest_path),
        },
        "dataset_schema": {"path": "schema.json", "sha256": schema_hash},
        "source_world_manifest": {
            "path": "manifest.json",
            "sha256": sha256(world_manifest_path),
        },
        "model_artifact_manifest": {
            "path": "artifact_manifest.json",
            "sha256": sha256(model_manifest_path),
        },
    }
    for split in ("train", "validation", "test"):
        data_relative = f"dgx/{config.benchmark}/{split}.parquet"
        labels_relative = (
            f"benchmarks/{config.benchmark}/{split}/labels/pair_labels.parquet"
        )
        data_path, data_hash = _verify_tracked(
            dataset_dir, dataset_manifest, data_relative, owner="dataset"
        )
        labels_path, labels_hash = _verify_tracked(
            dataset_dir, dataset_manifest, labels_relative, owner="dataset"
        )
        paths[split] = {"data": data_path, "labels": labels_path}
        sources[f"{split}_data"] = {"path": data_relative, "sha256": data_hash}
        sources[f"{split}_labels"] = {
            "path": labels_relative,
            "sha256": labels_hash,
        }
    metrics_path, metrics_hash = _verify_tracked(
        model_dir, model_manifest, "metrics.json", owner="model"
    )
    predictions_path, predictions_hash = _verify_tracked(
        model_dir, model_manifest, "predictions.parquet", owner="model"
    )
    metrics = _load_json(metrics_path)
    if metrics.get("benchmark") != config.benchmark:
        raise ValueError("champion benchmark does not match holdout benchmark")
    if metrics.get("dataset_id") != dataset_manifest.get("dataset_id"):
        raise ValueError("champion dataset identity does not match")
    if metrics.get("dataset_manifest_sha256") != sha256(dataset_manifest_path):
        raise ValueError("champion metrics do not bind the current dataset")
    if model_manifest.get("model_name") != metrics.get("model_name") or model_manifest.get(
        "run_id"
    ) != metrics.get("run_id"):
        raise ValueError("champion artifact identity is inconsistent")
    sources["champion_metrics"] = {"path": "metrics.json", "sha256": metrics_hash}
    sources["champion_predictions"] = {
        "path": "predictions.parquet",
        "sha256": predictions_hash,
    }
    paths["model"] = {"predictions": predictions_path}
    return (
        dataset_manifest,
        schema,
        model_manifest,
        metrics,
        world_manifest,
        sources,
        paths,
    )


def _load_lineage_frames(
    paths: Mapping[str, Mapping[str, Path]],
    *,
    dataset_id: str,
    world_manifest: Mapping[str, Any],
    schema: Mapping[str, Any],
) -> dict[str, pd.DataFrame]:
    required_features = (
        set(schema["numeric_feature_columns"])
        | set(schema["categorical_feature_columns"])
        | {"event_id", "hand_id", "pair_key", "target"}
    )
    frames: dict[str, pd.DataFrame] = {}
    for split in ("train", "validation", "test"):
        frame = pd.read_parquet(paths[split]["data"])
        labels = pd.read_parquet(paths[split]["labels"])
        missing = sorted(required_features - set(frame.columns))
        if missing:
            raise ValueError(f"{split} data is missing columns: {missing}")
        required_labels = {"hand_id", "pair_key", "is_collusive", "collusion_pair_id"}
        missing_labels = sorted(required_labels - set(labels.columns))
        if missing_labels:
            raise ValueError(f"{split} labels are missing columns: {missing_labels}")
        lineage = labels[
            ["hand_id", "pair_key", "is_collusive", "collusion_pair_id"]
        ].copy()
        lineage["hand_id"] = lineage["hand_id"].astype(str)
        lineage["pair_key"] = lineage["pair_key"].astype(str)
        frame["hand_id"] = frame["hand_id"].astype(str)
        frame["pair_key"] = frame["pair_key"].astype(str)
        frame = frame.merge(
            lineage,
            on=["hand_id", "pair_key"],
            how="left",
            validate="one_to_one",
        )
        if frame["is_collusive"].isna().any():
            raise ValueError(f"{split} lineage is incomplete")
        if not np.array_equal(
            frame["target"].astype(bool).to_numpy(),
            frame["is_collusive"].astype(bool).to_numpy(),
        ):
            raise ValueError(f"{split} target disagrees with private label sidecar")
        families: list[str] = []
        pair_indices: list[int | None] = []
        for positive, pair_id in zip(
            frame["is_collusive"].astype(bool), frame["collusion_pair_id"]
        ):
            if not positive:
                if pd.notna(pair_id):
                    raise ValueError("negative row unexpectedly has a collusion pair ID")
                families.append("normal")
                pair_indices.append(None)
                continue
            if pd.isna(pair_id):
                raise ValueError("positive row is missing its collusion pair ID")
            family, pair_index = scenario_family_from_pair_id(
                str(pair_id), dataset_id=dataset_id, split=split
            )
            families.append(family)
            pair_indices.append(pair_index)
        frame["scenario_family"] = families
        frame["scenario_pair_index"] = pd.array(pair_indices, dtype="Int64")
        frame["generator_seed"] = int(world_manifest["splits"][split]["seed"])
        frame["source_split"] = split
        frames[split] = frame
    event_sets = {
        split: set(frame["event_id"].astype(str)) for split, frame in frames.items()
    }
    for left, right in (("train", "validation"), ("train", "test"), ("validation", "test")):
        if event_sets[left] & event_sets[right]:
            raise ValueError(f"event IDs overlap between {left} and {right}")
    return frames


def _align_champion_predictions(
    frames: Mapping[str, pd.DataFrame], predictions_path: Path
) -> dict[str, pd.DataFrame]:
    predictions = pd.read_parquet(predictions_path)
    output: dict[str, pd.DataFrame] = {}
    for split in ("validation", "test"):
        scored = predictions.loc[
            predictions["split"].astype(str) == split,
            ["event_id", "hand_id", "pair_key", "calibrated_probability", "alert"],
        ].copy()
        for column in ("event_id", "hand_id", "pair_key"):
            scored[column] = scored[column].astype(str)
        if scored["event_id"].duplicated().any():
            raise ValueError(f"champion {split} predictions contain duplicate event IDs")
        base = frames[split].copy()
        base["event_id"] = base["event_id"].astype(str)
        joined = base.merge(
            scored,
            on="event_id",
            how="left",
            suffixes=("", "_prediction"),
            validate="one_to_one",
        )
        if joined[["calibrated_probability", "alert"]].isna().any().any():
            raise ValueError(f"champion {split} predictions are incomplete")
        for column in ("hand_id", "pair_key"):
            if not (
                joined[column].astype(str)
                == joined[f"{column}_prediction"].astype(str)
            ).all():
                raise ValueError(f"champion prediction {column} lineage mismatch")
        output[split] = joined
    return output


def _lineage_frame(frames: Mapping[str, pd.DataFrame]) -> pd.DataFrame:
    columns = [
        "event_id",
        "hand_id",
        "pair_key",
        "source_split",
        "generator_seed",
        "is_collusive",
        "collusion_pair_id",
        "scenario_family",
        "scenario_pair_index",
    ]
    return pd.concat(
        [frames[split][columns] for split in ("train", "validation", "test")],
        ignore_index=True,
    ).sort_values(["source_split", "hand_id", "pair_key"], kind="mergesort")


def _semantic_lineage_digest(frame: pd.DataFrame) -> str:
    digest = hashlib.sha256()
    for row in frame.itertuples(index=False, name=None):
        normalized: list[Any] = []
        for value in row:
            if pd.isna(value):
                normalized.append(None)
            elif isinstance(value, np.generic):
                normalized.append(value.item())
            elif isinstance(value, pd.Timestamp):
                normalized.append(value.isoformat())
            else:
                normalized.append(value)
        digest.update(
            json.dumps(
                normalized,
                separators=(",", ":"),
            ).encode()
        )
        digest.update(b"\n")
    return digest.hexdigest()


def _array_digest(values: Sequence[float]) -> str:
    array = np.asarray(values, dtype="<f8")
    return hashlib.sha256(np.ascontiguousarray(array).tobytes()).hexdigest()


def _point_and_bootstrap(
    frame: pd.DataFrame,
    probabilities: Sequence[float],
    *,
    threshold: float,
    max_alert_rate: float,
    config: ScenarioHoldoutConfig,
    bootstrap_seed: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    probability_array = np.asarray(probabilities, dtype=np.float64)
    point = binary_classification_report(
        frame["target"].astype(np.int8).to_numpy(),
        probability_array,
        threshold=threshold,
        max_alert_rate=max_alert_rate,
        hand_count=int(frame["hand_id"].nunique()),
    )
    bootstrap_frame = pd.DataFrame(
        {
            "hand_id": frame["hand_id"].astype(str),
            "target": frame["target"].astype(np.int8),
            "calibrated_probability": probability_array,
        }
    )
    bootstrap = hand_grouped_bootstrap_intervals(
        bootstrap_frame,
        threshold=threshold,
        max_alert_rate=max_alert_rate,
        config=StabilityConfig(
            split="test",
            bootstrap_samples=config.bootstrap_samples,
            confidence_level=config.confidence_level,
            random_seed=bootstrap_seed,
        ),
    )
    for metric, interval in bootstrap["metrics"].items():
        interval["point_estimate"] = point[metric]
    return point, bootstrap


def _scenario_hand_sets(frame: pd.DataFrame, family: str) -> tuple[set[str], set[str]]:
    positive = frame.loc[frame["target"].astype(bool)]
    family_hands = set(
        positive.loc[positive["scenario_family"] == family, "hand_id"].astype(str)
    )
    other_hands = set(
        positive.loc[positive["scenario_family"] != family, "hand_id"].astype(str)
    )
    normal_hands = set(frame["hand_id"].astype(str)) - family_hands - other_hands
    return family_hands, normal_hands


def compute_scenario_holdout_report(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    config: ScenarioHoldoutConfig,
) -> tuple[dict[str, Any], pd.DataFrame]:
    (
        dataset_manifest,
        schema,
        _model_manifest,
        metrics,
        world_manifest,
        sources,
        paths,
    ) = _verified_sources(dataset_dir, model_dir, source_world_dir, config)
    frames = _load_lineage_frames(
        paths,
        dataset_id=dataset_manifest["dataset_id"],
        world_manifest=world_manifest,
        schema=schema,
    )
    lineage = _lineage_frame(frames)
    scored = _align_champion_predictions(frames, paths["model"]["predictions"])
    threshold = float(metrics["thresholds"]["catboost"])
    training = metrics["training_config"]
    max_alert_rate = float(training["max_alert_rate"])
    required_training = (
        "iterations",
        "depth",
        "learning_rate",
        "early_stopping_rounds",
        "positive_class_weight",
        "max_alert_rate",
    )
    missing_training = [key for key in required_training if key not in training]
    if missing_training:
        raise ValueError(f"champion training configuration is missing: {missing_training}")
    split_seeds = {
        split: int(world_manifest["splits"][split]["seed"])
        for split in ("train", "validation", "test")
    }
    if len(set(split_seeds.values())) != 3:
        raise ValueError("train/validation/test generator seeds must be distinct")

    generator_seed_holdouts: list[dict[str, Any]] = []
    for offset, split in enumerate(("validation", "test")):
        point, bootstrap = _point_and_bootstrap(
            scored[split],
            scored[split]["calibrated_probability"].astype(float),
            threshold=threshold,
            max_alert_rate=max_alert_rate,
            config=config,
            bootstrap_seed=config.bootstrap_seed + offset,
        )
        generator_seed_holdouts.append(
            {
                "split": split,
                "generator_seed": split_seeds[split],
                "unseen_from_training_seed": split_seeds[split] != split_seeds["train"],
                "point_metrics": point,
                "bootstrap": bootstrap,
            }
        )

    scenario_results: list[dict[str, Any]] = []
    numeric = schema["numeric_feature_columns"]
    categorical = schema["categorical_feature_columns"]
    for family_index, family in enumerate(SCENARIO_FAMILIES):
        family_hands: dict[str, set[str]] = {}
        normal_hands: dict[str, set[str]] = {}
        for split in ("train", "validation", "test"):
            family_hands[split], normal_hands[split] = _scenario_hand_sets(
                frames[split], family
            )
        if any(
            int(
                frames[split].loc[
                    frames[split]["scenario_family"] == family, "target"
                ].sum()
            )
            < config.minimum_scenario_positives
            for split in ("train", "validation", "test")
        ):
            raise ValueError(f"scenario family lacks reliable positive coverage: {family}")

        train = frames["train"].loc[
            ~frames["train"]["hand_id"].astype(str).isin(family_hands["train"])
        ].copy()
        validation = frames["validation"].loc[
            ~frames["validation"]["hand_id"].astype(str).isin(
                family_hands["validation"]
            )
        ].copy()
        evaluation_hands = family_hands["test"] | normal_hands["test"]
        test = frames["test"].loc[
            frames["test"]["hand_id"].astype(str).isin(evaluation_hands)
        ].copy()
        if set(train["hand_id"].astype(str)) & family_hands["train"]:
            raise RuntimeError("held-out scenario hand leaked into training")
        if set(validation["hand_id"].astype(str)) & family_hands["validation"]:
            raise RuntimeError("held-out scenario hand leaked into calibration")

        preprocessor = PairPreprocessor.fit(train, numeric, categorical)
        train_matrix = preprocessor.transform(train)
        validation_matrix = preprocessor.transform(validation)
        test_matrix = preprocessor.transform(test)
        train_labels = train["target"].astype(np.int8).to_numpy()
        validation_labels = validation["target"].astype(np.int8).to_numpy()
        if len(np.unique(train_labels)) != 2 or len(np.unique(validation_labels)) != 2:
            raise ValueError(f"scenario exclusion removed a required class: {family}")
        model = CatBoostClassifier(
            iterations=int(training["iterations"]),
            depth=int(training["depth"]),
            learning_rate=float(training["learning_rate"]),
            loss_function="Logloss",
            eval_metric="PRAUC",
            class_weights=[1.0, float(training["positive_class_weight"])],
            random_seed=config.random_seed,
            allow_writing_files=False,
            verbose=False,
        )
        model.fit(
            Pool(train_matrix, train_labels, feature_names=list(preprocessor.output_columns)),
            eval_set=Pool(
                validation_matrix,
                validation_labels,
                feature_names=list(preprocessor.output_columns),
            ),
            early_stopping_rounds=int(training["early_stopping_rounds"]),
            use_best_model=True,
            verbose=False,
        )
        raw_validation = np.asarray(
            model.predict_proba(validation_matrix), dtype=np.float64
        )[:, 1]
        calibrator = PlattCalibrator.fit(validation_labels, raw_validation)
        calibrated_validation = calibrator.predict(raw_validation)
        family_threshold = select_alert_budget_threshold(
            validation_labels, calibrated_validation, max_alert_rate
        )
        raw_test = np.asarray(model.predict_proba(test_matrix), dtype=np.float64)[:, 1]
        calibrated_test = calibrator.predict(raw_test)
        holdout_point, holdout_bootstrap = _point_and_bootstrap(
            test,
            calibrated_test,
            threshold=family_threshold,
            max_alert_rate=max_alert_rate,
            config=config,
            bootstrap_seed=config.bootstrap_seed + 100 + family_index,
        )
        champion_test = scored["test"].loc[
            scored["test"]["hand_id"].astype(str).isin(evaluation_hands)
        ].copy()
        champion_point, champion_bootstrap = _point_and_bootstrap(
            champion_test,
            champion_test["calibrated_probability"].astype(float),
            threshold=threshold,
            max_alert_rate=max_alert_rate,
            config=config,
            bootstrap_seed=config.bootstrap_seed + 200 + family_index,
        )
        scenario_results.append(
            {
                "scenario_family": family,
                "lineage_rule": (
                    f"pair_index modulo {len(SCENARIO_FAMILIES)} == {family_index}"
                ),
                "generator_seeds": split_seeds,
                "excluded_family_hands": {
                    split: len(family_hands[split])
                    for split in ("train", "validation")
                },
                "training_counts_after_exclusion": class_counts(train),
                "validation_counts_after_exclusion": class_counts(validation),
                "test_counts": class_counts(test),
                "best_iteration": int(model.get_best_iteration()),
                "calibration": calibrator.to_dict(),
                "threshold": family_threshold,
                "holdout_model": {
                    "point_metrics": holdout_point,
                    "bootstrap": holdout_bootstrap,
                    "prediction_sha256": _array_digest(calibrated_test),
                },
                "champion_reference": {
                    "point_metrics": champion_point,
                    "bootstrap": champion_bootstrap,
                },
                "pr_auc_delta_holdout_minus_champion": (
                    float(holdout_point["pr_auc"] - champion_point["pr_auc"])
                ),
                "scenario_hands_seen_during_training_or_calibration": 0,
            }
        )

    holdout_pr_auc = [
        float(result["holdout_model"]["point_metrics"]["pr_auc"])
        for result in scenario_results
    ]
    summary = {
        "minimum_scenario_holdout_pr_auc": min(holdout_pr_auc),
        "maximum_scenario_holdout_pr_auc": max(holdout_pr_auc),
        "mean_scenario_holdout_pr_auc": float(np.mean(holdout_pr_auc)),
        "families_evaluated": len(scenario_results),
        "status": "observed",
        "production_model_changed": False,
        "scenario_model_selected": False,
    }
    report = {
        "contract_version": SCENARIO_HOLDOUT_CONTRACT_VERSION,
        "configuration": config.to_dict(),
        "dataset": {
            "dataset_id": dataset_manifest["dataset_id"],
            "feature_definition_version": dataset_manifest[
                "feature_definition_version"
            ],
            "lineage_version": SCENARIO_LINEAGE_VERSION,
        },
        "champion": {
            "model_name": metrics["model_name"],
            "run_id": metrics["run_id"],
            "threshold": threshold,
            "training_generator_seed": split_seeds["train"],
        },
        "scenario_family_mapping": {
            "source": "synthetic collusion_pair_id pair index",
            "assignment": "generator round-robin CollusionPattern enum order",
            "families": list(SCENARIO_FAMILIES),
        },
        "generator_seed_holdouts": generator_seed_holdouts,
        "scenario_family_holdouts": scenario_results,
        "summary": summary,
        "source_artifacts": sources,
        "lineage": {
            "rows": len(lineage),
            "semantic_sha256": _semantic_lineage_digest(lineage),
            "private_evaluation_only": True,
            "included_in_model_features": False,
        },
        "leakage_controls": {
            "loaded_splits": ["train", "validation", "test"],
            "challenge_dataset_loaded": False,
            "challenge_labels_loaded": False,
            "scenario_lineage_used_as_model_feature": False,
            "held_out_family_hands_removed_from_training": True,
            "held_out_family_hands_removed_from_calibration": True,
            "test_used_for_training_calibration_or_selection": False,
            "production_model_changed": False,
        },
    }
    return report, lineage


def _canonical_payload(report: Mapping[str, Any]) -> bytes:
    payload = {
        key: value
        for key, value in report.items()
        if key not in {"generated_at", "integrity"}
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def build_scenario_holdout_report(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    report_path: Path,
    lineage_path: Path,
    *,
    config: ScenarioHoldoutConfig | None = None,
) -> dict[str, Any]:
    config = config or ScenarioHoldoutConfig()
    report, lineage = compute_scenario_holdout_report(
        dataset_dir, model_dir, source_world_dir, config
    )
    lineage_path.parent.mkdir(parents=True, exist_ok=True)
    lineage.to_parquet(lineage_path, index=False, compression="zstd")
    report["lineage"]["file"] = {
        "path": lineage_path.name,
        "sha256": sha256(lineage_path),
    }
    report["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": hashlib.sha256(_canonical_payload(report)).hexdigest(),
    }
    report["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def validate_scenario_holdout_report(
    dataset_dir: Path,
    model_dir: Path,
    source_world_dir: Path,
    report_path: Path,
    lineage_path: Path,
    *,
    recompute: bool = False,
) -> dict[str, Any]:
    report = _load_json(report_path)
    if report.get("contract_version") != SCENARIO_HOLDOUT_CONTRACT_VERSION:
        raise ValueError("unsupported scenario-holdout contract")
    expected_digest = hashlib.sha256(_canonical_payload(report)).hexdigest()
    if report.get("integrity") != {
        "algorithm": "sha256",
        "payload_sha256": expected_digest,
    }:
        raise ValueError("scenario-holdout payload integrity check failed")
    if report.get("lineage", {}).get("file", {}).get("sha256") != sha256(lineage_path):
        raise ValueError("scenario lineage file hash mismatch")
    lineage = pd.read_parquet(lineage_path)
    if len(lineage) != report["lineage"]["rows"]:
        raise ValueError("scenario lineage row count mismatch")
    if _semantic_lineage_digest(lineage) != report["lineage"]["semantic_sha256"]:
        raise ValueError("scenario lineage semantic hash mismatch")
    config = ScenarioHoldoutConfig.from_dict(report["configuration"])
    (
        dataset_manifest,
        _schema,
        _model_manifest,
        metrics,
        _world_manifest,
        sources,
        _paths,
    ) = _verified_sources(dataset_dir, model_dir, source_world_dir, config)
    if report.get("source_artifacts") != sources:
        raise ValueError("scenario-holdout source artifacts changed")
    if report.get("dataset", {}).get("dataset_id") != dataset_manifest.get(
        "dataset_id"
    ):
        raise ValueError("scenario-holdout dataset identity changed")
    if report.get("champion", {}).get("model_name") != metrics.get(
        "model_name"
    ) or report.get("champion", {}).get("run_id") != metrics.get("run_id"):
        raise ValueError("scenario-holdout champion identity changed")
    controls = report.get("leakage_controls", {})
    if controls.get("loaded_splits") != ["train", "validation", "test"] or any(
        controls.get(key) is not False
        for key in (
            "challenge_dataset_loaded",
            "challenge_labels_loaded",
            "scenario_lineage_used_as_model_feature",
            "test_used_for_training_calibration_or_selection",
            "production_model_changed",
        )
    ):
        raise ValueError("scenario-holdout leakage controls are invalid")
    results = report.get("scenario_family_holdouts", [])
    if [result.get("scenario_family") for result in results] != list(
        SCENARIO_FAMILIES
    ):
        raise ValueError("scenario families are missing or reordered")
    if any(
        result.get("scenario_hands_seen_during_training_or_calibration") != 0
        for result in results
    ):
        raise ValueError("held-out scenario leaked into model fitting")
    if recompute:
        expected, expected_lineage = compute_scenario_holdout_report(
            dataset_dir, model_dir, source_world_dir, config
        )
        actual = {
            key: value
            for key, value in report.items()
            if key not in {"generated_at", "integrity"}
        }
        actual["lineage"] = {
            key: value for key, value in actual["lineage"].items() if key != "file"
        }
        if actual != expected:
            raise ValueError("scenario-holdout training does not recompute")
        if _semantic_lineage_digest(expected_lineage) != report["lineage"][
            "semantic_sha256"
        ]:
            raise ValueError("scenario lineage does not recompute")
    return {
        "model_name": report["champion"]["model_name"],
        "run_id": report["champion"]["run_id"],
        "families": len(results),
        "minimum_pr_auc": report["summary"]["minimum_scenario_holdout_pr_auc"],
        "maximum_pr_auc": report["summary"]["maximum_scenario_holdout_pr_auc"],
        "recomputed": recompute,
    }
