"""Hand-grouped uncertainty evidence for registered pair-risk models.

The public evaluation unit is a poker hand, not an individual pair row.  A
six-player hand contributes fifteen correlated pair rows, so every bootstrap
draw samples complete hands and assigns one multiplicity to all rows from that
hand.  Private challenge data is deliberately outside this module.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd

from pipeline.ml.pair_model import binary_classification_report


STABILITY_CONTRACT_VERSION = 1
SUPPORTED_BENCHMARKS = ("cold_start", "temporal", "new_relationship")
PUBLIC_EVALUATION_SPLITS = ("validation", "test")
BOOTSTRAP_METRICS = (
    "pr_auc",
    "roc_auc",
    "brier_score",
    "precision",
    "recall",
    "f1",
    "alert_rate",
    "recall_at_alert_budget",
    "precision_at_alert_budget",
    "false_positives_per_1000_hands",
)


@dataclass(frozen=True)
class StabilityConfig:
    """Configuration recorded inside a reproducible stability report."""

    benchmark: str = "cold_start"
    split: str = "test"
    bootstrap_samples: int = 1000
    confidence_level: float = 0.95
    random_seed: int = 42

    def __post_init__(self) -> None:
        if self.benchmark not in SUPPORTED_BENCHMARKS:
            raise ValueError(f"unsupported benchmark: {self.benchmark}")
        if self.split not in PUBLIC_EVALUATION_SPLITS:
            raise ValueError(
                "stability evaluation accepts validation or public test only"
            )
        if self.bootstrap_samples < 1:
            raise ValueError("bootstrap_samples must be positive")
        if not 0 < self.confidence_level < 1:
            raise ValueError("confidence_level must be in (0, 1)")

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "split": self.split,
            "bootstrap_samples": self.bootstrap_samples,
            "confidence_level": self.confidence_level,
            "random_seed": self.random_seed,
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "StabilityConfig":
        return cls(
            benchmark=str(raw["benchmark"]),
            split=str(raw["split"]),
            bootstrap_samples=int(raw["bootstrap_samples"]),
            confidence_level=float(raw["confidence_level"]),
            random_seed=int(raw["random_seed"]),
        )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def hand_group_bootstrap_weights(
    hand_ids: Sequence[Any], generator: np.random.Generator
) -> tuple[np.ndarray, np.ndarray]:
    """Return one bootstrap multiplicity per row and per unique hand.

    Drawing a multinomial count for every hand is equivalent to sampling the
    original number of hands with replacement, while avoiding materializing a
    repeated 75,000-row frame for every replicate.
    """

    hands = np.asarray(hand_ids, dtype=str)
    if not len(hands):
        raise ValueError("hand_ids must be non-empty")
    unique_hands, row_to_hand = np.unique(hands, return_inverse=True)
    number_of_hands = len(unique_hands)
    hand_counts = generator.multinomial(
        number_of_hands, np.full(number_of_hands, 1.0 / number_of_hands)
    ).astype(np.int64)
    return hand_counts[row_to_hand], hand_counts


@dataclass(frozen=True)
class _MetricWorkspace:
    labels: np.ndarray
    probabilities: np.ndarray
    predicted: np.ndarray
    descending_order: np.ndarray
    group_starts: np.ndarray

    @classmethod
    def build(
        cls, labels: Sequence[int], probabilities: Sequence[float], threshold: float
    ) -> "_MetricWorkspace":
        y = np.asarray(labels, dtype=np.int8)
        p = np.asarray(probabilities, dtype=np.float64)
        if not len(y) or len(y) != len(p):
            raise ValueError("labels and probabilities must be non-empty and aligned")
        if not set(np.unique(y)).issubset({0, 1}):
            raise ValueError("labels must be binary")
        if not np.isfinite(p).all() or np.any((p < 0) | (p > 1)):
            raise ValueError("probabilities must be finite and in [0, 1]")
        order = np.argsort(-p, kind="stable")
        sorted_probabilities = p[order]
        starts = np.concatenate(
            ([0], np.flatnonzero(np.diff(sorted_probabilities) != 0) + 1)
        ).astype(np.int64)
        return cls(
            labels=y,
            probabilities=p,
            predicted=p >= float(threshold),
            descending_order=order,
            group_starts=starts,
        )

    def weighted_metrics(
        self,
        row_weights: Sequence[int | float],
        *,
        max_alert_rate: float,
        sampled_hands: int,
    ) -> dict[str, float]:
        weights = np.asarray(row_weights, dtype=np.float64)
        if weights.shape != self.labels.shape:
            raise ValueError("row weights are not aligned with labels")
        if np.any(weights < 0) or not np.isfinite(weights).all():
            raise ValueError("row weights must be finite and non-negative")
        total = float(weights.sum())
        if total <= 0 or sampled_hands < 1:
            raise ValueError("bootstrap sample must contain rows and hands")

        labels = self.labels.astype(np.float64, copy=False)
        positives = float(np.dot(weights, labels))
        negatives = total - positives
        predicted = self.predicted.astype(np.float64, copy=False)
        true_positives = float(np.dot(weights, labels * predicted))
        alerts = float(np.dot(weights, predicted))
        false_positives = alerts - true_positives
        false_negatives = positives - true_positives

        precision = true_positives / alerts if alerts else 0.0
        recall = true_positives / positives if positives else math.nan
        f1_denominator = 2 * true_positives + false_positives + false_negatives
        f1 = 2 * true_positives / f1_denominator if f1_denominator else 0.0
        brier = float(
            np.dot(weights, np.square(self.probabilities - labels)) / total
        )

        order = self.descending_order
        ordered_weights = weights[order]
        ordered_labels = labels[order]
        group_positive = np.add.reduceat(
            ordered_weights * ordered_labels, self.group_starts
        )
        group_total = np.add.reduceat(ordered_weights, self.group_starts)
        group_negative = group_total - group_positive

        if positives:
            cumulative_positive = np.cumsum(group_positive)
            cumulative_total = np.cumsum(group_total)
            group_precision = np.divide(
                cumulative_positive,
                cumulative_total,
                out=np.zeros_like(cumulative_positive),
                where=cumulative_total > 0,
            )
            pr_auc = float(np.dot(group_positive, group_precision) / positives)
        else:
            pr_auc = math.nan

        if positives and negatives:
            cumulative_negative = np.cumsum(group_negative)
            lower_scored_negative = negatives - cumulative_negative
            concordant = np.dot(
                group_positive,
                lower_scored_negative + 0.5 * group_negative,
            )
            roc_auc = float(concordant / (positives * negatives))
        else:
            roc_auc = math.nan

        allowed = max(1, int(math.floor(total * max_alert_rate)))
        cumulative_rows = np.cumsum(ordered_weights)
        rows_before = cumulative_rows - ordered_weights
        selected_weight = np.clip(allowed - rows_before, 0, ordered_weights)
        selected_total = float(selected_weight.sum())
        selected_positive = float(np.dot(selected_weight, ordered_labels))
        precision_at_budget = (
            selected_positive / selected_total if selected_total else 0.0
        )
        recall_at_budget = (
            selected_positive / positives if positives else math.nan
        )

        return {
            "pr_auc": pr_auc,
            "roc_auc": roc_auc,
            "brier_score": brier,
            "precision": precision,
            "recall": recall,
            "f1": f1,
            "alert_rate": alerts / total,
            "recall_at_alert_budget": recall_at_budget,
            "precision_at_alert_budget": precision_at_budget,
            "false_positives_per_1000_hands": (
                false_positives * 1000.0 / sampled_hands
            ),
        }


def hand_grouped_bootstrap_intervals(
    frame: pd.DataFrame,
    *,
    threshold: float,
    max_alert_rate: float,
    config: StabilityConfig,
) -> dict[str, Any]:
    """Compute deterministic percentile intervals from complete-hand draws."""

    required = {"hand_id", "target", "calibrated_probability"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"stability frame is missing columns: {missing}")
    if frame["hand_id"].isna().any():
        raise ValueError("stability frame contains a missing hand_id")

    hand_ids = frame["hand_id"].astype(str).to_numpy()
    unique_hands, hand_row_counts = np.unique(hand_ids, return_counts=True)
    workspace = _MetricWorkspace.build(
        frame["target"].astype(int).to_numpy(),
        frame["calibrated_probability"].astype(float).to_numpy(),
        threshold,
    )
    generator = np.random.default_rng(config.random_seed)
    values = {
        metric: np.full(config.bootstrap_samples, np.nan, dtype=np.float64)
        for metric in BOOTSTRAP_METRICS
    }
    for sample in range(config.bootstrap_samples):
        row_weights, hand_counts = hand_group_bootstrap_weights(hand_ids, generator)
        if int(hand_counts.sum()) != len(unique_hands):
            raise RuntimeError("bootstrap hand multiplicities do not preserve draw size")
        metrics = workspace.weighted_metrics(
            row_weights,
            max_alert_rate=max_alert_rate,
            sampled_hands=len(unique_hands),
        )
        for metric in BOOTSTRAP_METRICS:
            values[metric][sample] = metrics[metric]

    alpha = (1.0 - config.confidence_level) / 2.0
    intervals: dict[str, Any] = {}
    for metric, samples in values.items():
        finite = samples[np.isfinite(samples)]
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
        "random_seed": config.random_seed,
        "unique_hands": int(len(unique_hands)),
        "rows": int(len(frame)),
        "rows_per_hand_min": int(hand_row_counts.min()),
        "rows_per_hand_max": int(hand_row_counts.max()),
        "all_rows_share_hand_multiplicity": True,
        "metrics": intervals,
    }


def _verify_tracked_file(
    root: Path, artifacts: Mapping[str, str], relative: str, *, owner: str
) -> tuple[Path, str]:
    expected = artifacts.get(relative)
    if expected is None:
        raise ValueError(f"{owner} manifest does not track {relative}")
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    actual = sha256(path)
    if actual != expected:
        raise ValueError(f"{owner} artifact hash mismatch: {relative}")
    return path, actual


def _load_public_evaluation(
    dataset_dir: Path, model_dir: Path, config: StabilityConfig
) -> tuple[dict[str, Any], dict[str, Any], pd.DataFrame, dict[str, Any]]:
    dataset_dir, model_dir = dataset_dir.resolve(), model_dir.resolve()
    dataset_manifest_path = dataset_dir / "manifest.json"
    model_manifest_path = model_dir / "artifact_manifest.json"
    if not dataset_manifest_path.is_file() or not model_manifest_path.is_file():
        raise FileNotFoundError("dataset and model artifact manifests are required")
    dataset_manifest = json.loads(dataset_manifest_path.read_text())
    model_manifest = json.loads(model_manifest_path.read_text())
    if dataset_manifest.get("challenge_labels_public"):
        raise ValueError("challenge labels must not be public")

    evaluation_relative = f"dgx/{config.benchmark}/{config.split}.parquet"
    evaluation_path, evaluation_hash = _verify_tracked_file(
        dataset_dir,
        dataset_manifest["artifacts"],
        evaluation_relative,
        owner="dataset",
    )
    metrics_path, metrics_hash = _verify_tracked_file(
        model_dir, model_manifest["artifacts"], "metrics.json", owner="model"
    )
    predictions_path, predictions_hash = _verify_tracked_file(
        model_dir,
        model_manifest["artifacts"],
        "predictions.parquet",
        owner="model",
    )
    metrics = json.loads(metrics_path.read_text())
    identities = {
        str(model_manifest.get("run_id")),
        str(metrics.get("run_id")),
    }
    if len(identities) != 1:
        raise ValueError("model manifest and metrics run IDs disagree")
    if metrics.get("benchmark") != config.benchmark:
        raise ValueError("stability benchmark does not match the trained model")
    if metrics.get("dataset_id") != dataset_manifest.get("dataset_id"):
        raise ValueError("dataset identity does not match model metrics")
    if metrics.get("feature_definition_version") != dataset_manifest.get(
        "feature_definition_version"
    ):
        raise ValueError("feature-definition identity does not match")

    evaluation = pd.read_parquet(evaluation_path)
    required_evaluation = {"event_id", "hand_id", "pair_key", "target"}
    missing = sorted(required_evaluation - set(evaluation.columns))
    if missing:
        raise ValueError(f"public evaluation is missing columns: {missing}")
    if evaluation["event_id"].astype(str).duplicated().any():
        raise ValueError("public evaluation has duplicate event IDs")

    predictions = pd.read_parquet(predictions_path)
    predictions = predictions.loc[
        predictions["split"].astype(str) == config.split,
        [
            "event_id",
            "hand_id",
            "pair_key",
            "calibrated_probability",
            "alert",
        ],
    ].copy()
    if predictions["event_id"].astype(str).duplicated().any():
        raise ValueError("model predictions have duplicate event IDs")
    for frame in (evaluation, predictions):
        for column in ("event_id", "hand_id", "pair_key"):
            frame[column] = frame[column].astype(str)

    aligned = evaluation[["event_id", "hand_id", "pair_key", "target"]].merge(
        predictions,
        on="event_id",
        how="left",
        suffixes=("_dataset", "_prediction"),
        validate="one_to_one",
    )
    if aligned[["calibrated_probability", "alert"]].isna().any().any():
        raise ValueError("public evaluation predictions are incomplete")
    for column in ("hand_id", "pair_key"):
        if not (
            aligned[f"{column}_dataset"] == aligned[f"{column}_prediction"]
        ).all():
            raise ValueError(f"prediction {column} lineage does not match dataset")
    aligned = aligned.rename(columns={"hand_id_dataset": "hand_id"})

    sources = {
        "dataset_manifest": {
            "path": "manifest.json",
            "sha256": sha256(dataset_manifest_path),
        },
        "public_evaluation": {
            "path": evaluation_relative,
            "sha256": evaluation_hash,
        },
        "model_artifact_manifest": {
            "path": "artifact_manifest.json",
            "sha256": sha256(model_manifest_path),
        },
        "model_metrics": {"path": "metrics.json", "sha256": metrics_hash},
        "model_predictions": {
            "path": "predictions.parquet",
            "sha256": predictions_hash,
        },
    }
    return dataset_manifest, metrics, aligned, sources


def _metric_close(actual: Any, expected: Any, *, tolerance: float = 1e-12) -> bool:
    if actual is None or expected is None:
        return actual is expected
    return math.isclose(float(actual), float(expected), rel_tol=tolerance, abs_tol=tolerance)


def compute_stability_report(
    dataset_dir: Path,
    model_dir: Path,
    config: StabilityConfig,
) -> dict[str, Any]:
    """Compute the hash-bound report payload without writing it."""

    dataset_manifest, metrics, aligned, sources = _load_public_evaluation(
        dataset_dir, model_dir, config
    )
    threshold = float(metrics["thresholds"]["catboost"])
    max_alert_rate = float(metrics["training_config"]["max_alert_rate"])
    probabilities = aligned["calibrated_probability"].astype(float).to_numpy()
    labels = aligned["target"].astype(int).to_numpy()
    expected_alerts = probabilities >= threshold
    if not np.array_equal(expected_alerts, aligned["alert"].astype(bool).to_numpy()):
        raise ValueError("stored alert flags do not follow the frozen threshold")
    point = binary_classification_report(
        labels,
        probabilities,
        threshold=threshold,
        max_alert_rate=max_alert_rate,
        hand_count=int(aligned["hand_id"].nunique()),
    )
    artifact_point = metrics["reports"]["catboost"][config.split]
    for metric in (
        "rows",
        "hands",
        "positives",
        "negatives",
        *BOOTSTRAP_METRICS,
    ):
        if not _metric_close(point[metric], artifact_point[metric]):
            raise ValueError(f"recomputed point metric disagrees with artifact: {metric}")

    bootstrap_frame = pd.DataFrame(
        {
            "hand_id": aligned["hand_id"],
            "target": labels,
            "calibrated_probability": probabilities,
        }
    )
    bootstrap = hand_grouped_bootstrap_intervals(
        bootstrap_frame,
        threshold=threshold,
        max_alert_rate=max_alert_rate,
        config=config,
    )
    for metric in BOOTSTRAP_METRICS:
        bootstrap["metrics"][metric]["point_estimate"] = point[metric]

    return {
        "contract_version": STABILITY_CONTRACT_VERSION,
        "configuration": config.to_dict(),
        "dataset": {
            "dataset_id": dataset_manifest["dataset_id"],
            "feature_definition_version": dataset_manifest[
                "feature_definition_version"
            ],
            "challenge_labels_public": False,
        },
        "model": {
            "model_name": metrics["model_name"],
            "run_id": metrics["run_id"],
            "threshold": threshold,
            "max_alert_rate": max_alert_rate,
        },
        "counts": {
            "rows": int(point["rows"]),
            "hands": int(point["hands"]),
            "positives": int(point["positives"]),
            "negatives": int(point["negatives"]),
        },
        "point_metrics": point,
        "bootstrap": bootstrap,
        "source_artifacts": sources,
        "leakage_controls": {
            "sampling_unit": "hand_id",
            "pair_rows_sampled_independently": False,
            "private_challenge_dataset_loaded": False,
            "evaluated_splits": [config.split],
        },
    }


def build_stability_report(
    dataset_dir: Path,
    model_dir: Path,
    output_path: Path,
    *,
    config: StabilityConfig | None = None,
) -> dict[str, Any]:
    config = config or StabilityConfig()
    report = compute_stability_report(dataset_dir, model_dir, config)
    report["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def validate_stability_report(
    dataset_dir: Path, model_dir: Path, report_path: Path
) -> dict[str, Any]:
    """Verify source hashes and deterministically recompute report evidence."""

    report = json.loads(report_path.read_text())
    if report.get("contract_version") != STABILITY_CONTRACT_VERSION:
        raise ValueError("unsupported stability report contract")
    config = StabilityConfig.from_dict(report["configuration"])
    expected = compute_stability_report(dataset_dir, model_dir, config)
    actual = {key: value for key, value in report.items() if key != "generated_at"}
    if actual != expected:
        raise ValueError("stability report does not match deterministic recomputation")
    return {
        "model_name": expected["model"]["model_name"],
        "run_id": expected["model"]["run_id"],
        "split": config.split,
        "rows": expected["counts"]["rows"],
        "hands": expected["counts"]["hands"],
        "bootstrap_samples": config.bootstrap_samples,
        "pr_auc": expected["point_metrics"]["pr_auc"],
        "pr_auc_interval": expected["bootstrap"]["metrics"]["pr_auc"],
    }
