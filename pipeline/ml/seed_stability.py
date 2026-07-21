"""Validation-only CatBoost seed-robustness evidence.

This experiment is intentionally separate from the production trainer.  It
opens only the frozen train and validation parquet files, fits the champion
configuration for at least five random seeds, and never selects a seed using
test or private challenge evidence.
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
from catboost import CatBoostClassifier, Pool

from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)
from pipeline.ml.stability import sha256


SEED_STABILITY_CONTRACT_VERSION = 1
DEFAULT_SEEDS = (11, 23, 42, 67, 101)
SUMMARY_METRICS = (
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
class SeedStabilityConfig:
    benchmark: str = "cold_start"
    seeds: tuple[int, ...] = DEFAULT_SEEDS
    maximum_relative_pr_auc_spread: float = 0.25
    minimum_pr_auc_prevalence_multiple: float = 2.0

    def __post_init__(self) -> None:
        if self.benchmark not in ("cold_start", "temporal", "new_relationship"):
            raise ValueError(f"unsupported benchmark: {self.benchmark}")
        if len(self.seeds) < 5:
            raise ValueError("seed stability requires at least five training seeds")
        if len(set(self.seeds)) != len(self.seeds):
            raise ValueError("training seeds must be unique")
        if any(seed < 0 for seed in self.seeds):
            raise ValueError("training seeds must be non-negative")
        if self.maximum_relative_pr_auc_spread <= 0:
            raise ValueError("maximum_relative_pr_auc_spread must be positive")
        if self.minimum_pr_auc_prevalence_multiple <= 0:
            raise ValueError("minimum_pr_auc_prevalence_multiple must be positive")

    def to_dict(self) -> dict[str, Any]:
        return {
            "benchmark": self.benchmark,
            "seeds": list(self.seeds),
            "maximum_relative_pr_auc_spread": self.maximum_relative_pr_auc_spread,
            "minimum_pr_auc_prevalence_multiple": (
                self.minimum_pr_auc_prevalence_multiple
            ),
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "SeedStabilityConfig":
        return cls(
            benchmark=str(raw["benchmark"]),
            seeds=tuple(int(seed) for seed in raw["seeds"]),
            maximum_relative_pr_auc_spread=float(
                raw["maximum_relative_pr_auc_spread"]
            ),
            minimum_pr_auc_prevalence_multiple=float(
                raw["minimum_pr_auc_prevalence_multiple"]
            ),
        )


def parse_seeds(raw: str | Sequence[int]) -> tuple[int, ...]:
    if isinstance(raw, str):
        values = tuple(int(value.strip()) for value in raw.split(",") if value.strip())
    else:
        values = tuple(int(value) for value in raw)
    return values


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
    dataset_dir: Path, model_dir: Path, config: SeedStabilityConfig
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Path],
]:
    dataset_dir = dataset_dir.resolve()
    model_dir = model_dir.resolve()
    manifest_path = dataset_dir / "manifest.json"
    schema_path = dataset_dir / "schema.json"
    model_manifest_path = model_dir / "artifact_manifest.json"
    manifest = _load_json(manifest_path)
    schema = _load_json(schema_path)
    model_manifest = _load_json(model_manifest_path)
    if manifest.get("challenge_labels_public") or schema.get(
        "challenge_labels_public"
    ):
        raise ValueError("challenge labels must remain private")
    if manifest.get("feature_definition_version") != "pair-features-v1":
        raise ValueError("seed stability accepts pair-features-v1 only")
    _verify_tracked(dataset_dir, manifest, "schema.json", owner="dataset")
    split_paths: dict[str, Path] = {}
    split_hashes: dict[str, str] = {}
    for split in ("train", "validation"):
        relative = f"dgx/{config.benchmark}/{split}.parquet"
        path, digest = _verify_tracked(dataset_dir, manifest, relative, owner="dataset")
        split_paths[split] = path
        split_hashes[split] = digest
    metrics_path, metrics_hash = _verify_tracked(
        model_dir, model_manifest, "metrics.json", owner="model"
    )
    metrics = _load_json(metrics_path)
    if metrics.get("benchmark") != config.benchmark:
        raise ValueError("champion benchmark does not match seed experiment")
    if metrics.get("dataset_id") != manifest.get("dataset_id"):
        raise ValueError("champion dataset ID does not match seed experiment")
    if metrics.get("feature_definition_version") != manifest.get(
        "feature_definition_version"
    ):
        raise ValueError("champion feature definition does not match")
    if metrics.get("dataset_manifest_sha256") != sha256(manifest_path):
        raise ValueError("champion metrics do not bind the current dataset manifest")
    if model_manifest.get("model_name") != metrics.get("model_name"):
        raise ValueError("model name disagrees between manifest and metrics")
    if model_manifest.get("run_id") != metrics.get("run_id"):
        raise ValueError("model run disagrees between manifest and metrics")
    sources = {
        "dataset_manifest": {
            "path": "manifest.json",
            "sha256": sha256(manifest_path),
        },
        "dataset_schema": {"path": "schema.json", "sha256": sha256(schema_path)},
        "train": {
            "path": f"dgx/{config.benchmark}/train.parquet",
            "sha256": split_hashes["train"],
        },
        "validation": {
            "path": f"dgx/{config.benchmark}/validation.parquet",
            "sha256": split_hashes["validation"],
        },
        "model_artifact_manifest": {
            "path": "artifact_manifest.json",
            "sha256": sha256(model_manifest_path),
        },
        "champion_metrics": {"path": "metrics.json", "sha256": metrics_hash},
    }
    return manifest, schema, model_manifest, metrics, sources, split_paths


def _load_allowed_frames(
    split_paths: Mapping[str, Path], schema: Mapping[str, Any]
) -> dict[str, pd.DataFrame]:
    frames = {
        split: pd.read_parquet(split_paths[split])
        for split in ("train", "validation")
    }
    required = (
        set(schema["numeric_feature_columns"])
        | set(schema["categorical_feature_columns"])
        | {"target", "hand_id", "pair_key", "event_id"}
    )
    for split, frame in frames.items():
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{split} is missing columns: {missing}")
        if not set(frame["target"].astype(int).unique()).issubset({0, 1}):
            raise ValueError(f"{split} has non-binary targets")
        if frame["event_id"].astype(str).duplicated().any():
            raise ValueError(f"{split} has duplicate event IDs")
    train_events = set(frames["train"]["event_id"].astype(str))
    validation_events = set(frames["validation"]["event_id"].astype(str))
    if train_events & validation_events:
        raise ValueError("train and validation event IDs overlap")
    train_hands = set(frames["train"]["hand_id"].astype(str))
    validation_hands = set(frames["validation"]["hand_id"].astype(str))
    if train_hands & validation_hands:
        raise ValueError("train and validation hand IDs overlap")
    return frames


def _array_digest(values: Sequence[float]) -> str:
    array = np.asarray(values, dtype="<f8")
    return hashlib.sha256(np.ascontiguousarray(array).tobytes()).hexdigest()


def _statistics(values: Sequence[float]) -> dict[str, float]:
    array = np.asarray(values, dtype=np.float64)
    if not len(array) or not np.isfinite(array).all():
        raise ValueError("seed metric values must be finite and non-empty")
    return {
        "minimum": float(array.min()),
        "maximum": float(array.max()),
        "mean": float(array.mean()),
        "standard_deviation": float(array.std(ddof=0)),
        "range": float(array.max() - array.min()),
    }


def summarize_seed_results(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if len(results) < 5:
        raise ValueError("at least five seed results are required")
    summaries = {
        metric: _statistics(
            [float(result["validation_metrics"][metric]) for result in results]
        )
        for metric in SUMMARY_METRICS
    }
    summaries["threshold"] = _statistics(
        [float(result["threshold"]) for result in results]
    )
    summaries["best_iteration"] = _statistics(
        [float(result["best_iteration"]) for result in results]
    )
    return summaries


def _robustness_decision(
    summaries: Mapping[str, Any],
    *,
    prevalence: float,
    config: SeedStabilityConfig,
) -> dict[str, Any]:
    pr_auc = summaries["pr_auc"]
    relative_spread = (
        float(pr_auc["range"]) / float(pr_auc["mean"])
        if float(pr_auc["mean"])
        else math.inf
    )
    minimum_multiple = (
        float(pr_auc["minimum"]) / prevalence if prevalence else math.inf
    )
    reasons: list[str] = []
    if relative_spread > config.maximum_relative_pr_auc_spread:
        reasons.append(
            "validation PR-AUC relative spread exceeds the configured limit"
        )
    if minimum_multiple < config.minimum_pr_auc_prevalence_multiple:
        reasons.append(
            "minimum validation PR-AUC is below the configured prevalence multiple"
        )
    return {
        "status": "stable" if not reasons else "warning",
        "reasons": reasons,
        "validation_pr_auc_relative_spread": relative_spread,
        "minimum_validation_pr_auc_prevalence_multiple": minimum_multiple,
        "policy": {
            "maximum_relative_pr_auc_spread": (
                config.maximum_relative_pr_auc_spread
            ),
            "minimum_pr_auc_prevalence_multiple": (
                config.minimum_pr_auc_prevalence_multiple
            ),
        },
        "seed_selection_performed": False,
        "production_model_changed": False,
    }


def compute_seed_stability_report(
    dataset_dir: Path,
    model_dir: Path,
    config: SeedStabilityConfig,
) -> dict[str, Any]:
    """Train the champion configuration across seeds on train/validation only."""

    (
        manifest,
        schema,
        _model_manifest,
        champion_metrics,
        sources,
        split_paths,
    ) = _verified_sources(dataset_dir, model_dir, config)
    frames = _load_allowed_frames(split_paths, schema)
    counts = {split: class_counts(frame) for split, frame in frames.items()}
    for split in ("train", "validation"):
        if not counts[split]["positives"] or not counts[split]["negatives"]:
            raise ValueError(f"{split} must contain both classes")
    preprocessor = PairPreprocessor.fit(
        frames["train"],
        schema["numeric_feature_columns"],
        schema["categorical_feature_columns"],
    )
    matrices = {
        split: preprocessor.transform(frame) for split, frame in frames.items()
    }
    labels = {
        split: frame["target"].astype(np.int8).to_numpy()
        for split, frame in frames.items()
    }
    feature_names = list(preprocessor.output_columns)
    train_pool = Pool(matrices["train"], labels["train"], feature_names=feature_names)
    validation_pool = Pool(
        matrices["validation"], labels["validation"], feature_names=feature_names
    )
    training = champion_metrics["training_config"]
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
        raise ValueError(
            f"champion training configuration is missing: {missing_training}"
        )
    results: list[dict[str, Any]] = []
    for seed in config.seeds:
        model = CatBoostClassifier(
            iterations=int(training["iterations"]),
            depth=int(training["depth"]),
            learning_rate=float(training["learning_rate"]),
            loss_function="Logloss",
            eval_metric="PRAUC",
            class_weights=[1.0, float(training["positive_class_weight"])],
            random_seed=seed,
            allow_writing_files=False,
            verbose=False,
        )
        model.fit(
            train_pool,
            eval_set=validation_pool,
            early_stopping_rounds=int(training["early_stopping_rounds"]),
            use_best_model=True,
            verbose=False,
        )
        raw = np.asarray(
            model.predict_proba(matrices["validation"]), dtype=np.float64
        )[:, 1]
        calibrator = PlattCalibrator.fit(labels["validation"], raw)
        calibrated = calibrator.predict(raw)
        threshold = select_alert_budget_threshold(
            labels["validation"],
            calibrated,
            float(training["max_alert_rate"]),
        )
        report = binary_classification_report(
            labels["validation"],
            calibrated,
            threshold=threshold,
            max_alert_rate=float(training["max_alert_rate"]),
            hand_count=counts["validation"]["hands"],
        )
        results.append(
            {
                "seed": seed,
                "best_iteration": int(model.get_best_iteration()),
                "calibration": calibrator.to_dict(),
                "threshold": threshold,
                "validation_metrics": report,
                "prediction_digests": {
                    "raw_probability_sha256": _array_digest(raw),
                    "calibrated_probability_sha256": _array_digest(calibrated),
                },
            }
        )
    summaries = summarize_seed_results(results)
    validation_prevalence = float(
        counts["validation"]["positives"] / counts["validation"]["rows"]
    )
    robustness = _robustness_decision(
        summaries,
        prevalence=validation_prevalence,
        config=config,
    )
    return {
        "contract_version": SEED_STABILITY_CONTRACT_VERSION,
        "configuration": config.to_dict(),
        "dataset": {
            "dataset_id": manifest["dataset_id"],
            "feature_definition_version": manifest["feature_definition_version"],
            "benchmark": config.benchmark,
        },
        "champion": {
            "model_name": champion_metrics["model_name"],
            "run_id": champion_metrics["run_id"],
            "training_configuration": {
                key: training[key] for key in required_training
            },
        },
        "counts": counts,
        "seed_results": results,
        "metric_summaries": summaries,
        "robustness": robustness,
        "source_artifacts": sources,
        "leakage_controls": {
            "loaded_splits": ["train", "validation"],
            "test_dataset_loaded": False,
            "challenge_dataset_loaded": False,
            "challenge_labels_loaded": False,
            "model_predictions_loaded": False,
            "seed_selected_using_evaluation": False,
            "train_validation_event_overlap": 0,
            "train_validation_hand_overlap": 0,
        },
    }


def _canonical_payload(report: Mapping[str, Any]) -> bytes:
    payload = {
        key: value
        for key, value in report.items()
        if key not in {"generated_at", "integrity"}
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def build_seed_stability_report(
    dataset_dir: Path,
    model_dir: Path,
    output_path: Path,
    *,
    config: SeedStabilityConfig | None = None,
) -> dict[str, Any]:
    config = config or SeedStabilityConfig()
    report = compute_seed_stability_report(dataset_dir, model_dir, config)
    report["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": hashlib.sha256(_canonical_payload(report)).hexdigest(),
    }
    report["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    return report


def validate_seed_stability_report(
    dataset_dir: Path,
    model_dir: Path,
    report_path: Path,
    *,
    recompute: bool = False,
) -> dict[str, Any]:
    report = _load_json(report_path)
    if report.get("contract_version") != SEED_STABILITY_CONTRACT_VERSION:
        raise ValueError("unsupported seed-stability contract")
    expected_digest = hashlib.sha256(_canonical_payload(report)).hexdigest()
    if report.get("integrity") != {
        "algorithm": "sha256",
        "payload_sha256": expected_digest,
    }:
        raise ValueError("seed-stability payload integrity check failed")
    config = SeedStabilityConfig.from_dict(report["configuration"])
    manifest, _schema, _model_manifest, metrics, sources, _paths = _verified_sources(
        dataset_dir, model_dir, config
    )
    if report.get("source_artifacts") != sources:
        raise ValueError("seed-stability source artifacts changed")
    if report.get("dataset", {}).get("dataset_id") != manifest.get("dataset_id"):
        raise ValueError("seed-stability dataset identity changed")
    if report.get("champion", {}).get("model_name") != metrics.get("model_name"):
        raise ValueError("seed-stability champion identity changed")
    if report.get("champion", {}).get("run_id") != metrics.get("run_id"):
        raise ValueError("seed-stability champion run changed")
    controls = report.get("leakage_controls", {})
    if controls.get("loaded_splits") != ["train", "validation"] or any(
        controls.get(key) is not False
        for key in (
            "test_dataset_loaded",
            "challenge_dataset_loaded",
            "challenge_labels_loaded",
            "model_predictions_loaded",
            "seed_selected_using_evaluation",
        )
    ):
        raise ValueError("seed-stability leakage controls are invalid")
    results = report.get("seed_results", [])
    if [result.get("seed") for result in results] != list(config.seeds):
        raise ValueError("seed results are missing, duplicated, or reordered")
    if report.get("metric_summaries") != summarize_seed_results(results):
        raise ValueError("seed-stability metric summaries do not recompute")
    validation_counts = report["counts"]["validation"]
    prevalence = float(
        validation_counts["positives"] / validation_counts["rows"]
    )
    if report.get("robustness") != _robustness_decision(
        report["metric_summaries"], prevalence=prevalence, config=config
    ):
        raise ValueError("seed-stability robustness decision does not recompute")
    if recompute:
        expected = compute_seed_stability_report(dataset_dir, model_dir, config)
        actual = {
            key: value
            for key, value in report.items()
            if key not in {"generated_at", "integrity"}
        }
        if actual != expected:
            raise ValueError("seed-stability training does not deterministically recompute")
    pr_auc = report["metric_summaries"]["pr_auc"]
    return {
        "model_name": report["champion"]["model_name"],
        "run_id": report["champion"]["run_id"],
        "seeds": len(config.seeds),
        "status": report["robustness"]["status"],
        "minimum_pr_auc": pr_auc["minimum"],
        "maximum_pr_auc": pr_auc["maximum"],
        "relative_pr_auc_spread": report["robustness"][
            "validation_pr_auc_relative_spread"
        ],
        "recomputed": recompute,
    }
