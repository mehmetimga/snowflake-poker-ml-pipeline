from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline.ml.seed_stability import (
    SeedStabilityConfig,
    build_seed_stability_report,
    validate_seed_stability_report,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _frame(split: str, rows: int, seed: int) -> pd.DataFrame:
    generator = np.random.default_rng(seed)
    target = (np.arange(rows) % 10 == 0).astype(np.int8)
    return pd.DataFrame(
        {
            "event_id": [f"{split}-event-{index}" for index in range(rows)],
            "hand_id": [f"{split}-hand-{index // 5}" for index in range(rows)],
            "pair_key": [f"pair-{index}" for index in range(rows)],
            "feature_a": target * 2.0 + generator.normal(0, 0.35, rows),
            "feature_b": generator.normal(0, 1, rows),
            "context_status": np.where(np.arange(rows) % 2, "matched", "missing"),
            "target": target,
        }
    )


def _write_fixture(root: Path) -> tuple[Path, Path, Path]:
    dataset = root / "dataset"
    model = root / "model"
    output = root / "registry" / "validation_seed_stability.json"
    schema_path = dataset / "schema.json"
    train_path = dataset / "dgx" / "cold_start" / "train.parquet"
    validation_path = dataset / "dgx" / "cold_start" / "validation.parquet"
    train_path.parent.mkdir(parents=True)
    model.mkdir(parents=True)
    _write_json(
        schema_path,
        {
            "schema_version": 1,
            "feature_definition_version": "pair-features-v1",
            "challenge_labels_public": False,
            "numeric_feature_columns": ["feature_a", "feature_b"],
            "categorical_feature_columns": ["context_status"],
            "target_column": "target",
        },
    )
    _frame("train", 200, 7).to_parquet(train_path, index=False)
    _frame("validation", 100, 8).to_parquet(validation_path, index=False)
    manifest_path = dataset / "manifest.json"
    _write_json(
        manifest_path,
        {
            "schema_version": 1,
            "dataset_id": "seed-fixture-v1",
            "feature_definition_version": "pair-features-v1",
            "challenge_labels_public": False,
            "artifacts": {
                "schema.json": _sha256(schema_path),
                "dgx/cold_start/train.parquet": _sha256(train_path),
                "dgx/cold_start/validation.parquet": _sha256(validation_path),
                # Forbidden split files intentionally do not exist. The
                # experiment succeeds only if its loader never opens them.
                "dgx/cold_start/test.parquet": "not-loaded",
                "benchmarks/challenge/challenge/features.parquet": "not-loaded",
            },
        },
    )
    metrics_path = model / "metrics.json"
    _write_json(
        metrics_path,
        {
            "model_name": "fixture-catboost",
            "run_id": "fixture-run",
            "benchmark": "cold_start",
            "dataset_id": "seed-fixture-v1",
            "feature_definition_version": "pair-features-v1",
            "dataset_manifest_sha256": _sha256(manifest_path),
            "training_config": {
                "iterations": 20,
                "depth": 2,
                "learning_rate": 0.1,
                "early_stopping_rounds": 5,
                "positive_class_weight": 5.0,
                "max_alert_rate": 0.2,
                "random_seed": 42,
            },
        },
    )
    _write_json(
        model / "artifact_manifest.json",
        {
            "model_name": "fixture-catboost",
            "run_id": "fixture-run",
            "artifacts": {"metrics.json": _sha256(metrics_path)},
        },
    )
    return dataset, model, output


def test_seed_config_requires_five_unique_seeds() -> None:
    with pytest.raises(ValueError, match="at least five"):
        SeedStabilityConfig(seeds=(1, 2, 3, 4))
    with pytest.raises(ValueError, match="unique"):
        SeedStabilityConfig(seeds=(1, 2, 3, 4, 4))


def test_seed_stability_uses_only_train_validation_and_detects_mutation(
    tmp_path: Path,
) -> None:
    dataset, model, output = _write_fixture(tmp_path)
    config = SeedStabilityConfig(
        seeds=(1, 2, 3, 4, 5),
        maximum_relative_pr_auc_spread=1.0,
        minimum_pr_auc_prevalence_multiple=0.1,
    )
    report = build_seed_stability_report(dataset, model, output, config=config)
    result = validate_seed_stability_report(
        dataset, model, output, recompute=True
    )
    assert result["seeds"] == 5
    assert result["recomputed"] is True
    assert report["leakage_controls"]["loaded_splits"] == ["train", "validation"]
    assert report["leakage_controls"]["test_dataset_loaded"] is False
    assert report["leakage_controls"]["challenge_dataset_loaded"] is False
    assert set(report["source_artifacts"]) == {
        "dataset_manifest",
        "dataset_schema",
        "train",
        "validation",
        "model_artifact_manifest",
        "champion_metrics",
    }

    report["seed_results"][0]["validation_metrics"]["pr_auc"] = 0.0
    _write_json(output, report)
    with pytest.raises(ValueError, match="integrity"):
        validate_seed_stability_report(dataset, model, output)
