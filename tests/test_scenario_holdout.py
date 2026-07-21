from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline.ml.scenario_holdout import (
    SCENARIO_FAMILIES,
    ScenarioHoldoutConfig,
    build_scenario_holdout_report,
    scenario_family_from_pair_id,
    validate_scenario_holdout_report,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _split_frames(split: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    rows: list[dict[str, object]] = []
    labels: list[dict[str, object]] = []
    event_index = 0
    for pair_index, family in enumerate(SCENARIO_FAMILIES):
        for hand_index in range(3):
            hand_id = f"{split}-{family}-hand-{hand_index}"
            for pair_row in range(3):
                positive = pair_row == 0
                pair_key = f"{hand_id}-pair-{pair_row}"
                rows.append(
                    {
                        "event_id": f"{split}-event-{event_index}",
                        "hand_id": hand_id,
                        "pair_key": pair_key,
                        "feature_a": 2.0 + pair_index * 0.1 if positive else 0.1,
                        "feature_b": float((event_index % 5) / 10),
                        "context_status": "matched" if event_index % 2 else "missing",
                        "target": int(positive),
                    }
                )
                labels.append(
                    {
                        "hand_id": hand_id,
                        "pair_key": pair_key,
                        "is_collusive": positive,
                        "collusion_pair_id": (
                            f"fixture-v1_{split}_pair_{pair_index:03d}"
                            if positive
                            else None
                        ),
                    }
                )
                event_index += 1
    for hand_index in range(8):
        hand_id = f"{split}-normal-hand-{hand_index}"
        for pair_row in range(3):
            pair_key = f"{hand_id}-pair-{pair_row}"
            rows.append(
                {
                    "event_id": f"{split}-event-{event_index}",
                    "hand_id": hand_id,
                    "pair_key": pair_key,
                    "feature_a": 0.05,
                    "feature_b": float((event_index % 5) / 10),
                    "context_status": "matched" if event_index % 2 else "missing",
                    "target": 0,
                }
            )
            labels.append(
                {
                    "hand_id": hand_id,
                    "pair_key": pair_key,
                    "is_collusive": False,
                    "collusion_pair_id": None,
                }
            )
            event_index += 1
    return pd.DataFrame(rows), pd.DataFrame(labels)


def _write_fixture(root: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    dataset = root / "dataset"
    model = root / "model"
    world = root / "world"
    registry = root / "registry"
    model.mkdir(parents=True)
    world.mkdir(parents=True)
    world_manifest_path = world / "manifest.json"
    _write_json(
        world_manifest_path,
        {
            "schema_version": 1,
            "dataset_id": "fixture-v1",
            "base_seed": 17,
            "splits": {
                "train": {"seed": 17},
                "validation": {"seed": 10017},
                "test": {"seed": 20017},
            },
        },
    )
    schema_path = dataset / "schema.json"
    _write_json(
        schema_path,
        {
            "schema_version": 1,
            "feature_definition_version": "pair-features-v1",
            "challenge_labels_public": False,
            "numeric_feature_columns": ["feature_a", "feature_b"],
            "categorical_feature_columns": ["context_status"],
        },
    )
    artifacts = {"schema.json": _sha256(schema_path)}
    prediction_frames: list[pd.DataFrame] = []
    for split in ("train", "validation", "test"):
        frame, labels = _split_frames(split)
        data_path = dataset / "dgx" / "cold_start" / f"{split}.parquet"
        label_path = (
            dataset
            / "benchmarks"
            / "cold_start"
            / split
            / "labels"
            / "pair_labels.parquet"
        )
        data_path.parent.mkdir(parents=True, exist_ok=True)
        label_path.parent.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(data_path, index=False)
        labels.to_parquet(label_path, index=False)
        artifacts[str(data_path.relative_to(dataset))] = _sha256(data_path)
        artifacts[str(label_path.relative_to(dataset))] = _sha256(label_path)
        if split in ("validation", "test"):
            probabilities = np.where(frame["target"].astype(bool), 0.9, 0.1)
            prediction_frames.append(
                pd.DataFrame(
                    {
                        "split": split,
                        "event_id": frame["event_id"],
                        "hand_id": frame["hand_id"],
                        "pair_key": frame["pair_key"],
                        "calibrated_probability": probabilities,
                        "alert": probabilities >= 0.5,
                    }
                )
            )
    dataset_manifest_path = dataset / "manifest.json"
    _write_json(
        dataset_manifest_path,
        {
            "schema_version": 1,
            "dataset_id": "fixture-v1",
            "feature_definition_version": "pair-features-v1",
            "challenge_labels_public": False,
            "source_manifest_sha256": _sha256(world_manifest_path),
            "artifacts": artifacts,
        },
    )
    predictions_path = model / "predictions.parquet"
    pd.concat(prediction_frames, ignore_index=True).to_parquet(
        predictions_path, index=False
    )
    metrics_path = model / "metrics.json"
    _write_json(
        metrics_path,
        {
            "model_name": "fixture-catboost",
            "run_id": "fixture-run",
            "benchmark": "cold_start",
            "dataset_id": "fixture-v1",
            "feature_definition_version": "pair-features-v1",
            "dataset_manifest_sha256": _sha256(dataset_manifest_path),
            "thresholds": {"catboost": 0.5},
            "training_config": {
                "iterations": 12,
                "depth": 2,
                "learning_rate": 0.1,
                "early_stopping_rounds": 4,
                "positive_class_weight": 3.0,
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
            "artifacts": {
                "metrics.json": _sha256(metrics_path),
                "predictions.parquet": _sha256(predictions_path),
            },
        },
    )
    return (
        dataset,
        model,
        world,
        registry / "scenario_holdout_report.json",
        registry / "generator_scenario_lineage.parquet",
        registry,
    )


def test_scenario_pair_mapping_is_strict_and_round_robin() -> None:
    for index, family in enumerate(SCENARIO_FAMILIES):
        actual, pair_index = scenario_family_from_pair_id(
            f"fixture-v1_test_pair_{index:03d}",
            dataset_id="fixture-v1",
            split="test",
        )
        assert actual == family
        assert pair_index == index
    with pytest.raises(ValueError, match="dataset/split"):
        scenario_family_from_pair_id(
            "fixture-v1_train_pair_000",
            dataset_id="fixture-v1",
            split="test",
        )


def test_scenario_holdouts_remove_family_hands_and_recompute(tmp_path: Path) -> None:
    dataset, model, world, report_path, lineage_path, _registry = _write_fixture(
        tmp_path
    )
    report = build_scenario_holdout_report(
        dataset,
        model,
        world,
        report_path,
        lineage_path,
        config=ScenarioHoldoutConfig(
            bootstrap_samples=5,
            minimum_scenario_positives=2,
        ),
    )
    result = validate_scenario_holdout_report(
        dataset,
        model,
        world,
        report_path,
        lineage_path,
        recompute=True,
    )
    assert result["families"] == 4
    assert result["recomputed"] is True
    assert report["leakage_controls"]["challenge_dataset_loaded"] is False
    assert report["lineage"]["included_in_model_features"] is False
    assert all(
        item["scenario_hands_seen_during_training_or_calibration"] == 0
        for item in report["scenario_family_holdouts"]
    )
    assert {item["generator_seed"] for item in report["generator_seed_holdouts"]} == {
        10017,
        20017,
    }

    report["summary"]["minimum_scenario_holdout_pr_auc"] = 0.0
    _write_json(report_path, report)
    with pytest.raises(ValueError, match="integrity"):
        validate_scenario_holdout_report(
            dataset, model, world, report_path, lineage_path
        )
