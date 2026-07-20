from __future__ import annotations

import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
from sklearn.metrics import average_precision_score, roc_auc_score

from pipeline.ml.pair_model import binary_classification_report
from pipeline.ml.stability import (
    _MetricWorkspace,
    StabilityConfig,
    build_stability_report,
    hand_group_bootstrap_weights,
    hand_grouped_bootstrap_intervals,
    validate_stability_report,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def test_hand_bootstrap_assigns_one_multiplicity_to_every_row_in_a_hand() -> None:
    hands = np.repeat(["h-1", "h-2", "h-3", "h-4"], [3, 2, 4, 1])
    row_weights, hand_counts = hand_group_bootstrap_weights(
        hands, np.random.default_rng(17)
    )
    assert int(hand_counts.sum()) == 4
    for hand in set(hands):
        assert len(set(row_weights[hands == hand])) == 1


def test_hand_grouped_intervals_are_deterministic() -> None:
    frame = pd.DataFrame(
        {
            "hand_id": np.repeat([f"hand-{index}" for index in range(20)], 3),
            "target": np.tile([1, 0, 0], 20),
            "calibrated_probability": np.tile([0.9, 0.4, 0.1], 20),
        }
    )
    config = StabilityConfig(bootstrap_samples=25, random_seed=9)
    first = hand_grouped_bootstrap_intervals(
        frame, threshold=0.8, max_alert_rate=0.2, config=config
    )
    second = hand_grouped_bootstrap_intervals(
        frame, threshold=0.8, max_alert_rate=0.2, config=config
    )
    assert first == second
    assert first["sampling_unit"] == "hand_id"
    assert first["all_rows_share_hand_multiplicity"] is True
    assert first["metrics"]["pr_auc"]["effective_samples"] == 25


def test_weighted_ranking_metrics_match_sklearn_with_score_ties() -> None:
    generator = np.random.default_rng(44)
    labels = generator.integers(0, 2, 100, dtype=np.int8)
    probabilities = np.round(generator.random(100), 1)
    weights = generator.integers(0, 5, 100)
    actual = _MetricWorkspace.build(labels, probabilities, 0.6).weighted_metrics(
        weights, max_alert_rate=0.2, sampled_hands=20
    )
    assert actual["pr_auc"] == pytest.approx(
        average_precision_score(labels, probabilities, sample_weight=weights),
        abs=1e-12,
    )
    assert actual["roc_auc"] == pytest.approx(
        roc_auc_score(labels, probabilities, sample_weight=weights), abs=1e-12
    )


def _write_fixture(root: Path) -> tuple[Path, Path, Path]:
    dataset = root / "dataset"
    model = root / "model"
    report = root / "registry" / "stability_report.json"
    evaluation_path = dataset / "dgx" / "cold_start" / "test.parquet"
    evaluation_path.parent.mkdir(parents=True)
    model.mkdir(parents=True)

    hands = np.repeat([f"hand-{index}" for index in range(12)], 3)
    pairs = [f"pair-{index % 3}" for index in range(len(hands))]
    event_ids = [f"event-{index}" for index in range(len(hands))]
    labels = np.tile([1, 0, 0], 12).astype(np.int8)
    probabilities = np.tile([0.9, 0.4, 0.1], 12)
    threshold = 0.8
    evaluation = pd.DataFrame(
        {
            "event_id": event_ids,
            "hand_id": hands,
            "pair_key": pairs,
            "target": labels,
        }
    )
    evaluation.to_parquet(evaluation_path, index=False)
    dataset_manifest = {
        "dataset_id": "fixture-v1",
        "feature_definition_version": "pair-features-v1",
        "challenge_labels_public": False,
        "artifacts": {
            "dgx/cold_start/test.parquet": _sha256(evaluation_path),
        },
    }
    (dataset / "manifest.json").write_text(
        json.dumps(dataset_manifest, indent=2, sort_keys=True) + "\n"
    )

    predictions_path = model / "predictions.parquet"
    pd.DataFrame(
        {
            "split": "test",
            "event_id": event_ids,
            "hand_id": hands,
            "pair_key": pairs,
            "calibrated_probability": probabilities,
            "alert": probabilities >= threshold,
        }
    ).to_parquet(predictions_path, index=False)
    point = binary_classification_report(
        labels,
        probabilities,
        threshold=threshold,
        max_alert_rate=0.2,
        hand_count=12,
    )
    metrics = {
        "benchmark": "cold_start",
        "dataset_id": "fixture-v1",
        "feature_definition_version": "pair-features-v1",
        "model_name": "fixture-model",
        "run_id": "fixture-run",
        "thresholds": {"catboost": threshold},
        "training_config": {"max_alert_rate": 0.2},
        "reports": {"catboost": {"test": point}},
    }
    metrics_path = model / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n")
    artifact_manifest = {
        "model_name": "fixture-model",
        "run_id": "fixture-run",
        "artifacts": {
            "metrics.json": _sha256(metrics_path),
            "predictions.parquet": _sha256(predictions_path),
        },
    }
    (model / "artifact_manifest.json").write_text(
        json.dumps(artifact_manifest, indent=2, sort_keys=True) + "\n"
    )
    return dataset, model, report


def test_stability_report_recomputes_and_detects_mutation(tmp_path: Path) -> None:
    dataset, model, report_path = _write_fixture(tmp_path)
    report = build_stability_report(
        dataset,
        model,
        report_path,
        config=StabilityConfig(bootstrap_samples=30, random_seed=11),
    )
    result = validate_stability_report(dataset, model, report_path)
    assert result["model_name"] == "fixture-model"
    assert result["bootstrap_samples"] == 30
    assert report["leakage_controls"]["private_challenge_dataset_loaded"] is False

    report["point_metrics"]["pr_auc"] = 0.0
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    with pytest.raises(ValueError, match="deterministic recomputation"):
        validate_stability_report(dataset, model, report_path)
