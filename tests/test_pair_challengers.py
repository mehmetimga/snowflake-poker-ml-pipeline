from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
import torch
import hashlib
import json

from pipeline.dl.pair_challengers import (
    NeuralPairPreprocessor,
    PairChallengerConfig,
    challenger_gate,
    paired_hand_bootstrap_pr_auc,
    train_pair_challengers,
)
from pipeline.dl.tabular_models import MODEL_NAMES, build_tabular_model


def test_neural_preprocessor_fits_only_given_train_frame_and_maps_unknown():
    train = pd.DataFrame(
        {
            "amount": [1.0, None, 5.0],
            "constant": [2.0, 2.0, 2.0],
            "status": ["matched", "missing", "matched"],
        }
    )
    preprocessor = NeuralPairPreprocessor.fit(
        train, ["amount", "constant"], ["status"]
    )
    numeric, categorical = preprocessor.transform(
        pd.DataFrame(
            {
                "amount": [None, np.inf],
                "constant": [2.0, 2.0],
                "status": ["corrected", None],
            }
        )
    )

    assert preprocessor.numeric_fill_values["amount"] == 3.0
    assert preprocessor.numeric_scales["constant"] == 1.0
    assert numeric.shape == (2, 2)
    assert np.isfinite(numeric).all()
    unknown = preprocessor.categorical_values["status"].index("__UNKNOWN__")
    assert categorical[:, 0].tolist() == [unknown, unknown]
    assert preprocessor.to_dict()["fit_split"] == "train"


@pytest.mark.parametrize("model_name", MODEL_NAMES)
def test_tabular_challengers_return_one_logit_per_row(model_name: str):
    torch.manual_seed(42)
    model = build_tabular_model(model_name, numeric_dim=5, categorical_cardinalities=(3, 4))
    output = model(
        torch.randn(7, 5),
        torch.tensor([[0, 1], [1, 2], [2, 3], [0, 0], [1, 1], [2, 2], [0, 3]]),
    )

    assert output.shape == (7,)
    assert torch.isfinite(output).all()


def test_paired_bootstrap_clusters_by_hand_and_detects_better_ranking():
    frame = pd.DataFrame(
        {
            "hand_id": np.repeat([f"h-{index}" for index in range(20)], 2),
            "target": np.tile([0, 1], 20),
        }
    )
    candidate = np.tile([0.1, 0.9], 20)
    baseline = np.tile([0.6, 0.4], 20)
    result = paired_hand_bootstrap_pr_auc(
        frame, candidate, baseline, samples=30, seed=42
    )

    assert result["unit"] == "hand"
    assert result["effective_samples"] == 30
    assert result["pr_auc_difference_p2_5"] > 0


def test_challenger_gate_requires_statistical_and_operational_gain():
    candidate = {
        "pr_auc": 0.40,
        "recall_at_alert_budget": 0.75,
        "f1": 0.45,
        "alert_rate": 0.01,
    }
    baseline = {
        "pr_auc": 0.36,
        "recall_at_alert_budget": 0.70,
        "f1": 0.42,
    }
    bootstrap = {"pr_auc_difference_p2_5": 0.01}
    gate = challenger_gate(
        candidate,
        baseline,
        bootstrap,
        minimum_relative_pr_gain=0.02,
        max_alert_rate=0.02,
    )

    assert gate["promotion_candidate"] is True
    assert gate["promotion_eligible"] is False
    assert gate["requires_private_challenge_evaluation"] is True

    rejected = challenger_gate(
        {**candidate, "recall_at_alert_budget": 0.60},
        baseline,
        bootstrap,
        minimum_relative_pr_gain=0.02,
        max_alert_rate=0.02,
    )
    assert rejected["promotion_candidate"] is False
    assert any("recall" in reason for reason in rejected["reasons"])


def test_challenger_config_rejects_unknown_or_duplicate_models():
    with pytest.raises(ValueError, match="selected"):
        PairChallengerConfig(models=("unknown",))
    with pytest.raises(ValueError, match="unique"):
        PairChallengerConfig(models=("residual_mlp", "residual_mlp"))


def _hash(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_challenger_training_writes_reproducible_artifacts(tmp_path):
    dataset = tmp_path / "dataset"
    baseline = tmp_path / "baseline"
    output = tmp_path / "output"
    split_dir = dataset / "dgx" / "cold_start"
    split_dir.mkdir(parents=True)
    baseline.mkdir()
    schema = {
        "challenge_labels_public": False,
        "numeric_feature_columns": ["amount"],
        "categorical_feature_columns": ["status"],
    }
    (dataset / "schema.json").write_text(json.dumps(schema))
    artifacts = {"schema.json": _hash(dataset / "schema.json")}
    frames = {}
    for split, rows in (("train", 40), ("validation", 20), ("test", 20)):
        frame = pd.DataFrame(
            {
                "event_id": [f"{split}-event-{index}" for index in range(rows)],
                "hand_id": [f"{split}-hand-{index // 2}" for index in range(rows)],
                "pair_key": [f"a:{index}" for index in range(rows)],
                "benchmark_split": split,
                "amount": np.linspace(0, 1, rows),
                "status": np.where(np.arange(rows) % 2, "matched", "missing"),
                "target": np.asarray([0, 1] * (rows // 2), dtype=np.int8),
            }
        )
        path = split_dir / f"{split}.parquet"
        frame.to_parquet(path, index=False)
        artifacts[f"dgx/cold_start/{split}.parquet"] = _hash(path)
        frames[split] = frame
    manifest = {
        "dataset_id": "unit-pair-v1",
        "feature_definition_version": "pair-features-v1",
        "challenge_labels_public": False,
        "artifacts": artifacts,
    }
    (dataset / "manifest.json").write_text(json.dumps(manifest))
    baseline_predictions = pd.DataFrame(
        {
            "split": "test",
            "event_id": frames["test"]["event_id"],
            "calibrated_probability": np.linspace(0.05, 0.95, len(frames["test"])),
        }
    )
    baseline_predictions.to_parquet(baseline / "predictions.parquet", index=False)
    (baseline / "metrics.json").write_text(
        json.dumps(
            {
                "run_id": "catboost-unit",
                "benchmark": "cold_start",
                "dataset_manifest_sha256": _hash(dataset / "manifest.json"),
                "reports": {
                    "catboost": {
                        "test": {
                            "pr_auc": 0.5,
                            "recall_at_alert_budget": 0.1,
                            "f1": 0.1,
                        }
                    }
                },
            }
        )
    )

    summary = train_pair_challengers(
        PairChallengerConfig(
            dataset_dir=dataset,
            baseline_dir=baseline,
            output_dir=output,
            models=("residual_mlp",),
            epochs=1,
            batch_size=8,
            patience=1,
            bootstrap_samples=5,
            num_workers=0,
            device_name="cpu",
        )
    )

    assert summary["challenge_labels_used"] is False
    assert summary["models"]["residual_mlp"]["epochs_ran"] == 1
    assert (output / "residual_mlp" / "model.pt").is_file()
    assert (output / "predictions.parquet").is_file()
    artifact_manifest = json.loads((output / "artifact_manifest.json").read_text())
    assert "residual_mlp/model.pt" in artifact_manifest["artifacts"]
