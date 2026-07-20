from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    rules_only_score,
    select_alert_budget_threshold,
)
from pipeline.ml.pair_train import _quality_gate


def test_pair_preprocessor_fits_train_statistics_and_maps_unknown_categories():
    train = pd.DataFrame(
        {
            "amount": [1.0, None, 9.0],
            "status": ["matched", "missing", "matched"],
        }
    )
    preprocessor = PairPreprocessor.fit(train, ["amount"], ["status"])
    matrix = preprocessor.transform(
        pd.DataFrame({"amount": [None, np.inf], "status": ["late", None]})
    )

    assert preprocessor.numeric_fill_values["amount"] == 5.0
    assert matrix.shape == (2, len(preprocessor.output_columns))
    assert np.isfinite(matrix).all()
    unknown = preprocessor.output_columns.index("status==__UNKNOWN__")
    assert matrix[:, unknown].tolist() == [1.0, 1.0]
    assert PairPreprocessor.from_dict(preprocessor.to_dict()) == preprocessor


def test_platt_calibrator_is_serializable_and_bounded():
    labels = np.array([0, 0, 0, 1, 1])
    probabilities = np.array([0.05, 0.1, 0.3, 0.6, 0.9])
    calibrator = PlattCalibrator.fit(labels, probabilities)
    restored = PlattCalibrator.from_dict(calibrator.to_dict())
    calibrated = restored.predict(probabilities)

    assert calibrator.method == "platt_validation"
    assert np.all((calibrated > 0) & (calibrated < 1))


def test_validation_threshold_respects_alert_budget():
    labels = np.array([0, 0, 1, 0, 1, 0, 0, 0, 0, 0])
    probabilities = np.array([0.1, 0.2, 0.9, 0.3, 0.8, 0.4, 0.5, 0.6, 0.7, 0.05])
    threshold = select_alert_budget_threshold(labels, probabilities, 0.2)

    assert int((probabilities >= threshold).sum()) <= 2
    assert threshold == 0.8


def test_binary_report_handles_a_single_class_without_fake_auc():
    report = binary_classification_report(
        [0, 0, 0],
        [0.1, 0.2, 0.3],
        threshold=0.25,
        max_alert_rate=0.34,
        hand_count=1,
    )

    assert report["roc_auc"] is None
    assert report["pr_auc"] is None
    assert report["recall_at_alert_budget"] is None
    assert report["alerts"] == 1


def test_rules_only_score_is_bounded_and_uses_inference_features_only():
    frame = pd.DataFrame(
        {
            "current_one_folded_other_won": [0, 1],
            "context_same_device": [0, 1],
            "context_same_network": [0, 1],
            "pair_outcome_asymmetry": [0.0, 2.0],
            "pair_a_fold_b_win_rate": [0.0, 1.0],
            "pair_b_fold_a_win_rate": [0.0, 0.5],
        }
    )
    scores = rules_only_score(frame)

    assert scores.tolist() == [0.0, 1.0]


def test_quality_gate_rejects_baseline_level_pr_auc_and_zero_recall():
    reports = {
        "catboost": {
            "test": {
                "pr_auc": 0.00101,
                "positive_rate": 0.001,
                "recall_at_alert_budget": 0.0,
                "f1": 0.0,
            },
            "challenge": {"pr_auc": 0.0011, "positive_rate": 0.001},
        },
        "rules_only": {"test": {"pr_auc": 0.00098}},
        "player_only": {"test": {"pr_auc": 0.00088}},
    }
    counts = {
        split: {"positives": positives}
        for split, positives in (("train", 350), ("validation", 74), ("test", 75))
    }

    gate = _quality_gate(counts, reports, {"p95_ms": 1.0})

    assert gate["promotion_eligible"] is False
    assert any("base rate" in reason for reason in gate["reasons"])
    assert any("zero test F1" in reason for reason in gate["reasons"])
