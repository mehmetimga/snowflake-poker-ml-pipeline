from __future__ import annotations

import numpy as np
import pandas as pd

from pipeline.ops.drift import (
    evaluate_drift,
    numeric_reference,
    population_stability_index,
)
from pipeline.ops.feedback import AnalystFeedback


def test_population_stability_index_is_zero_for_identical_distribution() -> None:
    assert population_stability_index([0.2, 0.3, 0.5], [0.2, 0.3, 0.5]) == 0.0


def test_drift_report_marks_large_shift_critical() -> None:
    reference = {
        "dataset_id": "dataset",
        "benchmark": "cold_start",
        "thresholds": {"warning_psi": 0.1, "critical_psi": 0.25},
        "numeric_features": {"value": numeric_reference(np.arange(100), bins=5)},
        "categorical_features": {"kind": {"proportions": {"a": 1.0}}},
        "score": numeric_reference(np.linspace(0, 1, 100), bins=5),
    }
    frame = pd.DataFrame({"value": np.arange(100) + 1000, "kind": ["b"] * 100})
    report = evaluate_drift(
        reference, frame, np.ones(100), split="test", model_name="model", model_run_id="run"
    )
    assert report["status"] == "critical"
    assert report["summary"]["critical_checks"] >= 2


def test_analyst_feedback_becomes_delayed_tenant_scoped_label() -> None:
    feedback = AnalystFeedback(
        tenant_id="tenant-a", product_id="poker", feedback_id="feedback-1",
        risk_score_event_id="score-1", hand_id="hand-1", pair_key="a:b",
        model_run_id="run-1",
        disposition="confirmed_collusion", confidence=0.9,
        reason_code="shared_device_and_chip_flow", evidence={"case_id": "case-1"},
        analyst_subject="analyst:42", reviewed_at="2026-07-20T10:00:00Z",
        label_available_at="2026-07-20T10:01:00Z",
    )
    label = feedback.training_label()
    assert label is not None
    assert label["target"] == 1
    assert label["tenant_id"] == "tenant-a"
    assert label["pair_key"] == "a:b"
    assert label["label_available_at"] == "2026-07-20T10:01:00Z"
