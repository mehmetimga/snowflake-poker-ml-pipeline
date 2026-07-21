from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline.rules.evaluation import (
    RuleEvaluationConfig,
    hand_grouped_rule_intervals,
    independent_label_mask,
    repeated_fold_rule_firings,
    rule_point_metrics,
)


ROOT = Path(__file__).resolve().parents[1]


def _config(samples: int = 30) -> RuleEvaluationConfig:
    raw = json.loads((ROOT / "schemas/rules/rule-evaluation-v1.json").read_text())
    raw["bootstrap"]["samples"] = samples
    return RuleEvaluationConfig.from_mapping(raw)


def test_circular_labels_are_excluded_and_unknown_provenance_fails() -> None:
    included, audit = independent_label_mask(
        ["synthetic", "rule_derived", "synthetic"], _config()
    )
    assert included.tolist() == [True, False, True]
    assert audit["excluded_circular_rows"] == 1
    assert audit["circular_rule_labels_used_as_truth"] is False
    with pytest.raises(ValueError, match="unknown label provenance"):
        independent_label_mask(["analyst_guess"], _config())


def test_rule_intervals_are_deterministic_and_sample_whole_hands() -> None:
    hands = np.repeat([f"hand-{index}" for index in range(20)], 3)
    labels = np.tile([1, 0, 0], 20)
    fired = np.tile([True, True, False], 20)
    config = _config(samples=25)
    first = hand_grouped_rule_intervals(labels, fired, hands, config)
    second = hand_grouped_rule_intervals(labels, fired, hands, config)
    assert first == second
    assert first["sampling_unit"] == "hand_id"
    assert first["all_rows_share_hand_multiplicity"] is True
    assert first["metrics"]["precision"]["effective_samples"] == 25
    point = rule_point_metrics(labels, fired, hands)
    assert point["precision"] == 0.5
    assert point["recall"] == 1.0


def test_stateful_fold_rule_replays_in_event_time() -> None:
    start = pd.Timestamp("2026-07-21T00:00:00Z")
    frame = pd.DataFrame(
        {
            "event_id": [f"event-{index}" for index in range(6)],
            "pair_key": ["a:b"] * 6,
            "played_at": [start + pd.Timedelta(hours=index) for index in range(6)],
            "current_fold_actions_a": [1, 1, 1, 0, 0, 1],
            "current_fold_actions_b": [0] * 6,
            "current_won_amount_a": [0] * 6,
            "current_won_amount_b": [10, 10, 10, 0, 0, 10],
        }
    )
    definition = json.loads(
        (ROOT / "schemas/rules/stateful-pair-rules-v1.json").read_text()
    )["rules"][0]
    fired = repeated_fold_rule_firings(frame, definition)
    assert fired.tolist() == [False, False, False, False, True, True]
