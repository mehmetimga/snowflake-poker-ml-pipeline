from __future__ import annotations

import json
import uuid
from datetime import datetime
from pathlib import Path

import pytest

from pipeline.rules import (
    REPEATED_FOLD_RULE_CONFIG,
    RepeatedFoldWindowRule,
    StatefulPairObservation,
)


FIXTURE_PATH = Path("schemas/examples/stateful-fold-rule-v1.golden.json")
DEFINITIONS_PATH = Path("schemas/rules/stateful-pair-rules-v1.json")


def _observation(
    scope: dict[str, str], raw: dict[str, object]
) -> StatefulPairObservation:
    return StatefulPairObservation(
        event_id=uuid.UUID(str(raw["event_id"])),
        tenant_id=scope["tenant_id"],
        product_id=scope["product_id"],
        dataset_id=scope["dataset_id"],
        dataset_split=scope["dataset_split"],
        trace_id=uuid.UUID(scope["trace_id"]),
        hand_id=str(raw["hand_id"]),
        pair_key=scope["pair_key"],
        played_at=datetime.fromisoformat(str(raw["played_at"])),
        emitted_at=datetime.fromisoformat(str(raw["emitted_at"])),
        snapshot_revision=int(raw["snapshot_revision"]),
        a_fold_b_win=bool(raw["a_fold_b_win"]),
        b_fold_a_win=bool(raw["b_fold_a_win"]),
    )


def _result_identity(result) -> tuple[object, ...]:
    return (
        result.status,
        result.window_hand_count,
        result.directional_count,
        result.directional_rate,
        str(result.evidence_event.event_id) if result.evidence_event else None,
    )


def test_stateful_rule_definition_is_frozen_and_never_blends_probability() -> None:
    governed = json.loads(DEFINITIONS_PATH.read_text())["rules"][0]

    assert governed["rule_id"] == REPEATED_FOLD_RULE_CONFIG.rule_id
    assert governed["rule_version"] == REPEATED_FOLD_RULE_CONFIG.rule_version
    assert governed["window_hours"] == REPEATED_FOLD_RULE_CONFIG.window_hours
    assert governed["minimum_hands"] == REPEATED_FOLD_RULE_CONFIG.minimum_hands
    assert governed["minimum_directional_count"] == (
        REPEATED_FOLD_RULE_CONFIG.minimum_directional_count
    )
    assert governed["directional_rate_threshold"] == (
        REPEATED_FOLD_RULE_CONFIG.directional_rate_threshold
    )
    assert governed["probability_blend"] is False


def test_python_stateful_rule_matches_golden_replay_and_checkpoint_restore() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text())
    rule = RepeatedFoldWindowRule()
    restored = None

    for index, raw in enumerate(fixture["operations"]):
        observation = _observation(fixture["scope"], raw)
        watermark = (
            datetime.fromisoformat(raw["watermark"]) if raw.get("watermark") else None
        )
        result = rule.evaluate(observation, watermark=watermark)
        assert result.status == raw["expected_status"]
        assert result.window_hand_count == raw["expected_hands"]
        assert result.directional_count == raw["expected_count"]
        assert result.directional_rate == pytest.approx(raw["expected_rate"])
        assert (
            str(result.evidence_event.event_id) if result.evidence_event else None
        ) == raw["expected_rule_event_id"]
        if result.evidence_event:
            assert result.evidence_event.payload.observation_revision == (
                observation.snapshot_revision
            )
            assert "probability" not in json.dumps(
                result.evidence_event.payload.evidence
            )

        if restored is not None:
            assert _result_identity(
                restored.evaluate(observation, watermark=watermark)
            ) == _result_identity(result)
        if index == 3:
            restored = RepeatedFoldWindowRule.restore(rule.snapshot())

    assert rule.state_size == 6
    assert restored is not None
    assert restored.snapshot() == rule.snapshot()


def test_stateful_rule_rejects_conflicting_same_revision() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text())
    rule = RepeatedFoldWindowRule()
    original = _observation(fixture["scope"], fixture["operations"][0])
    rule.evaluate(original)
    conflicting = StatefulPairObservation(
        **{**original.__dict__, "a_fold_b_win": False}
    )

    with pytest.raises(ValueError, match="conflicting rule inputs"):
        rule.evaluate(conflicting)
