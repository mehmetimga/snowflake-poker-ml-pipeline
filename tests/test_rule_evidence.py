from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.events import (
    RULE_EVIDENCE_RECORDED,
    RULE_EVIDENCE_TOPIC,
    RuleEvidenceEvent,
    build_rule_evidence_event,
    contract_schema_bundle,
    rule_evidence_partition_key,
)


FIXTURE_PATH = Path("schemas/examples/poker.rule-evidence.v1.json")


def _fixture() -> dict[str, object]:
    return json.loads(FIXTURE_PATH.read_text())


def _build() -> RuleEvidenceEvent:
    return build_rule_evidence_event(
        tenant_id="tenant-a",
        product_id="poker",
        dataset_id="context-full-v2",
        dataset_split="test",
        trace_id=uuid.UUID("11111111-1111-5111-8111-111111111111"),
        rule_id="pair.fold-benefit",
        rule_version=1,
        rule_owner="risk-analytics",
        entity_type="pair",
        entity_key="player-a:player-b",
        hand_id="H-000042",
        severity="high",
        raw_score=72.0,
        evidence={
            "one_folded_other_won": True,
            "hands_together": 42,
            "directional_fold_win_rate": 0.72,
            "rule_threshold": 0.6,
        },
        effective_at=datetime(2026, 7, 21, 10, 15, tzinfo=timezone.utc),
        emitted_at=datetime(
            2026, 7, 21, 10, 15, 0, 250_000, tzinfo=timezone.utc
        ),
    )


def test_shared_fixture_round_trips_and_replays_deterministically() -> None:
    fixture = _fixture()
    parsed = RuleEvidenceEvent.model_validate(fixture)
    first = _build()
    second = _build()

    assert parsed == first == second
    assert first.event_id == uuid.UUID("8bcfb4e4-2113-52c3-85c2-a6ca4cb19823")
    assert rule_evidence_partition_key(first) == "pair:player-a:player-b"
    assert first.model_dump(mode="json") == fixture


@pytest.mark.parametrize(
    "field",
    [
        "is_collusive",
        "hand_risk_probability",
        "decision_policy_version",
        "private_challenge_labels",
        "alert",
    ],
)
def test_rule_evidence_rejects_labels_model_probability_and_policy_output(
    field: str,
) -> None:
    value = _fixture()
    value["payload"]["evidence"]["nested"] = {field: True}  # type: ignore[index]

    with pytest.raises(ValidationError, match="forbidden rule-evidence field"):
        RuleEvidenceEvent.model_validate(value)


def test_rule_event_id_rejects_semantic_identity_mutation() -> None:
    value = _fixture()
    value["payload"]["rule_version"] = 2  # type: ignore[index]

    with pytest.raises(ValidationError, match="deterministic replay identity"):
        RuleEvidenceEvent.model_validate(value)


def test_schema_bundle_and_static_schema_publish_rule_contract() -> None:
    bundle = contract_schema_bundle()
    schema = json.loads(
        Path("schemas/events/poker.rule-evidence.v1.schema.json").read_text()
    )

    assert RULE_EVIDENCE_RECORDED in bundle["derived_events"]
    assert bundle["derived_topics"][RULE_EVIDENCE_RECORDED] == RULE_EVIDENCE_TOPIC
    assert schema["properties"]["event_type"]["const"] == RULE_EVIDENCE_RECORDED
    assert schema["properties"]["payload"]["properties"]["raw_score"] == {
        "type": "number",
        "minimum": 0,
        "maximum": 100,
    }
