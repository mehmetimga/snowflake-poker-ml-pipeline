from __future__ import annotations

import json
import uuid
from pathlib import Path

import pytest
from pydantic import ValidationError

from pipeline.events import (
    REVIEW_DECISION_RECORDED,
    REVIEW_DECISIONS_TOPIC,
    ReviewDecisionEvent,
    contract_schema_bundle,
)
from pipeline.policy import (
    PolicyEvaluationInput,
    ReviewPolicyDefinition,
    evaluate_review_inputs,
    load_review_policy,
)


POLICY_PATH = Path("schemas/policies/review-policy-v1.json")
FIXTURE_PATH = Path("schemas/examples/review-policy-v1.golden.json")


def _input(**updates: object) -> PolicyEvaluationInput:
    raw = json.loads(FIXTURE_PATH.read_text())
    values: dict[str, object] = {
        key: value for key, value in raw.items() if key not in {"schema_version", "expected"}
    }
    values.update(updates)
    return PolicyEvaluationInput.model_validate(values)


def test_review_policy_matches_golden_and_is_replay_deterministic() -> None:
    policy = load_review_policy(POLICY_PATH)
    fixture = json.loads(FIXTURE_PATH.read_text())

    first = evaluate_review_inputs(_input(), policy)
    replay = evaluate_review_inputs(_input(), policy)

    assert str(first.event_id) == fixture["expected"]["decision_id"]
    assert first == replay
    assert first.payload.outcome == fixture["expected"]["outcome"]
    assert first.payload.action == fixture["expected"]["action"]
    assert first.payload.reason_codes == fixture["expected"]["reason_codes"]
    assert first.payload.rule_evidence[0].category == fixture["expected"]["rule_category"]
    assert "risk_probability" not in first.model_dump(mode="json")["payload"]

    corrected = evaluate_review_inputs(
        _input(risk_score_event_id=uuid.uuid5(uuid.NAMESPACE_URL, "corrected-score")),
        policy,
    )
    assert corrected.event_id != first.event_id


def test_soft_evidence_does_not_create_review_without_model_threshold() -> None:
    decision = evaluate_review_inputs(
        _input(model_threshold_exceeded=False), load_review_policy(POLICY_PATH)
    )

    assert decision.payload.outcome == "no_review"
    assert decision.payload.action == "none"
    assert decision.payload.reason_codes == []
    assert decision.payload.rule_evidence[0].category == "soft"


def test_future_hard_rule_requires_review_but_current_policy_has_none() -> None:
    raw = json.loads(POLICY_PATH.read_text())
    assert raw["hard_rules"] == []
    repeated = raw["soft_rules"].pop()
    raw["hard_rules"] = [repeated]
    hard_policy = ReviewPolicyDefinition.model_validate(raw)

    decision = evaluate_review_inputs(
        _input(model_threshold_exceeded=False), hard_policy
    )

    assert decision.payload.outcome == "mandatory_review"
    assert decision.payload.action == "analyst_review"
    assert decision.payload.reason_codes == [
        "hard-rule.pair.repeated-fold-to-partner-wins.v1"
    ]


def test_unknown_rule_and_inconsistent_decision_fail_closed() -> None:
    raw = json.loads(FIXTURE_PATH.read_text())
    raw["rule_evidence"][0]["rule_id"] = "pair.unknown"
    with pytest.raises(ValueError, match="not governed"):
        evaluate_review_inputs(
            PolicyEvaluationInput.model_validate(
                {key: value for key, value in raw.items() if key not in {"schema_version", "expected"}}
            ),
            load_review_policy(POLICY_PATH),
        )

    decision = evaluate_review_inputs(_input(), load_review_policy(POLICY_PATH))
    invalid = decision.model_dump(mode="python")
    invalid["payload"]["outcome"] = "no_review"
    with pytest.raises(ValidationError, match="outcome does not match"):
        ReviewDecisionEvent.model_validate(invalid)


def test_every_current_rule_is_explicitly_soft_and_quality_goes_to_dlq() -> None:
    policy = json.loads(POLICY_PATH.read_text())
    stateless = json.loads(Path("schemas/rules/pair-rules-v1.json").read_text())
    stateful = json.loads(Path("schemas/rules/stateful-pair-rules-v1.json").read_text())
    current = {
        (value["rule_id"], value["rule_version"])
        for value in (*stateless["rules"], *stateful["rules"])
    }
    soft = {
        (value["rule_id"], value["rule_version"])
        for value in policy["soft_rules"]
    }

    assert soft == current
    assert policy["hard_rules"] == []
    assert policy["data_quality_behavior"] == "dead_letter"


def test_schema_bundle_and_static_schema_publish_review_decision() -> None:
    bundle = contract_schema_bundle()
    schema = json.loads(
        Path("schemas/events/poker.review-decision.v1.schema.json").read_text()
    )

    assert REVIEW_DECISION_RECORDED in bundle["derived_events"]
    assert (
        bundle["derived_topics"][REVIEW_DECISION_RECORDED]
        == REVIEW_DECISIONS_TOPIC
    )
    assert schema["properties"]["event_type"]["const"] == REVIEW_DECISION_RECORDED
    assert schema["$defs"]["reviewDecisionPayload"]["properties"]["outcome"] == {
        "enum": ["no_review", "review_recommended", "mandatory_review"]
    }
