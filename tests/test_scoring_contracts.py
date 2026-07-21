from __future__ import annotations

import uuid
from datetime import datetime, timezone
from itertools import combinations

import pytest
from pydantic import ValidationError

from pipeline.events import (
    RISK_ALERT_CREATED,
    RISK_ALERTS_TOPIC,
    RISK_SCORE_COMPUTED,
    RISK_SCORES_TOPIC,
    REVIEW_DECISION_RECORDED,
    REVIEW_DECISIONS_TOPIC,
    RULE_EVIDENCE_RECORDED,
    RULE_EVIDENCE_TOPIC,
    PairRiskScore,
    PlayerRiskScore,
    RiskAlertEvent,
    RiskAlertPayload,
    RiskScoreEvent,
    RiskScorePayload,
    assert_inference_safe,
    contract_schema_bundle,
    stable_review_decision_id,
)
from pipeline.kafka.topics import ScoringTopics, scoring_topic_specs


NOW = datetime(2026, 7, 20, tzinfo=timezone.utc)


def _scores() -> tuple[list[PairRiskScore], list[PlayerRiskScore]]:
    players = ["a", "b", "c", "d", "e", "f"]
    pair_scores = [
        PairRiskScore(
            feature_event_id=uuid.uuid5(uuid.NAMESPACE_URL, pair_key),
            pair_key=pair_key,
            player_a=left,
            player_b=right,
            snapshot_revision=1,
            raw_probability=0.9 if pair_key == "a:b" else 0.1,
            calibrated_probability=0.9 if pair_key == "a:b" else 0.1,
            alert=pair_key == "a:b",
        )
        for left, right in combinations(players, 2)
        for pair_key in [f"{left}:{right}"]
    ]
    player_scores = [
        PlayerRiskScore(
            player_id=player,
            risk_probability=0.9 if player in {"a", "b"} else 0.1,
            alert=player in {"a", "b"},
        )
        for player in players
    ]
    return pair_scores, player_scores


def _risk_score_event() -> RiskScoreEvent:
    pair_scores, player_scores = _scores()
    return RiskScoreEvent(
        event_id=uuid.uuid5(uuid.NAMESPACE_URL, "score-event"),
        tenant_id="tenant",
        product_id="poker",
        dataset_id="dataset",
        dataset_split="test",
        occurred_at=NOW,
        emitted_at=NOW,
        trace_id=uuid.uuid5(uuid.NAMESPACE_URL, "trace"),
        payload=RiskScorePayload(
            score_id="0123456789abcdef0123456789abcdef",
            hand_id="hand-1",
            table_id="table-1",
            played_at=NOW,
            model_name="pair-catboost-v1",
            model_run_id="pair_test_run",
            decision_policy_version=1,
            decision_threshold=0.8,
            service_implementation="go-risk-scorer",
            service_build_version="test-build",
            scored_at=NOW,
            pair_scores=pair_scores,
            player_scores=player_scores,
            hand_risk_probability=0.9,
            alert=True,
        ),
    )


def test_risk_score_and_alert_contracts_are_inference_safe():
    score = _risk_score_event()
    rule_event_id = uuid.UUID("8bcfb4e4-2113-52c3-85c2-a6ca4cb19823")
    score = score.model_copy(
        update={
            "payload": score.payload.model_copy(
                update={"rule_evidence_event_ids": [rule_event_id]}
            )
        }
    )
    highest = score.payload.pair_scores[0]
    alert_id = uuid.uuid5(uuid.NAMESPACE_URL, "alert-event")
    decision_id = stable_review_decision_id(
        tenant_id=score.tenant_id,
        product_id=score.product_id,
        dataset_id=score.dataset_id,
        dataset_split=score.dataset_split,
        policy_id="poker.review-routing",
        policy_version=1,
        risk_score_event_id=score.event_id,
    )
    alert = RiskAlertEvent(
        event_id=alert_id,
        tenant_id=score.tenant_id,
        product_id=score.product_id,
        dataset_id=score.dataset_id,
        dataset_split=score.dataset_split,
        occurred_at=NOW,
        emitted_at=NOW,
        trace_id=score.trace_id,
        payload=RiskAlertPayload(
            alert_id=alert_id,
            risk_score_event_id=score.event_id,
            score_id=score.payload.score_id,
            hand_id=score.payload.hand_id,
            table_id=score.payload.table_id,
            played_at=NOW,
            model_name=score.payload.model_name,
            model_run_id=score.payload.model_run_id,
            decision_policy_version=score.payload.decision_policy_version,
            decision_threshold=0.8,
            service_implementation=score.payload.service_implementation,
            service_build_version=score.payload.service_build_version,
            review_decision_event_id=decision_id,
            review_policy_id="poker.review-routing",
            review_policy_version=1,
            review_policy_mode="shadow",
            policy_outcome="review_recommended",
            policy_reason_codes=["model.threshold-exceeded"],
            rule_evidence_event_ids=score.payload.rule_evidence_event_ids,
            risk_probability=0.9,
            highest_risk_pair=highest,
            highest_risk_players=score.payload.player_scores[:2],
            scored_at=NOW,
        ),
    )

    assert score.event_type == RISK_SCORE_COMPUTED
    assert alert.event_type == RISK_ALERT_CREATED
    assert score.payload.rule_evidence_event_ids == alert.payload.rule_evidence_event_ids
    assert_inference_safe(score.model_dump(mode="json"))
    assert_inference_safe(alert.model_dump(mode="json"))


def test_risk_score_rejects_threshold_inconsistent_decision():
    value = _risk_score_event().model_dump(mode="python")
    value["payload"]["alert"] = False
    with pytest.raises(ValidationError, match="decision threshold"):
        RiskScoreEvent.model_validate(value)


def test_risk_score_rejects_duplicate_rule_evidence_references():
    value = _risk_score_event().model_dump(mode="python")
    reference = uuid.UUID("8bcfb4e4-2113-52c3-85c2-a6ca4cb19823")
    value["payload"]["rule_evidence_event_ids"] = [reference, reference]
    with pytest.raises(ValidationError, match="references must be unique"):
        RiskScoreEvent.model_validate(value)


def test_schema_bundle_and_scoring_topics_are_versioned():
    bundle = contract_schema_bundle()
    assert RISK_SCORE_COMPUTED in bundle["derived_events"]
    assert RISK_ALERT_CREATED in bundle["derived_events"]
    assert REVIEW_DECISION_RECORDED in bundle["derived_events"]
    assert bundle["derived_topics"][RISK_SCORE_COMPUTED] == RISK_SCORES_TOPIC
    assert bundle["derived_topics"][RISK_ALERT_CREATED] == RISK_ALERTS_TOPIC
    assert bundle["derived_topics"][RULE_EVIDENCE_RECORDED] == RULE_EVIDENCE_TOPIC
    assert bundle["derived_topics"][REVIEW_DECISION_RECORDED] == REVIEW_DECISIONS_TOPIC

    topics = ScoringTopics(
        risk_scores="scores-v1", rule_evidence="rules-v1",
        review_decisions="decisions-v1",
        risk_alerts="alerts-v1", dead_letters="dlq-v1"
    )
    specs = scoring_topic_specs(topics, partitions=2, replication_factor=1)
    assert [spec.name for spec in specs] == [
        "scores-v1", "rules-v1", "decisions-v1", "alerts-v1", "dlq-v1"
    ]
    assert all(spec.configs["retention.ms"] == "2592000000" for spec in specs)
