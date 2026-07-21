from __future__ import annotations

import json
import uuid
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import pytest
from pydantic import ValidationError

from pipeline.events import (
    CurrentHandPairFeatures,
    PairContextFeatures,
    PairFeatureEvent,
    PairFeaturePayload,
    PairHistoryFeatures,
    UserHistoryFeatures,
    build_rule_evidence_event,
)
from pipeline.ml.pair_model import rules_only_score
from pipeline.rules import (
    PAIR_RULE_DEFINITIONS,
    evaluate_pair_rules,
    rules_only_pair_score,
)


GOLDEN_PATH = Path("schemas/examples/pair-rules-v1.golden.json")
DEFINITIONS_PATH = Path("schemas/rules/pair-rules-v1.json")


def _golden_event() -> tuple[PairFeatureEvent, dict[str, object]]:
    fixture = json.loads(GOLDEN_PATH.read_text())
    value = fixture["input"]
    played_at = datetime.fromisoformat(value["played_at"])
    user_history = UserHistoryFeatures(
        hands_seen=10,
        total_won_amount=100,
        mean_won_amount=10,
        fold_rate=0.2,
        raise_rate=0.3,
        saw_flop_rate=0.5,
    )
    payload = PairFeaturePayload(
        hand_id=value["hand_id"],
        table_id=value["table_id"],
        played_at=played_at,
        pair_key=value["pair_key"],
        player_a=value["player_a"],
        player_b=value["player_b"],
        num_players=6,
        source_hand_event_id=uuid.UUID("30000000-0000-5000-8000-000000000003"),
        source_player_context_event_id_a=uuid.UUID(
            "40000000-0000-5000-8000-000000000004"
        ),
        source_player_context_event_id_b=uuid.UUID(
            "50000000-0000-5000-8000-000000000005"
        ),
        source_revision_a=1,
        source_revision_b=1,
        context_status_a="matched",
        context_status_b="matched",
        context_version_a=1,
        context_version_b=1,
        snapshot_revision=value["snapshot_revision"],
        current_hand=CurrentHandPairFeatures(
            position_index_a=0,
            position_index_b=1,
            position_gap=1,
            invested_amount_a=10,
            invested_amount_b=20,
            invested_pot_ratio_a=0.1,
            invested_pot_ratio_b=0.2,
            invested_abs_diff_ratio=0.1,
            won_amount_a=0,
            won_amount_b=100,
            outcome_abs_diff_ratio=1,
            aggressive_actions_a=0,
            aggressive_actions_b=1,
            fold_actions_a=1,
            fold_actions_b=0,
            both_saw_flop=False,
            both_saw_river=False,
            one_folded_other_won=value["current_hand"]["one_folded_other_won"],
        ),
        context=PairContextFeatures(
            context_missing_a=False,
            context_missing_b=False,
            skill_rating_a=0.4,
            skill_rating_b=0.5,
            skill_rating_abs_diff=0.1,
            account_age_days_a=300,
            account_age_days_b=200,
            account_age_abs_diff_days=100,
            same_country=True,
            same_timezone=True,
            same_acquisition_channel=False,
            same_device=value["context"]["same_device"],
            same_network=value["context"]["same_network"],
            bankroll_bucket_distance=0,
            preferred_stake_bucket_distance=1,
        ),
        user_history_a=user_history,
        user_history_b=user_history,
        pair_history=PairHistoryFeatures(
            hands_together=8,
            total_won_amount_a=100,
            total_won_amount_b=300,
            outcome_asymmetry=value["pair_history"]["outcome_asymmetry"],
            a_fold_b_win_rate=value["pair_history"]["a_fold_b_win_rate"],
            b_fold_a_win_rate=value["pair_history"]["b_fold_a_win_rate"],
            both_saw_flop_rate=0.5,
            same_table_rate=0.75,
            last_seen_age_seconds=60,
        ),
    )
    return (
        PairFeatureEvent(
            event_id=uuid.UUID(value["event_id"]),
            tenant_id=value["tenant_id"],
            product_id=value["product_id"],
            dataset_id=value["dataset_id"],
            dataset_split=value["dataset_split"],
            occurred_at=played_at,
            emitted_at=datetime.fromisoformat(value["emitted_at"]),
            trace_id=uuid.UUID(value["trace_id"]),
            payload=payload,
        ),
        fixture,
    )


def test_governed_definitions_match_python_constants() -> None:
    governed = json.loads(DEFINITIONS_PATH.read_text())

    assert governed["feature_definition_version"] == "pair-features-v1"
    assert governed["rules_only_benchmark"]["probability_blend"] is False
    assert governed["rules"] == [asdict(value) for value in PAIR_RULE_DEFINITIONS]


def test_python_rule_evaluator_matches_cross_language_golden_fixture() -> None:
    event, fixture = _golden_event()
    emitted_at = datetime.fromisoformat(fixture["input"]["rule_emitted_at"])

    fired = evaluate_pair_rules(event, emitted_at=emitted_at)
    expected = fixture["expected"]["fired_rules"]

    assert [value.payload.rule_id for value in fired] == [
        value["rule_id"] for value in expected
    ]
    assert [value.payload.raw_score for value in fired] == [
        value["raw_score"] for value in expected
    ]
    assert [str(value.event_id) for value in fired] == [
        value["rule_event_id"] for value in expected
    ]
    assert rules_only_pair_score(event) == pytest.approx(
        fixture["expected"]["rules_only_score"]
    )


def test_rules_only_pair_score_preserves_existing_dataframe_benchmark() -> None:
    event, _ = _golden_event()
    payload = event.payload
    frame = pd.DataFrame(
        {
            "current_one_folded_other_won": [payload.current_hand.one_folded_other_won],
            "context_same_device": [payload.context.same_device],
            "context_same_network": [payload.context.same_network],
            "pair_outcome_asymmetry": [payload.pair_history.outcome_asymmetry],
            "pair_a_fold_b_win_rate": [payload.pair_history.a_fold_b_win_rate],
            "pair_b_fold_a_win_rate": [payload.pair_history.b_fold_a_win_rate],
        }
    )

    assert rules_only_pair_score(event) == rules_only_score(frame)[0]


def test_rule_replay_is_idempotent_and_zero_signals_do_not_fire() -> None:
    event, fixture = _golden_event()
    emitted_at = datetime.fromisoformat(fixture["input"]["rule_emitted_at"])
    first = evaluate_pair_rules(event, emitted_at=emitted_at)
    replay = evaluate_pair_rules(event, emitted_at=emitted_at + timedelta(minutes=1))
    assert [value.event_id for value in first] == [value.event_id for value in replay]

    corrected_payload = event.payload.model_copy(update={"snapshot_revision": 4})
    corrected = event.model_copy(update={"payload": corrected_payload})
    corrected_rules = evaluate_pair_rules(corrected, emitted_at=emitted_at)
    assert [value.event_id for value in first] != [
        value.event_id for value in corrected_rules
    ]
    assert {value.payload.observation_revision for value in corrected_rules} == {4}

    payload = event.payload.model_copy(
        update={
            "current_hand": event.payload.current_hand.model_copy(
                update={"one_folded_other_won": False}
            ),
            "context": event.payload.context.model_copy(
                update={"same_device": False, "same_network": False}
            ),
            "pair_history": event.payload.pair_history.model_copy(
                update={
                    "outcome_asymmetry": 0.0,
                    "a_fold_b_win_rate": 0.0,
                    "b_fold_a_win_rate": 0.0,
                }
            ),
        }
    )
    zero_event = event.model_copy(update={"payload": payload})

    assert evaluate_pair_rules(zero_event, emitted_at=emitted_at) == []
    assert rules_only_pair_score(zero_event) == 0.0


def test_pair_feature_transport_validates_upstream_flink_rule_evidence() -> None:
    event, _ = _golden_event()
    evidence = build_rule_evidence_event(
        tenant_id=event.tenant_id,
        product_id=event.product_id,
        dataset_id=event.dataset_id,
        dataset_split=event.dataset_split,
        trace_id=event.trace_id,
        rule_id="pair.repeated-fold-to-partner-wins",
        rule_version=1,
        rule_owner="risk-analytics",
        entity_type="pair",
        entity_key=event.payload.pair_key,
        hand_id=event.payload.hand_id,
        observation_revision=event.payload.snapshot_revision,
        severity="high",
        raw_score=60,
        evidence={"window_hand_count": 5, "directional_fold_win_rate": 0.6},
        effective_at=event.payload.played_at,
        emitted_at=event.emitted_at,
    )
    value = event.model_dump(mode="json")
    value["upstream_rule_evidence"] = [evidence.model_dump(mode="json")]

    validated = PairFeatureEvent.model_validate(value)
    assert validated.upstream_rule_evidence[0]["event_id"] == str(evidence.event_id)

    value["upstream_rule_evidence"][0]["payload"]["observation_revision"] = 4
    with pytest.raises(ValidationError, match="deterministic replay identity"):
        PairFeatureEvent.model_validate(value)
