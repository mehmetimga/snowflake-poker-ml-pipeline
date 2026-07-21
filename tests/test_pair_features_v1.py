from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from pipeline.context import enrich_player_hand
from pipeline.events import (
    HAND_COMPLETED,
    PAIR_FEATURES_COMPUTED,
    PAIR_FEATURES_TOPIC,
    USER_CONTEXT_UPDATED,
    HandCompletedPayload,
    PairFeatureEvent,
    UserContextPayload,
    build_event,
    contract_schema_bundle,
)
from pipeline.features import PairFeatureCore
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.generator.dataset import separate_hand_labels


def _hand_events(n_hands: int = 2):
    events = []
    generator = HandGenerator(
        GeneratorConfig(
            n_hands=n_hands,
            n_players=6,
            n_tables=1,
            n_colluding_pairs=1,
            seed=1201,
            dataset_id="pair-feature-test-v1",
            dataset_split="train",
        )
    )
    for raw_hand in generator.iter_hands():
        hand, _ = separate_hand_labels(raw_hand)
        payload = HandCompletedPayload.model_validate(hand)
        events.append(
            build_event(
                event_type=HAND_COMPLETED,
                aggregate_id=payload.hand_id,
                payload=payload,
                dataset_id="pair-feature-test-v1",
                dataset_split="train",
                occurred_at=payload.played_at,
            )
        )
    return events


def _context(player_id: str, played_at: datetime, version: int = 1):
    payload = UserContextPayload(
        user_id=player_id,
        context_version=version,
        effective_at=played_at - timedelta(days=2),
        account_created_at=played_at - timedelta(days=365 + version),
        country_bucket="TR",
        timezone="Europe/Istanbul",
        acquisition_channel="organic",
        kyc_level="verified",
        account_status="active",
        bankroll_bucket="medium",
        preferred_stake_bucket="low",
        skill_rating=0.4 + version / 10,
        device_id=f"device-{player_id}",
        network_cluster_id="shared-network",
    )
    return build_event(
        event_type=USER_CONTEXT_UPDATED,
        aggregate_id=f"{player_id}:context:{version}",
        payload=payload,
        dataset_id="pair-feature-test-v1",
        dataset_split="train",
        occurred_at=payload.effective_at,
    )


def _enriched_hand(hand, *, missing_player: str | None = None):
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    return [
        enrich_player_hand(
            hand,
            player_id=player["player_id"],
            context_event=(
                None
                if player["player_id"] == missing_player
                else _context(player["player_id"], played_at)
            ),
            emitted_at=played_at + timedelta(seconds=1),
        )
        for player in hand.payload["players"]
    ]


def test_six_player_hand_expands_to_fifteen_versioned_pair_snapshots():
    hand = _hand_events(1)[0]
    rows = PairFeatureCore().process_many(_enriched_hand(hand))

    assert len(rows) == 15
    assert len({row.payload.pair_key for row in rows}) == 15
    assert {row.event_type for row in rows} == {PAIR_FEATURES_COMPUTED}
    assert {row.payload.snapshot_revision for row in rows} == {1}
    assert all(row.payload.player_a < row.payload.player_b for row in rows)
    assert all(row.payload.pair_history.hands_together == 0 for row in rows)
    assert all(row.payload.user_history_a.hands_seen == 0 for row in rows)


def test_second_hand_uses_prior_only_user_and_pair_history():
    first, second = _hand_events(2)
    core = PairFeatureCore()
    first_rows = core.process_many(_enriched_hand(first))
    second_rows = core.process_many(_enriched_hand(second))

    shared_pair = first_rows[0].payload.pair_key
    row = next(value for value in second_rows if value.payload.pair_key == shared_pair)
    assert row.payload.pair_history.hands_together == 1
    assert row.payload.pair_history.last_seen_age_seconds is not None
    assert row.payload.user_history_a.hands_seen == 1
    assert row.payload.user_history_b.hands_seen == 1


def test_context_correction_reemits_only_five_affected_pairs_without_advancing_history():
    hand = _hand_events(1)[0]
    player_id = hand.payload["players"][0]["player_id"]
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    core = PairFeatureCore()
    initial = core.process_many(_enriched_hand(hand, missing_player=player_id))
    correction = enrich_player_hand(
        hand,
        player_id=player_id,
        context_event=_context(player_id, played_at, version=2),
        corrected=True,
        revision=2,
        emitted_at=played_at + timedelta(seconds=5),
    )

    corrected = core.process(correction)
    duplicate = core.process(correction)

    assert len(initial) == 15
    assert len(corrected) == 5
    assert duplicate == []
    assert all(player_id in (row.payload.player_a, row.payload.player_b) for row in corrected)
    assert {row.payload.snapshot_revision for row in corrected} == {2}
    assert all(row.payload.pair_history.hands_together == 0 for row in corrected)
    corrected_side = [
        row.payload.context.context_missing_a
        if row.payload.player_a == player_id
        else row.payload.context.context_missing_b
        for row in corrected
    ]
    assert corrected_side == [False] * 5


def test_pair_feature_replay_identifiers_and_payloads_are_deterministic():
    enriched = _enriched_hand(_hand_events(1)[0])

    first = PairFeatureCore().process_many(enriched)
    replay = PairFeatureCore().process_many(enriched)

    assert [row.event_id for row in first] == [row.event_id for row in replay]
    assert [row.payload for row in first] == [row.payload for row in replay]


def test_pair_feature_contract_is_exported_for_non_python_consumers():
    bundle = contract_schema_bundle()

    assert PAIR_FEATURES_COMPUTED in bundle["derived_events"]
    assert bundle["derived_topics"][PAIR_FEATURES_COMPUTED] == PAIR_FEATURES_TOPIC


def test_pair_feature_contract_rejects_emission_before_hand_time():
    row = PairFeatureCore().process_many(_enriched_hand(_hand_events(1)[0]))[0]
    raw = row.model_dump(mode="json")
    raw["emitted_at"] = row.occurred_at - timedelta(microseconds=1)

    with pytest.raises(
        ValueError,
        match="pair-feature emitted_at cannot precede occurred_at",
    ):
        PairFeatureEvent.model_validate(raw)
