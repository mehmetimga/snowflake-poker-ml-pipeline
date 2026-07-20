from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pipeline.context import TemporalContextJoinCore, select_context_as_of
from pipeline.events import (
    HAND_COMPLETED,
    PLAYER_HAND_CONTEXT_ENRICHED,
    USER_CONTEXT_UPDATED,
    HandCompletedPayload,
    UserContextPayload,
    build_event,
    contract_schema_bundle,
)
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.generator.dataset import separate_hand_labels


def _hand_event():
    hand, _ = separate_hand_labels(
        next(
            HandGenerator(
                GeneratorConfig(
                    n_hands=1,
                    n_players=12,
                    n_tables=2,
                    n_colluding_pairs=2,
                    seed=811,
                    dataset_split="train",
                    dataset_id="temporal-test-v1",
                )
            ).iter_hands()
        )
    )
    payload = HandCompletedPayload.model_validate(hand)
    return build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id="temporal-test-v1",
        dataset_split="train",
        occurred_at=payload.played_at,
    )


def _context_event(
    user_id: str,
    *,
    version: int,
    effective_at: datetime,
    bankroll_bucket: str = "medium",
):
    payload = UserContextPayload(
        user_id=user_id,
        context_version=version,
        effective_at=effective_at,
        account_created_at=effective_at - timedelta(days=365),
        country_bucket="TR",
        timezone="Europe/Istanbul",
        acquisition_channel="organic",
        kyc_level="verified",
        account_status="active",
        bankroll_bucket=bankroll_bucket,
        preferred_stake_bucket="low",
        skill_rating=0.55,
        device_id=f"device-{user_id}",
        network_cluster_id="network-1",
    )
    return build_event(
        event_type=USER_CONTEXT_UPDATED,
        aggregate_id=f"{user_id}:context:{version}",
        payload=payload,
        dataset_id="temporal-test-v1",
        dataset_split="train",
        occurred_at=effective_at,
    )


def _epoch_ms(value: datetime) -> int:
    return int(value.timestamp() * 1_000)


def test_select_context_as_of_uses_effective_time_not_version_order():
    hand = _hand_event()
    player_id = hand.payload["players"][0]["player_id"]
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    events = [
        _context_event(player_id, version=1, effective_at=played_at - timedelta(hours=2)),
        _context_event(player_id, version=3, effective_at=played_at - timedelta(hours=1)),
        _context_event(player_id, version=2, effective_at=played_at + timedelta(hours=1)),
    ]

    selected = select_context_as_of(events, user_id=player_id, played_at=played_at)

    assert selected is not None
    assert selected.payload["context_version"] == 3


def test_context_before_hand_produces_matched_rows_at_watermark():
    hand = _hand_event()
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    core = TemporalContextJoinCore(allowed_lateness_ms=5_000)
    for player in hand.payload["players"]:
        core.process_context(
            _context_event(
                player["player_id"],
                version=1,
                effective_at=played_at - timedelta(minutes=1),
            )
        )
    core.process_hand(hand)

    output = core.advance_watermark(_epoch_ms(played_at) + 5_000)

    assert len(output) == hand.payload["num_players"]
    assert {event.payload.context_status for event in output} == {"matched"}
    assert {event.payload.context_version for event in output} == {1}


def test_context_after_hand_but_before_deadline_is_marked_late():
    hand = _hand_event()
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    player_id = hand.payload["players"][0]["player_id"]
    core = TemporalContextJoinCore(allowed_lateness_ms=30_000)
    core.process_hand(hand)
    core.process_context(
        _context_event(
            player_id,
            version=1,
            effective_at=played_at - timedelta(minutes=1),
        )
    )

    output = core.advance_watermark(_epoch_ms(played_at) + 30_000)
    row = next(event for event in output if event.payload.player.player_id == player_id)

    assert row.payload.context_status == "matched_late"
    assert row.payload.context_effective_at <= row.payload.played_at


def test_missing_row_is_corrected_with_higher_revision_inside_window():
    hand = _hand_event()
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    player_id = hand.payload["players"][0]["player_id"]
    core = TemporalContextJoinCore(
        allowed_lateness_ms=1_000,
        correction_window_ms=10_000,
    )
    core.process_hand(hand)
    initial = core.advance_watermark(_epoch_ms(played_at) + 1_000)
    missing = next(event for event in initial if event.payload.player.player_id == player_id)

    corrections = core.process_context(
        _context_event(
            player_id,
            version=1,
            effective_at=played_at - timedelta(minutes=1),
        )
    )

    assert missing.payload.context_status == "missing"
    assert missing.payload.revision == 1
    assert len(corrections) == 1
    assert corrections[0].payload.context_status == "corrected"
    assert corrections[0].payload.revision == 2
    assert corrections[0].event_id != missing.event_id


def test_context_after_correction_horizon_does_not_rewrite_output():
    hand = _hand_event()
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    player_id = hand.payload["players"][0]["player_id"]
    core = TemporalContextJoinCore(
        allowed_lateness_ms=1_000,
        correction_window_ms=2_000,
    )
    core.process_hand(hand)
    core.advance_watermark(_epoch_ms(played_at) + 1_000)
    core.advance_watermark(_epoch_ms(played_at) + 3_001)

    corrections = core.process_context(
        _context_event(
            player_id,
            version=1,
            effective_at=played_at - timedelta(minutes=1),
        )
    )

    assert corrections == []


def test_duplicate_delivery_is_idempotent_and_replay_ids_are_stable():
    hand = _hand_event()
    played_at = datetime.fromisoformat(hand.payload["played_at"])
    player_id = hand.payload["players"][0]["player_id"]
    context = _context_event(
        player_id,
        version=1,
        effective_at=played_at - timedelta(minutes=1),
    )

    def run_once():
        core = TemporalContextJoinCore(allowed_lateness_ms=1_000)
        core.process_context(context)
        assert core.process_context(context) == []
        core.process_hand(hand)
        core.process_hand(hand)
        return core.advance_watermark(_epoch_ms(played_at) + 1_000)

    first = run_once()
    replay = run_once()

    assert len(first) == hand.payload["num_players"]
    assert [event.event_id for event in first] == [event.event_id for event in replay]


def test_derived_contract_is_exported_separately_from_input_topics():
    bundle = contract_schema_bundle()

    assert PLAYER_HAND_CONTEXT_ENRICHED in bundle["derived_events"]
    assert bundle["derived_topics"][PLAYER_HAND_CONTEXT_ENRICHED] == (
        "poker.hand-player-context.v1"
    )
