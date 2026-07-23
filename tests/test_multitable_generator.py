from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.events import HandCompletedPayload, validate_event
from pipeline.generator import (
    GeneratorConfig,
    HandGenerator,
    MultiTablePokerWorld,
    MultiTableProfile,
    build_multitable_dataset,
)
from pipeline.generator.dataset import iter_jsonl, separate_hand_labels


def _small_profile() -> MultiTableProfile:
    return MultiTableProfile.from_dict(
        {
            "schema_version": 1,
            "profile_id": "multitable-test-v1",
            "dataset_id": "multitable-test-v1",
            "split_hands": {
                "train": 24,
                "validation": 12,
                "test": 12,
                "challenge": 12,
            },
            "registered_players": 120,
            "daily_active_players": 40,
            "peak_concurrent_players": 20,
            "table_size_counts": {"4": 2, "5": 2, "6": 2},
            "hands_per_table_hour": 60,
            "simulated_day_hours": 1,
            "max_simultaneous_tables": 2,
            "simultaneous_table_distribution": {"1": 0.5, "2": 0.5},
            "seat_rebalance_minutes": 30,
            "minimum_session_minutes": 20,
            "maximum_session_minutes": 60,
            "n_colluding_pairs": 10,
            "seed": 991,
        }
    )


def test_checked_in_smoke_profile_has_planned_capacity():
    profile = MultiTableProfile.from_json(
        Path("config/generator/multitable-smoke-v1.json")
    )
    schema = json.loads(
        Path("schemas/generator/poker.multitable-profile.v1.schema.json").read_text()
    )

    assert schema["$id"] == "poker.multitable-profile.v1"
    assert profile.table_count == 100
    assert profile.concurrent_seats == 530
    assert profile.registered_players == 10_000
    assert profile.daily_active_players == 2_000
    assert profile.peak_concurrent_players == 400
    assert profile.expected_tables_per_active_player == pytest.approx(1.345)
    assert profile.cohort_minutes == pytest.approx(96.0)
    assert sum(profile.split_hands.values()) == 6_000


def test_profile_rejects_capacity_that_cannot_fill_tables():
    raw = _small_profile().to_dict()
    raw["simultaneous_table_distribution"] = {"1": 1.0, "2": 0.0}

    with pytest.raises(ValueError, match="cannot fill"):
        MultiTableProfile.from_dict(raw)


@pytest.mark.parametrize(
    ("player_count", "positions"),
    (
        (4, {"SB", "BB", "CO", "BTN"}),
        (5, {"SB", "BB", "UTG", "CO", "BTN"}),
        (6, {"SB", "BB", "UTG", "MP", "CO", "BTN"}),
    ),
)
def test_pokerkit_accepts_explicit_4_to_6_player_schedule(
    player_count: int,
    positions: set[str],
):
    generator = HandGenerator(
        GeneratorConfig(
            n_hands=0,
            n_players=12,
            n_tables=2,
            n_colluding_pairs=3,
            seed=771,
            dataset_split="train",
            dataset_id="scheduled-test-v1",
        )
    )
    played_at = datetime(2026, 8, 1, 12, 30, tzinfo=timezone.utc)
    player_ids = [player.player_id for player in generator.players[:player_count]]

    raw_hand = generator.generate_hand(
        player_count,
        table_id=generator.tables[1],
        seat_player_ids=player_ids,
        played_at=played_at,
    )
    safe_hand, _ = separate_hand_labels(raw_hand)
    payload = HandCompletedPayload.model_validate(safe_hand)

    assert payload.table_id == generator.tables[1]
    assert payload.played_at == played_at
    assert payload.num_players == player_count
    assert {player.position for player in payload.players} == positions
    assert {str(player.player_id) for player in payload.players} == set(player_ids)
    assert sum(player.won_amount for player in payload.players) == pytest.approx(
        payload.pot_size
    )


def test_multitable_dataset_is_deterministic_and_capacity_safe(tmp_path: Path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = build_multitable_dataset(first_dir, _small_profile())
    second = build_multitable_dataset(second_dir, _small_profile())

    assert first == second
    assert first["artifacts"] == second["artifacts"]
    assert first["requested"]["tables"] == 6
    assert first["requested"]["concurrent_seats"] == 30
    assert first["player_populations_disjoint"]

    for split, expected_hands in _small_profile().split_hands.items():
        split_manifest = first["splits"][split]
        assert split_manifest["hands"] == expected_hands
        assert split_manifest["registered_players"] == 120
        assert split_manifest["sessions"] == 40
        assert split_manifest["seat_assignments"] == 30
        assert split_manifest["player_label_rows"] == sum(
            int(size) * int(count)
            for size, count in split_manifest["table_size_hand_histogram"].items()
        )
        assert split_manifest["pair_label_rows"] == sum(
            count * (int(size) * (int(size) - 1) // 2)
            for size, count in split_manifest["table_size_hand_histogram"].items()
        )

        seats = list(
            iter_jsonl(first_dir / split / "schedule" / "seat_assignments.jsonl")
        )
        by_table: dict[str, list[dict]] = defaultdict(list)
        by_player: Counter[str] = Counter()
        for seat in seats:
            by_table[seat["table_id"]].append(seat)
            by_player[seat["player_id"]] += 1
        assert sorted(len(rows) for rows in by_table.values()) == [
            4,
            4,
            5,
            5,
            6,
            6,
        ]
        assert all(
            len({row["player_id"] for row in rows}) == len(rows)
            for rows in by_table.values()
        )
        assert max(by_player.values()) == 2
        assert min(by_player.values()) == 1
        assert sum(by_player.values()) == 30

        events = list(iter_jsonl(first_dir / split / "events" / "hands.jsonl"))
        assert len(events) == expected_hands
        for event in events:
            validate_event(event)
            assert event["payload"]["num_players"] in {4, 5, 6}
            assert all(
                "is_suspicious" not in player and "collusion_pair_id" not in player
                for player in event["payload"]["players"]
            )

    assert (first_dir / "challenge" / "private_labels" / "pair_labels.jsonl").exists()
    assert not (first_dir / "challenge" / "labels").exists()


def test_table_clocks_are_ordered_and_interleave(tmp_path: Path):
    profile = _small_profile()
    build_multitable_dataset(tmp_path, profile)
    rows = list(iter_jsonl(tmp_path / "train" / "schedule" / "hands.jsonl"))
    by_table: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        by_table[row["table_id"]].append(row)

    assert len(by_table) == profile.table_count
    for table_rows in by_table.values():
        times = [
            datetime.fromisoformat(row["played_at"].replace("Z", "+00:00"))
            for row in table_rows
        ]
        sequences = [row["table_hand_sequence_no"] for row in table_rows]
        assert times == sorted(times)
        assert sequences == list(range(len(sequences)))

    globally_ordered_tables = [row["table_id"] for row in rows]
    assert len(set(globally_ordered_tables[: profile.table_count])) > 1
    assert any(
        datetime.fromisoformat(left["played_at"].replace("Z", "+00:00"))
        + timedelta(seconds=90)
        > datetime.fromisoformat(right["played_at"].replace("Z", "+00:00"))
        for left, right in zip(rows, rows[1:])
        if left["table_id"] != right["table_id"]
    )


def test_scheduler_rotates_sessions_and_seats_across_active_windows():
    profile = _small_profile()
    start_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    world = MultiTablePokerWorld(
        profile,
        split="train",
        hand_count=0,
        seed=profile.seed,
        start_at=start_at,
    )

    scheduled = list(world.scheduler.iter_hands(400))
    assignments = world.scheduler.seat_assignment_records
    sessions = world.scheduler.session_records

    assert scheduled[-1].played_at >= start_at + timedelta(days=1)
    assert len(sessions) == profile.daily_active_players * 2
    assert len(assignments) >= profile.concurrent_seats * 3

    interval_players: dict[tuple[str, str], set[str]] = defaultdict(set)
    for assignment in assignments:
        interval_players[
            (
                assignment.effective_from.isoformat(),
                assignment.effective_to.isoformat(),
            )
        ].add(assignment.player_id)
    ordered_intervals = sorted(interval_players)
    assert len(ordered_intervals) >= 3
    assert len(interval_players[ordered_intervals[0]]) == 20
    assert len(interval_players[ordered_intervals[1]]) == 20
    assert interval_players[ordered_intervals[0]].isdisjoint(
        interval_players[ordered_intervals[1]]
    )
