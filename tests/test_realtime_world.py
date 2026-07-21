from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.events import validate_event
from pipeline.generator import (
    FrozenDatasetConfig,
    RealtimeWorldConfig,
    build_realtime_world_dataset,
)
from pipeline.generator.world import SyntheticPokerWorld
from pipeline.generator.dataset import iter_jsonl


def _config() -> RealtimeWorldConfig:
    return RealtimeWorldConfig(
        dataset_id="world-test-v1",
        frozen=FrozenDatasetConfig(
            train_hands=3,
            validation_hands=2,
            test_hands=2,
            challenge_hands=1,
            n_players=12,
            n_tables=2,
            n_colluding_pairs=3,
            seed=413,
        ),
    )


def _assert_no_private_truth(value):
    if isinstance(value, dict):
        forbidden = {"is_suspicious", "is_collusive", "collusion_pair_id"}
        assert forbidden.isdisjoint(value)
        for child in value.values():
            _assert_no_private_truth(child)
    elif isinstance(value, list):
        for child in value:
            _assert_no_private_truth(child)


def test_world_dataset_is_reproducible_and_has_expected_pair_labels(tmp_path: Path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = build_realtime_world_dataset(first_dir, _config())
    second = build_realtime_world_dataset(second_dir, _config())

    assert first == second
    assert first["artifacts"] == second["artifacts"]
    for split, expected_hands in (("train", 3), ("validation", 2), ("test", 2), ("challenge", 1)):
        split_manifest = first["splits"][split]
        assert split_manifest["hands"] == expected_hands
        assert split_manifest["pair_label_rows"] == expected_hands * 15
        assert split_manifest["player_label_rows"] == expected_hands * 6

    assert (first_dir / "challenge" / "private_labels" / "pair_labels.jsonl").exists()
    assert not (first_dir / "challenge" / "labels").exists()
    assert json.loads((first_dir / "manifest.json").read_text()) == first
    schemas = json.loads((first_dir / "schemas.json").read_text())
    assert schemas["schema_version"] == 1
    assert "poker.hand.completed" in schemas["payloads"]
    assert "pair_hand" in schemas["private_labels"]
    first_hand = next(iter_jsonl(first_dir / "train" / "events" / "hands.jsonl"))
    assert first_hand["payload"]["hand_id"].startswith("WORLD-TEST-V1-TRAIN-H-")


def test_world_public_events_validate_and_do_not_contain_labels(tmp_path: Path):
    build_realtime_world_dataset(tmp_path, _config())
    event_files = (
        "hands.jsonl",
        "user_context.jsonl",
        "sessions.jsonl",
        "account_links.jsonl",
    )
    for split in ("train", "validation", "test", "challenge"):
        for filename in event_files:
            for event in iter_jsonl(tmp_path / split / "events" / filename):
                validate_event(event)
                _assert_no_private_truth(event)


def test_context_exists_before_hands_and_populations_are_disjoint(tmp_path: Path):
    manifest = build_realtime_world_dataset(tmp_path, _config())
    populations: dict[str, set[str]] = {}
    for split in ("train", "validation", "test", "challenge"):
        contexts = list(iter_jsonl(tmp_path / split / "events" / "user_context.jsonl"))
        hands = list(iter_jsonl(tmp_path / split / "events" / "hands.jsonl"))
        populations[split] = {event["payload"]["user_id"] for event in contexts}
        assert len(contexts) == manifest["splits"][split]["population_players"]
        if hands:
            assert max(event["occurred_at"] for event in contexts) < min(
                event["occurred_at"] for event in hands
            )

    names = list(populations)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            assert populations[left].isdisjoint(populations[right])


def test_world_dataset_accepts_a_live_event_time_anchor(tmp_path: Path):
    hand_start = datetime(2026, 7, 22, 9, 0, tzinfo=timezone.utc)
    context_start = hand_start - timedelta(days=1)

    build_realtime_world_dataset(
        tmp_path,
        _config(),
        hand_start_at=hand_start,
        context_start_at=context_start,
    )

    first_hand = next(iter_jsonl(tmp_path / "train" / "events" / "hands.jsonl"))
    contexts = list(iter_jsonl(tmp_path / "train" / "events" / "user_context.jsonl"))
    config = json.loads((tmp_path / "config.json").read_text())
    assert first_hand["occurred_at"] == "2026-07-22T09:00:00Z"
    assert max(event["occurred_at"] for event in contexts) < first_hand["occurred_at"]
    assert config["hand_start_at"] == "2026-07-22T09:00:00+00:00"
    assert config["context_start_at"] == "2026-07-21T09:00:00+00:00"


def test_world_dataset_rejects_a_naive_live_event_time_anchor(tmp_path: Path):
    with pytest.raises(ValueError, match="hand_start_at must include timezone"):
        build_realtime_world_dataset(
            tmp_path,
            _config(),
            hand_start_at=datetime(2026, 7, 22, 9, 0),
        )


def test_colluder_context_is_correlated_but_not_a_deterministic_label():
    world = SyntheticPokerWorld(
        dataset_id="context-signal-test-v2",
        split="train",
        hand_count=0,
        n_players=200,
        n_tables=20,
        n_colluding_pairs=30,
        seed=42,
    )
    contexts = {user.context.user_id: user.context for user in world.users}
    colluding_keys = {
        frozenset((pair.player_a, pair.player_b)) for pair in world.hand_generator.pairs
    }
    colluding_same_network = [
        contexts[pair.player_a].network_cluster_id
        == contexts[pair.player_b].network_cluster_id
        for pair in world.hand_generator.pairs
    ]
    normal_same_network = []
    player_ids = sorted(contexts)
    for index, left in enumerate(player_ids):
        for right in player_ids[index + 1 :]:
            if frozenset((left, right)) not in colluding_keys:
                normal_same_network.append(
                    contexts[left].network_cluster_id
                    == contexts[right].network_cluster_id
                )

    colluding_rate = sum(colluding_same_network) / len(colluding_same_network)
    normal_rate = sum(normal_same_network) / len(normal_same_network)
    assert 0 < colluding_rate < 1
    assert 0 < normal_rate < 1
    assert colluding_rate > normal_rate + 0.25
    for event in world.context_events():
        _assert_no_private_truth(event)
