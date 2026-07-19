from __future__ import annotations

import json
from pathlib import Path

from pipeline.generator import FrozenDatasetConfig, build_frozen_dataset, iter_labeled_hands
from pipeline.generator.dataset import iter_jsonl


def _config() -> FrozenDatasetConfig:
    return FrozenDatasetConfig(
        train_hands=4,
        validation_hands=3,
        test_hands=3,
        challenge_hands=2,
        n_players=20,
        n_tables=2,
        n_colluding_pairs=5,
        seed=17,
    )


def test_frozen_dataset_separates_labels_and_is_reproducible(tmp_path: Path):
    first_dir = tmp_path / "first"
    second_dir = tmp_path / "second"
    first = build_frozen_dataset(first_dir, _config())
    second = build_frozen_dataset(second_dir, _config())

    for split in ("train", "validation", "test", "challenge"):
        assert first["splits"][split]["events_sha256"] == second["splits"][split]["events_sha256"]
        assert first["splits"][split]["labels_sha256"] == second["splits"][split]["labels_sha256"]

    event = next(iter_jsonl(first_dir / "train.events.jsonl"))
    assert all("is_suspicious" not in player for player in event["players"])
    assert all("collusion_pair_id" not in player for player in event["players"])

    labeled = list(
        iter_labeled_hands(
            first_dir / "train.events.jsonl",
            first_dir / "train.labels.jsonl",
        )
    )
    assert len(labeled) == 4
    assert all("is_suspicious" in player for hand in labeled for player in hand["players"])
    assert json.loads((first_dir / "manifest.json").read_text())["player_populations_disjoint"]


def test_frozen_dataset_uses_disjoint_player_ids(tmp_path: Path):
    build_frozen_dataset(tmp_path, _config())
    populations: dict[str, set[str]] = {}
    for split in ("train", "validation", "test", "challenge"):
        populations[split] = {
            player["player_id"]
            for hand in iter_jsonl(tmp_path / f"{split}.events.jsonl")
            for player in hand["players"]
        }
    names = list(populations)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            assert populations[left].isdisjoint(populations[right])
