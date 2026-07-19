"""Frozen, labeled dataset artifacts for reproducible ML validation."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from importlib.metadata import version
from pathlib import Path
from typing import Iterable, Iterator

from .hand_generator import GeneratorConfig, HandGenerator


SPLIT_NAMES = ("train", "validation", "test", "challenge")
_SEED_OFFSETS = {"train": 0, "validation": 10_000, "test": 20_000, "challenge": 30_000}
_LABEL_FIELDS = ("is_suspicious", "collusion_pair_id")


@dataclass(frozen=True)
class FrozenDatasetConfig:
    train_hands: int = 20_000
    validation_hands: int = 5_000
    test_hands: int = 5_000
    challenge_hands: int = 5_000
    n_players: int = 200
    n_tables: int = 20
    n_colluding_pairs: int = 30
    seed: int = 42

    def counts(self) -> dict[str, int]:
        return {
            "train": self.train_hands,
            "validation": self.validation_hands,
            "test": self.test_hands,
            "challenge": self.challenge_hands,
        }


def _json_line(value: dict) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def separate_hand_labels(hand: dict) -> tuple[dict, list[dict]]:
    """Return an inference-safe event and its player-level label sidecar."""
    event = {key: value for key, value in hand.items() if key != "players"}
    event_players: list[dict] = []
    labels: list[dict] = []
    for player in hand["players"]:
        event_players.append(
            {key: value for key, value in player.items() if key not in _LABEL_FIELDS}
        )
        labels.append(
            {
                "hand_id": hand["hand_id"],
                "player_id": player["player_id"],
                "is_suspicious": bool(player.get("is_suspicious", False)),
                "collusion_pair_id": player.get("collusion_pair_id"),
            }
        )
    event["players"] = event_players
    return event, labels


def iter_jsonl(path: Path) -> Iterator[dict]:
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def iter_labeled_hands(events_path: Path, labels_path: Path) -> Iterator[dict]:
    """Merge an event JSONL file with a player-label JSONL sidecar."""
    labels = {
        (str(row["hand_id"]), str(row["player_id"])): row
        for row in iter_jsonl(labels_path)
    }
    used: set[tuple[str, str]] = set()
    for event in iter_jsonl(events_path):
        players: list[dict] = []
        for player in event["players"]:
            key = (str(event["hand_id"]), str(player["player_id"]))
            if key not in labels:
                raise ValueError(f"Missing label for hand/player {key}")
            label = labels[key]
            used.add(key)
            players.append(
                {
                    **player,
                    "is_suspicious": bool(label["is_suspicious"]),
                    "collusion_pair_id": label.get("collusion_pair_id"),
                }
            )
        yield {**event, "players": players}
    unused = set(labels) - used
    if unused:
        raise ValueError(f"Label sidecar contains {len(unused)} rows without matching events")


def build_frozen_dataset(
    output_dir: Path,
    config: FrozenDatasetConfig | None = None,
) -> dict:
    """Write deterministic event/label JSONL files plus a hash manifest."""
    cfg = config or FrozenDatasetConfig()
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest: dict = {
        "schema_version": 1,
        "generator": "pokerkit",
        "pokerkit_version": version("pokerkit"),
        "base_seed": cfg.seed,
        "player_populations_disjoint": True,
        "splits": {},
    }
    populations: dict[str, set[str]] = {}

    for split, count in cfg.counts().items():
        split_seed = cfg.seed + _SEED_OFFSETS[split]
        generator = HandGenerator(
            GeneratorConfig(
                n_hands=count,
                n_players=cfg.n_players,
                n_tables=cfg.n_tables,
                n_colluding_pairs=cfg.n_colluding_pairs,
                seed=split_seed,
                dataset_split=split,
            )
        )
        populations[split] = {player.player_id for player in generator.players}
        events_path = output_dir / f"{split}.events.jsonl"
        labels_path = output_dir / f"{split}.labels.jsonl"
        label_rows = 0
        positive_rows = 0
        with events_path.open("w") as events_file, labels_path.open("w") as labels_file:
            for hand in generator.iter_hands():
                event, labels = separate_hand_labels(hand)
                events_file.write(_json_line(event))
                for label in labels:
                    labels_file.write(_json_line(label))
                    label_rows += 1
                    positive_rows += int(label["is_suspicious"])

        manifest["splits"][split] = {
            "seed": split_seed,
            "hands": count,
            "players_per_hand": 6,
            "population_players": cfg.n_players,
            "label_rows": label_rows,
            "positive_label_rows": positive_rows,
            "events_file": events_path.name,
            "events_sha256": _sha256(events_path),
            "labels_file": labels_path.name,
            "labels_sha256": _sha256(labels_path),
        }

    for left_index, left in enumerate(SPLIT_NAMES):
        for right in SPLIT_NAMES[left_index + 1 :]:
            overlap = populations[left] & populations[right]
            if overlap:
                raise RuntimeError(f"Player leakage between {left} and {right}: {len(overlap)}")

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def labeled_hands(events: Iterable[dict], label_rows: Iterable[dict]) -> Iterator[dict]:
    """In-memory equivalent of :func:`iter_labeled_hands`, useful in tests."""
    labels = {
        (str(row["hand_id"]), str(row["player_id"])): row
        for row in label_rows
    }
    for event in events:
        players = []
        for player in event["players"]:
            label = labels[(str(event["hand_id"]), str(player["player_id"]))]
            players.append({**player, **{key: label.get(key) for key in _LABEL_FIELDS}})
        yield {**event, "players": players}
