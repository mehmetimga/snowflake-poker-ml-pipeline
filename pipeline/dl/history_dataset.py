"""Point-in-time multi-hand histories for Phase 10 pair-risk models."""

from __future__ import annotations

import hashlib
import io
import json
import math
import shutil
import zipfile
from collections import defaultdict, deque
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


HISTORY_SCHEMA_VERSION = 1
HISTORY_SPLITS = ("train", "validation", "test")
POSITION_INDEX = {"SB": 0, "BB": 1, "UTG": 2, "MP": 3, "CO": 4, "BTN": 5}

USER_HISTORY_FEATURES = (
    "log_invested_big_blinds",
    "log_won_big_blinds",
    "won_pot_ratio",
    "action_count",
    "fold_rate",
    "check_rate",
    "call_rate",
    "bet_rate",
    "raise_rate",
    "aggressive_rate",
    "saw_flop",
    "saw_turn",
    "saw_river",
    "position_fraction",
    "log_starting_stack_big_blinds",
    "log_pot_big_blinds",
    "log_minutes_since_previous_hand",
)

PAIR_CURRENT_COLUMNS = (
    "current_invested_pot_ratio_a",
    "current_invested_pot_ratio_b",
    "current_invested_abs_diff_ratio",
    "current_won_amount_a",
    "current_won_amount_b",
    "current_outcome_abs_diff_ratio",
    "current_aggressive_actions_a",
    "current_aggressive_actions_b",
    "current_fold_actions_a",
    "current_fold_actions_b",
    "current_both_saw_flop",
    "current_both_saw_river",
    "current_one_folded_other_won",
)
PAIR_HISTORY_FEATURES = PAIR_CURRENT_COLUMNS + (
    "log_minutes_since_previous_hand_together",
)


@dataclass(frozen=True)
class HistoryDatasetConfig:
    source_dir: Path = Path("data/datasets/context-full-v2")
    pair_dataset_dir: Path = Path("data/datasets/pair-full-v2")
    output_dir: Path = Path("data/datasets/pair-sequences-full-v2")
    max_history: int = 16
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.max_history < 2:
            raise ValueError("max_history must be at least two hands")
        resolved = {
            self.source_dir.resolve(),
            self.pair_dataset_dir.resolve(),
            self.output_dir.resolve(),
        }
        if len(resolved) != 3:
            raise ValueError("source, pair, and history directories must differ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def event_alignment_sha256(values: Iterable[object]) -> str:
    payload = "\n".join(str(value) for value in values).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def write_deterministic_npz(path: Path, arrays: Mapping[str, np.ndarray]) -> None:
    """Write an NPZ whose bytes do not depend on wall-clock ZIP metadata."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(path, "w", allowZip64=True) as archive:
        for name in sorted(arrays):
            info = zipfile.ZipInfo(f"{name}.npy", date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            with archive.open(info, "w", force_zip64=True) as output:
                np.lib.format.write_array(output, np.asanyarray(arrays[name]), allow_pickle=False)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as stream:
        for line in stream:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _timestamp_ns(value: object) -> int:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.value)


def _pair_players(pair_key: object) -> tuple[str, str]:
    values = str(pair_key).split(":")
    if len(values) != 2 or not values[0] or not values[1] or values[0] >= values[1]:
        raise ValueError(f"invalid canonical pair key: {pair_key!r}")
    return values[0], values[1]


def _minutes_gap(current_ns: int, previous_ns: int | None) -> float:
    if previous_ns is None:
        return 0.0
    if previous_ns >= current_ns:
        raise ValueError("history timestamp must be strictly earlier than the example")
    return math.log1p((current_ns - previous_ns) / 60_000_000_000)


def _user_step(
    payload: Mapping[str, Any], player: Mapping[str, Any], previous_ns: int | None
) -> np.ndarray:
    player_id = str(player["player_id"])
    actions = [
        action for action in payload["actions"] if str(action["player_id"]) == player_id
    ]
    count = max(len(actions), 1)
    action_counts = {
        name: sum(str(action["action_type"]).lower() == name for action in actions)
        for name in ("fold", "check", "call", "bet", "raise")
    }
    streets = {str(action["street"]).lower() for action in actions}
    big_blind = max(float(payload.get("big_blind", 0.0) or 0.0), 1e-6)
    pot = max(float(payload.get("pot_size", 0.0) or 0.0), 0.0)
    invested = sum(max(float(action.get("amount", 0.0) or 0.0), 0.0) for action in actions)
    won = max(float(player.get("won_amount", 0.0) or 0.0), 0.0)
    position = POSITION_INDEX.get(str(player.get("position", "")).upper(), -1)
    current_ns = _timestamp_ns(payload["played_at"])
    values = np.asarray(
        [
            math.log1p(invested / big_blind),
            math.log1p(won / big_blind),
            won / max(pot, 1e-6),
            float(len(actions)),
            action_counts["fold"] / count,
            action_counts["check"] / count,
            action_counts["call"] / count,
            action_counts["bet"] / count,
            action_counts["raise"] / count,
            (action_counts["bet"] + action_counts["raise"]) / count,
            float("flop" in streets),
            float("turn" in streets),
            float("river" in streets),
            max(position, 0) / 5.0,
            math.log1p(max(float(player.get("stack_start", 0.0) or 0.0), 0.0) / big_blind),
            math.log1p(pot / big_blind),
            _minutes_gap(current_ns, previous_ns),
        ],
        dtype=np.float32,
    )
    if not np.isfinite(values).all():
        raise ValueError(f"non-finite user history step for player {player_id}")
    return values


def _pair_step(row: pd.Series, current_ns: int, previous_ns: int | None) -> np.ndarray:
    values = [float(pd.to_numeric(row[column], errors="coerce")) for column in PAIR_CURRENT_COLUMNS]
    values.append(_minutes_gap(current_ns, previous_ns))
    output = np.asarray(values, dtype=np.float32)
    if not np.isfinite(output).all():
        raise ValueError(f"non-finite pair history step for {row['pair_key']}")
    return output


def _padded_history(
    history: Sequence[tuple[int, np.ndarray]], max_history: int, feature_dim: int
) -> tuple[np.ndarray, np.ndarray, int]:
    sequence = np.zeros((max_history, feature_dim), dtype=np.float16)
    mask = np.zeros(max_history, dtype=np.uint8)
    selected = history[-max_history:]
    if selected:
        start = max_history - len(selected)
        sequence[start:] = np.asarray([value for _, value in selected], dtype=np.float16)
        mask[start:] = 1
        last_seen = int(selected[-1][0])
    else:
        last_seen = -1
    return sequence, mask, last_seen


def build_split_history_arrays(
    pair_frame: pd.DataFrame,
    hand_events: Sequence[Mapping[str, Any]],
    *,
    max_history: int,
) -> tuple[dict[str, np.ndarray], dict[str, Any]]:
    """Build histories using only events strictly earlier than each example."""
    required = {
        "event_id",
        "hand_id",
        "played_at",
        "pair_key",
        "target",
        *PAIR_CURRENT_COLUMNS,
    }
    missing = sorted(required - set(pair_frame.columns))
    if missing:
        raise ValueError(f"pair frame is missing sequence columns: {missing}")
    frame = pair_frame.reset_index(drop=True).copy()
    if frame["event_id"].astype(str).duplicated().any():
        raise ValueError("sequence examples contain duplicate event IDs")
    frame["_played_ns"] = frame["played_at"].map(_timestamp_ns)
    hand_rows = {
        str(hand_id): np.asarray(indices, dtype=np.int64)
        for hand_id, indices in frame.groupby("hand_id", sort=False).indices.items()
    }

    records: list[tuple[int, str, Mapping[str, Any]]] = []
    seen_hands: set[str] = set()
    for event in hand_events:
        payload = event.get("payload", event)
        hand_id = str(payload["hand_id"])
        if hand_id in seen_hands:
            raise ValueError(f"duplicate source hand: {hand_id}")
        seen_hands.add(hand_id)
        records.append((_timestamp_ns(payload["played_at"]), hand_id, payload))
    records.sort(key=lambda value: (value[0], value[1]))
    if seen_hands != set(hand_rows):
        raise ValueError(
            f"source/pair hand mismatch: source={len(seen_hands)} pair={len(hand_rows)}"
        )

    row_count = len(frame)
    pair_sequences = np.zeros(
        (row_count, max_history, len(PAIR_HISTORY_FEATURES)), dtype=np.float16
    )
    pair_masks = np.zeros((row_count, max_history), dtype=np.uint8)
    pair_last_seen_ns = np.full(row_count, -1, dtype=np.int64)
    user_a_indices = np.empty(row_count, dtype=np.int32)
    user_b_indices = np.empty(row_count, dtype=np.int32)
    example_played_ns = frame["_played_ns"].to_numpy(dtype=np.int64)

    user_sequences: list[np.ndarray] = []
    user_masks: list[np.ndarray] = []
    user_last_seen_ns: list[int] = []
    user_history: dict[str, deque[tuple[int, np.ndarray]]] = defaultdict(
        lambda: deque(maxlen=max_history)
    )
    pair_history: dict[str, deque[tuple[int, np.ndarray]]] = defaultdict(
        lambda: deque(maxlen=max_history)
    )

    for played_ns, timestamp_group in groupby(records, key=lambda value: value[0]):
        group_records = list(timestamp_group)
        pending_user: list[tuple[str, int, np.ndarray]] = []
        pending_pair: list[tuple[str, int, np.ndarray]] = []
        for _, hand_id, payload in group_records:
            indices = hand_rows[hand_id]
            rows = frame.iloc[indices]
            if len(indices) != 15:
                raise ValueError(f"expected 15 pair examples for {hand_id}, found {len(indices)}")
            if set(rows["_played_ns"].astype(int)) != {played_ns}:
                raise ValueError(f"source/pair timestamps disagree for {hand_id}")
            players = {
                str(player["player_id"]): player for player in payload.get("players", [])
            }
            if len(players) != 6:
                raise ValueError(f"expected six players in {hand_id}, found {len(players)}")
            snapshot_indices: dict[str, int] = {}
            for player_id in sorted(players):
                sequence, mask, last_seen = _padded_history(
                    list(user_history[player_id]), max_history, len(USER_HISTORY_FEATURES)
                )
                if last_seen >= played_ns:
                    raise ValueError("user history contains a current or future hand")
                snapshot_indices[player_id] = len(user_sequences)
                user_sequences.append(sequence)
                user_masks.append(mask)
                user_last_seen_ns.append(last_seen)
                previous = user_history[player_id][-1][0] if user_history[player_id] else None
                pending_user.append(
                    (player_id, played_ns, _user_step(payload, players[player_id], previous))
                )
            observed_pairs: set[str] = set()
            for row_index in indices:
                row = frame.iloc[int(row_index)]
                pair_key = str(row["pair_key"])
                player_a, player_b = _pair_players(pair_key)
                if player_a not in players or player_b not in players:
                    raise ValueError(f"pair {pair_key} is not present in hand {hand_id}")
                if pair_key in observed_pairs:
                    raise ValueError(f"duplicate pair {pair_key} in hand {hand_id}")
                observed_pairs.add(pair_key)
                user_a_indices[row_index] = snapshot_indices[player_a]
                user_b_indices[row_index] = snapshot_indices[player_b]
                sequence, mask, last_seen = _padded_history(
                    list(pair_history[pair_key]), max_history, len(PAIR_HISTORY_FEATURES)
                )
                if last_seen >= played_ns:
                    raise ValueError("pair history contains a current or future hand")
                pair_sequences[row_index] = sequence
                pair_masks[row_index] = mask
                pair_last_seen_ns[row_index] = last_seen
                previous = pair_history[pair_key][-1][0] if pair_history[pair_key] else None
                pending_pair.append(
                    (pair_key, played_ns, _pair_step(row, played_ns, previous))
                )
        # Equal-timestamp hands become visible together only after all snapshots
        # at that timestamp have been captured.
        for player_id, timestamp, step in pending_user:
            user_history[player_id].append((timestamp, step))
        for pair_key, timestamp, step in pending_pair:
            pair_history[pair_key].append((timestamp, step))

    user_sequence_array = np.stack(user_sequences).astype(np.float16, copy=False)
    user_mask_array = np.stack(user_masks).astype(np.uint8, copy=False)
    user_last_array = np.asarray(user_last_seen_ns, dtype=np.int64)
    if np.any(pair_last_seen_ns >= example_played_ns):
        raise ValueError("pair history timestamp audit failed")
    if np.any(user_last_array[user_a_indices] >= example_played_ns) or np.any(
        user_last_array[user_b_indices] >= example_played_ns
    ):
        raise ValueError("user history timestamp audit failed")

    arrays = {
        "event_ids": frame["event_id"].astype(str).to_numpy(dtype=np.str_),
        "example_played_ns": example_played_ns,
        "labels": frame["target"].astype(np.int8).to_numpy(),
        "pair_last_seen_ns": pair_last_seen_ns,
        "pair_masks": pair_masks,
        "pair_sequences": pair_sequences,
        "user_a_indices": user_a_indices,
        "user_b_indices": user_b_indices,
        "user_last_seen_ns": user_last_array,
        "user_masks": user_mask_array,
        "user_sequences": user_sequence_array,
    }
    audit = {
        "rows": row_count,
        "hands": len(records),
        "positive_rows": int(arrays["labels"].sum()),
        "user_snapshots": len(user_sequence_array),
        "user_history_steps": int(user_mask_array.sum()),
        "pair_history_steps": int(pair_masks.sum()),
        "empty_user_snapshots": int((user_mask_array.sum(axis=1) == 0).sum()),
        "empty_pair_examples": int((pair_masks.sum(axis=1) == 0).sum()),
        "event_alignment_sha256": event_alignment_sha256(arrays["event_ids"]),
        "strictly_prior_timestamp_check": True,
        "equal_timestamp_isolation": True,
    }
    return arrays, audit


def build_history_dataset(config: HistoryDatasetConfig) -> dict[str, Any]:
    source_dir = config.source_dir.resolve()
    pair_dir = config.pair_dataset_dir.resolve()
    output_dir = config.output_dir.resolve()
    source_manifest_path = source_dir / "manifest.json"
    pair_manifest_path = pair_dir / "manifest.json"
    if not source_manifest_path.is_file() or not pair_manifest_path.is_file():
        raise FileNotFoundError("source and pair dataset manifests are required")
    source_manifest = json.loads(source_manifest_path.read_text())
    pair_manifest = json.loads(pair_manifest_path.read_text())
    if source_manifest["dataset_id"] != pair_manifest["dataset_id"]:
        raise ValueError("world and pair dataset IDs disagree")
    if not source_manifest.get("player_populations_disjoint"):
        raise ValueError("Phase 10 cold-start histories require disjoint split populations")
    if pair_manifest["feature_definition_version"] != "pair-features-v1":
        raise ValueError("Phase 10 requires pair-features-v1")
    if pair_manifest["challenge_labels_public"]:
        raise ValueError("private challenge labels cannot enter Phase 10")
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    (output_dir / "splits").mkdir(parents=True, exist_ok=True)

    schema = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "phase": 10,
        "max_history": config.max_history,
        "user_history_features": list(USER_HISTORY_FEATURES),
        "pair_history_features": list(PAIR_HISTORY_FEATURES),
        "history_semantics": "strictly_before_example_played_at",
        "padding": "left_zero_padded",
        "mask_semantics": "one_is_valid",
        "storage_dtype": "float16",
        "challenge_labels_public": False,
    }
    _write_json(output_dir / "schema.json", schema)
    artifacts: dict[str, str] = {"schema.json": sha256_file(output_dir / "schema.json")}
    split_audits: dict[str, Any] = {}
    populations: dict[str, set[str]] = {}
    for split in HISTORY_SPLITS:
        source_relative = f"{split}/events/hands.jsonl"
        pair_relative = f"dgx/cold_start/{split}.parquet"
        source_path = source_dir / source_relative
        pair_path = pair_dir / pair_relative
        if sha256_file(source_path) != source_manifest["artifacts"][source_relative]:
            raise ValueError(f"world hand artifact hash mismatch: {source_relative}")
        if sha256_file(pair_path) != pair_manifest["artifacts"][pair_relative]:
            raise ValueError(f"pair artifact hash mismatch: {pair_relative}")
        frame = pd.read_parquet(pair_path)
        if set(frame["benchmark_split"].astype(str)) != {split}:
            raise ValueError(f"{pair_relative} contains another benchmark split")
        players = set()
        for pair_key in frame["pair_key"]:
            players.update(_pair_players(pair_key))
        populations[split] = players
        arrays, audit = build_split_history_arrays(
            frame, _read_jsonl(source_path), max_history=config.max_history
        )
        relative = f"splits/{split}.npz"
        write_deterministic_npz(output_dir / relative, arrays)
        artifacts[relative] = sha256_file(output_dir / relative)
        split_audits[split] = audit
        print(
            f"[pair-history-dataset] split={split} rows={audit['rows']} "
            f"user_steps={audit['user_history_steps']} "
            f"pair_steps={audit['pair_history_steps']}",
            flush=True,
        )
    for left, right in (("train", "validation"), ("train", "test"), ("validation", "test")):
        overlap = populations[left] & populations[right]
        if overlap:
            raise ValueError(f"player leakage between {left}/{right}: {len(overlap)}")

    manifest = {
        "schema_version": HISTORY_SCHEMA_VERSION,
        "phase": 10,
        "dataset_id": source_manifest["dataset_id"],
        "benchmark": "cold_start",
        "feature_definition_version": pair_manifest["feature_definition_version"],
        "source_world_manifest_sha256": sha256_file(source_manifest_path),
        "source_pair_manifest_sha256": sha256_file(pair_manifest_path),
        "source_splits": list(HISTORY_SPLITS),
        "challenge_artifacts_read": False,
        "challenge_labels_public": False,
        "player_populations_disjoint": True,
        "max_history": config.max_history,
        "splits": split_audits,
        "artifacts": artifacts,
    }
    _write_json(output_dir / "manifest.json", manifest)
    return manifest


def load_history_split(path: Path) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as bundle:
        return {name: bundle[name] for name in bundle.files}
