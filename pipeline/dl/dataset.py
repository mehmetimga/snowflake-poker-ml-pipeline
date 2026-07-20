"""Build leakage-safe action-sequence datasets for deep-learning training."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

if TYPE_CHECKING:
    from pipeline.warehouse.factory import Warehouse


ACTION_VOCAB = {"fold": 0, "check": 1, "call": 2, "bet": 3, "raise": 4}
STREET_VOCAB = {"preflop": 0, "flop": 1, "turn": 2, "river": 3}
FEATURE_DIM = 4  # action_idx, street_idx, amount_norm, is_self


@dataclass(frozen=True)
class SequenceSplit:
    X: np.ndarray
    y: np.ndarray
    ids: list[tuple[str, str]]


@dataclass(frozen=True)
class SequencePartitions:
    train: SequenceSplit
    validation: SequenceSplit
    test: SequenceSplit
    strategy: str
    amount_scale: float


def fit_amount_scale(actions: pd.DataFrame) -> float:
    """Fit the amount normalization scale from training actions only."""
    if actions.empty or "amount" not in actions:
        return 1.0
    amounts = pd.to_numeric(actions["amount"], errors="coerce").fillna(0.0)
    return max(float(amounts.abs().max()), 1.0)


def build_sequences(
    warehouse: Warehouse,
    max_len: int = 60,
    hand_ids: Iterable[object] | None = None,
    amount_scale: float | None = None,
) -> tuple[np.ndarray, np.ndarray, list[tuple[str, str]]]:
    """Return X (n, max_len, FEATURE_DIM), y (n,), and ids."""
    from pipeline.warehouse.sql import sql_string_list, unique_strings

    ids_filter = unique_strings(hand_ids or [])
    if hand_ids is not None and not ids_filter:
        return np.zeros((0, max_len, FEATURE_DIM), dtype=np.float32), np.zeros((0,), dtype=np.int64), []

    if ids_filter:
        id_list = sql_string_list(ids_filter)
        actions = warehouse.fetch_df(
            f"SELECT * FROM RAW_ACTIONS WHERE hand_id IN ({id_list}) ORDER BY hand_id, sequence_no"
        )
        players = warehouse.fetch_df(
            f"SELECT hand_id, player_id, is_suspicious FROM RAW_PLAYERS WHERE hand_id IN ({id_list})"
        )
    else:
        actions = warehouse.fetch_df("SELECT * FROM RAW_ACTIONS ORDER BY hand_id, sequence_no")
        players = warehouse.fetch_df("SELECT hand_id, player_id, is_suspicious FROM RAW_PLAYERS")
    return build_sequences_from_dataframes(
        actions,
        players,
        max_len=max_len,
        amount_scale=amount_scale,
    )


def build_sequences_from_dataframes(
    actions: pd.DataFrame,
    players: pd.DataFrame,
    max_len: int = 60,
    amount_scale: float | None = None,
) -> tuple[np.ndarray, np.ndarray, list[tuple[str, str]]]:
    """Return sequence tensors from already-loaded RAW_ACTIONS/RAW_PLAYERS frames."""
    if actions.empty or players.empty:
        return np.zeros((0, max_len, FEATURE_DIM), dtype=np.float32), np.zeros((0,), dtype=np.int64), []

    actions = actions.copy()
    players = players.copy()
    actions["action_idx"] = actions["action_type"].map(ACTION_VOCAB).fillna(0).astype(int)
    actions["street_idx"] = actions["street"].map(STREET_VOCAB).fillna(0).astype(int)
    scale = fit_amount_scale(actions) if amount_scale is None else float(amount_scale)
    if not np.isfinite(scale) or scale <= 0:
        raise ValueError("amount_scale must be a finite positive number")
    amount_norm = (pd.to_numeric(actions["amount"], errors="coerce").fillna(0.0) / scale).astype(float)
    actions["amount_norm"] = amount_norm

    rows: list[np.ndarray] = []
    labels: list[int] = []
    ids: list[tuple[str, str]] = []

    grouped = actions.groupby("hand_id")
    players_idx = players.set_index(["hand_id", "player_id"]).to_dict("index")

    for (hand_id, player_id), info in players_idx.items():
        try:
            hand_actions = grouped.get_group(hand_id)
        except KeyError:
            continue
        seq = np.zeros((max_len, FEATURE_DIM), dtype=np.float32)
        for i, a in enumerate(hand_actions.itertuples()):
            if i >= max_len:
                break
            seq[i, 0] = float(a.action_idx)
            seq[i, 1] = float(a.street_idx)
            seq[i, 2] = float(a.amount_norm)
            seq[i, 3] = 1.0 if a.player_id == player_id else 0.0
        rows.append(seq)
        labels.append(int(info["is_suspicious"]))
        ids.append((str(hand_id), str(player_id)))

    X = np.stack(rows) if rows else np.zeros((0, max_len, FEATURE_DIM), dtype=np.float32)
    y = np.array(labels, dtype=np.int64)
    return X, y, ids


def build_frozen_sequence_partitions(
    warehouse: Warehouse,
    max_len: int = 60,
) -> SequencePartitions:
    """Load frozen train/validation/test rows and keep challenge/live data out.

    Amount normalization is fitted exclusively on training actions and reused
    for validation and test. Player populations must be disjoint, matching the
    frozen dataset contract used by the classical models.
    """
    actions = warehouse.fetch_df(
        """
        SELECT a.hand_id, a.sequence_no, a.player_id, a.street, a.action_type,
               a.amount, h.dataset_split
        FROM RAW_ACTIONS a
        INNER JOIN RAW_HANDS h ON h.hand_id = a.hand_id
        WHERE LOWER(h.dataset_split) IN ('train', 'validation', 'test')
        ORDER BY a.hand_id, a.sequence_no
        """
    )
    players = warehouse.fetch_df(
        """
        SELECT p.hand_id, p.player_id, p.is_suspicious, h.dataset_split
        FROM RAW_PLAYERS p
        INNER JOIN RAW_HANDS h ON h.hand_id = p.hand_id
        WHERE LOWER(h.dataset_split) IN ('train', 'validation', 'test')
        """
    )
    if actions.empty or players.empty:
        raise RuntimeError("Frozen DL data is empty; load train/validation/test hands first.")

    actions = actions.copy()
    players = players.copy()
    actions.columns = [str(column).lower() for column in actions.columns]
    players.columns = [str(column).lower() for column in players.columns]
    actions["dataset_split"] = actions["dataset_split"].astype(str).str.lower()
    players["dataset_split"] = players["dataset_split"].astype(str).str.lower()

    required = {"train", "validation", "test"}
    available = set(players["dataset_split"].dropna())
    missing = required - available
    if missing:
        raise RuntimeError(f"Frozen DL dataset is missing splits: {sorted(missing)}")

    populations = {
        split: set(players.loc[players["dataset_split"] == split, "player_id"].astype(str))
        for split in required
    }
    for left, right in (("train", "validation"), ("train", "test"), ("validation", "test")):
        overlap = populations[left] & populations[right]
        if overlap:
            raise RuntimeError(
                f"Player leakage between frozen {left}/{right} DL splits: {len(overlap)} IDs"
            )

    train_actions = actions[actions["dataset_split"] == "train"]
    amount_scale = fit_amount_scale(train_actions)
    split_data: dict[str, SequenceSplit] = {}
    for split in ("train", "validation", "test"):
        X, y, ids = build_sequences_from_dataframes(
            actions[actions["dataset_split"] == split],
            players[players["dataset_split"] == split],
            max_len=max_len,
            amount_scale=amount_scale,
        )
        split_data[split] = SequenceSplit(X=X, y=y, ids=ids)

    return SequencePartitions(
        train=split_data["train"],
        validation=split_data["validation"],
        test=split_data["test"],
        strategy="frozen_disjoint_players",
        amount_scale=amount_scale,
    )


def _split_arrays(split: SequenceSplit, prefix: str) -> dict[str, np.ndarray]:
    return {
        f"{prefix}_X": split.X.astype(np.float32, copy=False),
        f"{prefix}_y": split.y.astype(np.int64, copy=False),
        f"{prefix}_hand_ids": np.asarray([hand_id for hand_id, _ in split.ids], dtype=np.str_),
        f"{prefix}_player_ids": np.asarray([player_id for _, player_id in split.ids], dtype=np.str_),
    }


def save_sequence_partitions(partitions: SequencePartitions, path: Path) -> dict:
    """Write a secret-free NPZ bundle that can be copied to a GPU worker."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    arrays: dict[str, np.ndarray] = {
        "strategy": np.asarray(partitions.strategy),
        "amount_scale": np.asarray(partitions.amount_scale, dtype=np.float64),
        "feature_dim": np.asarray(FEATURE_DIM, dtype=np.int64),
    }
    for name in ("train", "validation", "test"):
        arrays.update(_split_arrays(getattr(partitions, name), name))
    np.savez_compressed(path, **arrays)

    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    manifest = {
        "schema_version": 1,
        "artifact": path.name,
        "sha256": digest,
        "split_strategy": partitions.strategy,
        "amount_scale": partitions.amount_scale,
        "feature_dim": FEATURE_DIM,
        "max_len": int(partitions.train.X.shape[1]),
        "splits": {
            name: {
                "rows": int(len(getattr(partitions, name).y)),
                "positive_rows": int(getattr(partitions, name).y.sum()),
            }
            for name in ("train", "validation", "test")
        },
    }
    path.with_suffix(".manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def load_sequence_partitions(path: Path) -> SequencePartitions:
    """Load a sequence bundle without enabling pickle deserialization."""
    path = Path(path)
    with np.load(path, allow_pickle=False) as bundle:
        feature_dim = int(bundle["feature_dim"])
        if feature_dim != FEATURE_DIM:
            raise ValueError(f"Expected feature_dim={FEATURE_DIM}, found {feature_dim}")

        split_data: dict[str, SequenceSplit] = {}
        for name in ("train", "validation", "test"):
            hand_ids = bundle[f"{name}_hand_ids"].astype(str).tolist()
            player_ids = bundle[f"{name}_player_ids"].astype(str).tolist()
            split_data[name] = SequenceSplit(
                X=bundle[f"{name}_X"].astype(np.float32),
                y=bundle[f"{name}_y"].astype(np.int64),
                ids=list(zip(hand_ids, player_ids)),
            )
        return SequencePartitions(
            train=split_data["train"],
            validation=split_data["validation"],
            test=split_data["test"],
            strategy=str(bundle["strategy"].item()),
            amount_scale=float(bundle["amount_scale"]),
        )


class HandSequenceDataset(Dataset):
    def __init__(self, X: np.ndarray, y: np.ndarray) -> None:
        self.X = torch.from_numpy(X)
        self.y = torch.from_numpy(y)

    def __len__(self) -> int:
        return len(self.X)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.X[idx], self.y[idx]
