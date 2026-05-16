"""Builds per-(hand_id, player_id) action sequences from RAW_ACTIONS for DL training."""

from __future__ import annotations

import numpy as np
import pandas as pd
import torch
from torch.utils.data import Dataset

from pipeline.warehouse import Warehouse


ACTION_VOCAB = {"fold": 0, "check": 1, "call": 2, "bet": 3, "raise": 4}
STREET_VOCAB = {"preflop": 0, "flop": 1, "turn": 2, "river": 3}
FEATURE_DIM = 4  # action_idx, street_idx, amount_norm, is_self


def build_sequences(warehouse: Warehouse, max_len: int = 60) -> tuple[np.ndarray, np.ndarray, list[tuple[str, str]]]:
    """Return X (n, max_len, FEATURE_DIM), y (n,), and ids."""
    actions = warehouse.fetch_df("SELECT * FROM RAW_ACTIONS ORDER BY hand_id, sequence_no")
    players = warehouse.fetch_df("SELECT hand_id, player_id, is_suspicious FROM RAW_PLAYERS")
    if actions.empty or players.empty:
        return np.zeros((0, max_len, FEATURE_DIM), dtype=np.float32), np.zeros((0,), dtype=np.int64), []

    actions["action_idx"] = actions["action_type"].map(ACTION_VOCAB).fillna(0).astype(int)
    actions["street_idx"] = actions["street"].map(STREET_VOCAB).fillna(0).astype(int)
    amount_norm = (actions["amount"] / (actions["amount"].max() + 1e-6)).astype(float)
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
        ids.append((hand_id, player_id))

    X = np.stack(rows) if rows else np.zeros((0, max_len, FEATURE_DIM), dtype=np.float32)
    y = np.array(labels, dtype=np.int64)
    return X, y, ids


class HandSequenceDataset(Dataset):
    def __init__(self, X: np.ndarray, y: np.ndarray) -> None:
        self.X = torch.from_numpy(X)
        self.y = torch.from_numpy(y)

    def __len__(self) -> int:
        return len(self.X)

    def __getitem__(self, idx: int) -> tuple[torch.Tensor, torch.Tensor]:
        return self.X[idx], self.y[idx]
