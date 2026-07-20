from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from pipeline.dl.dataset import (
    FEATURE_DIM,
    build_frozen_sequence_partitions,
    load_sequence_partitions,
    save_sequence_partitions,
)


class _Warehouse:
    def __init__(self, actions: pd.DataFrame, players: pd.DataFrame) -> None:
        self.actions = actions
        self.players = players

    def fetch_df(self, sql: str) -> pd.DataFrame:
        if "FROM RAW_ACTIONS" in sql:
            return self.actions.copy()
        if "FROM RAW_PLAYERS" in sql:
            return self.players.copy()
        raise AssertionError(sql)


def _frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    actions: list[dict] = []
    players: list[dict] = []
    amounts = {"train": (25.0, 100.0), "validation": (10.0, 1_000.0), "test": (20.0, 50.0)}
    for split, split_amounts in amounts.items():
        hand_id = f"{split}-hand"
        player_ids = (f"{split}-normal", f"{split}-positive")
        for sequence_no, (player_id, amount) in enumerate(zip(player_ids, split_amounts), 1):
            actions.append(
                {
                    "hand_id": hand_id,
                    "sequence_no": sequence_no,
                    "player_id": player_id,
                    "street": "preflop",
                    "action_type": "raise",
                    "amount": amount,
                    "dataset_split": split,
                }
            )
        for label, player_id in enumerate(player_ids):
            players.append(
                {
                    "hand_id": hand_id,
                    "player_id": player_id,
                    "is_suspicious": label,
                    "dataset_split": split,
                }
            )
    return pd.DataFrame(actions), pd.DataFrame(players)


def test_frozen_dl_partitions_use_train_scale_and_disjoint_splits():
    actions, players = _frames()
    partitions = build_frozen_sequence_partitions(_Warehouse(actions, players), max_len=4)

    assert partitions.strategy == "frozen_disjoint_players"
    assert partitions.amount_scale == 100.0
    assert partitions.train.X.shape == (2, 4, FEATURE_DIM)
    assert set(partitions.train.y) == {0, 1}
    assert {hand_id for hand_id, _ in partitions.validation.ids} == {"validation-hand"}
    # Validation is deliberately larger than train. A value >1 proves the
    # transformer did not fit a second normalization scale on the holdout.
    assert float(partitions.validation.X[:, :, 2].max()) == 10.0


def test_frozen_dl_partitions_reject_player_leakage():
    actions, players = _frames()
    players.loc[players["dataset_split"] == "validation", "player_id"] = "train-normal"
    with pytest.raises(RuntimeError, match="Player leakage"):
        build_frozen_sequence_partitions(_Warehouse(actions, players))


def test_sequence_bundle_round_trip_is_pickle_free(tmp_path: Path):
    actions, players = _frames()
    expected = build_frozen_sequence_partitions(_Warehouse(actions, players), max_len=4)
    artifact = tmp_path / "sequences.npz"

    manifest = save_sequence_partitions(expected, artifact)
    actual = load_sequence_partitions(artifact)

    assert manifest["split_strategy"] == "frozen_disjoint_players"
    assert manifest["splits"]["train"] == {"rows": 2, "positive_rows": 1}
    assert artifact.with_suffix(".manifest.json").exists()
    assert actual.amount_scale == expected.amount_scale
    assert actual.test.ids == expected.test.ids
    np.testing.assert_array_equal(actual.validation.X, expected.validation.X)
    np.testing.assert_array_equal(actual.validation.y, expected.validation.y)
