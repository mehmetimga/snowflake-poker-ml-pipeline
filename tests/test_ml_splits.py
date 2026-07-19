from __future__ import annotations

import pandas as pd
import pytest

from pipeline.ml.train import _partition_dataset


def _rows() -> pd.DataFrame:
    rows = []
    for split in ("train", "validation", "test", "challenge"):
        for index in range(4):
            rows.append(
                {
                    "hand_id": f"{split}-hand-{index}",
                    "player_id": f"{split}-player-{index}",
                    "dataset_split": split,
                    "is_suspicious": index % 2,
                }
            )
    return pd.DataFrame(rows)


def test_partition_dataset_honors_frozen_splits_and_excludes_challenge():
    partitions = _partition_dataset(_rows(), random_seed=42)

    assert partitions.strategy == "frozen_disjoint_players"
    assert set(partitions.train["dataset_split"]) == {"train"}
    assert set(partitions.validation["dataset_split"]) == {"validation"}
    assert set(partitions.test["dataset_split"]) == {"test"}
    assert not set(partitions.train["hand_id"]) & set(partitions.test["hand_id"])


def test_partition_dataset_rejects_player_leakage():
    frame = _rows()
    frame.loc[frame["dataset_split"] == "validation", "player_id"] = "train-player-0"
    with pytest.raises(RuntimeError, match="Player leakage"):
        _partition_dataset(frame, random_seed=42)
