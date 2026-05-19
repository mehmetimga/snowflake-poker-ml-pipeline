from __future__ import annotations

import pandas as pd

from pipeline.realtime.pair_memory import RollingPairMemory, pair_key


def test_pair_key_normalizes_order():
    assert pair_key("b", "a") == ("a", "b")


def test_rolling_pair_memory_updates_pair_features():
    players = pd.DataFrame(
        [
            {"hand_id": "h1", "player_id": "p1", "won_amount": -10.0},
            {"hand_id": "h1", "player_id": "p2", "won_amount": 10.0},
            {"hand_id": "h2", "player_id": "p2", "won_amount": 15.0},
            {"hand_id": "h2", "player_id": "p1", "won_amount": -15.0},
        ]
    )
    memory = RollingPairMemory()

    rows = memory.update_from_players(players)
    snapshot = memory.snapshot()

    assert len(rows) == 2
    assert len(snapshot) == 1
    row = snapshot.iloc[0]
    assert row["player_a"] == "p1"
    assert row["player_b"] == "p2"
    assert row["hands_together"] == 2
    assert row["soft_play_frequency"] == 1.0
    assert row["pair_memory_score"] > 0.5


def test_rolling_pair_memory_evicts_old_pairs():
    players = pd.DataFrame(
        [
            {"hand_id": "h1", "player_id": "p1", "won_amount": 0.0},
            {"hand_id": "h1", "player_id": "p2", "won_amount": 0.0},
            {"hand_id": "h2", "player_id": "p3", "won_amount": 0.0},
            {"hand_id": "h2", "player_id": "p4", "won_amount": 0.0},
        ]
    )
    memory = RollingPairMemory(max_pairs=1)
    memory.update_from_players(players)

    snapshot = memory.snapshot()

    assert len(snapshot) == 1
    assert snapshot.iloc[0]["player_a"] == "p3"
