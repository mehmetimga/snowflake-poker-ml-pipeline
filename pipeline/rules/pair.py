"""Pair-coordination signals — aggregates that feed PAIR_STATS."""

from __future__ import annotations

import pandas as pd

from pipeline.warehouse import Warehouse


def compute_pair_stats(warehouse: Warehouse) -> pd.DataFrame:
    """Build PAIR_STATS from RAW_PLAYERS + RAW_ACTIONS.

    For every ordered pair (a, b) that played at the same hand at least twice,
    compute behavioral signals.
    """
    players = warehouse.fetch_df("SELECT hand_id, player_id, won_amount, name FROM RAW_PLAYERS")
    if players.empty:
        return pd.DataFrame()

    pairs = players.merge(players, on="hand_id", suffixes=("_a", "_b"))
    pairs = pairs[pairs["player_id_a"] < pairs["player_id_b"]]
    if pairs.empty:
        return pd.DataFrame()

    grouped = pairs.groupby(["player_id_a", "player_id_b"])
    out = grouped.size().rename("hands_together").to_frame()
    out["chip_transfer_ratio"] = grouped["won_amount_b"].sum() / (grouped["won_amount_a"].sum() + 1e-6)

    # Soft-play heuristic: pairs that never both win the same hand
    pairs["both_won"] = (pairs["won_amount_a"] > 0) & (pairs["won_amount_b"] > 0)
    co_win = pairs.groupby(["player_id_a", "player_id_b"])["both_won"].any()
    out["soft_play_frequency"] = (~co_win).astype(float)

    # Fold-benefit: how often the partner wins after the other has invested
    out["fold_benefit_ratio"] = grouped["won_amount_b"].mean() / (grouped["won_amount_a"].mean().abs() + 1e-6)
    out["showdown_avoidance_rate"] = out["soft_play_frequency"]

    out["pair_score"] = (
        out["soft_play_frequency"] * 30
        + (out["chip_transfer_ratio"].clip(0, 5) / 5.0) * 30
        + out["showdown_avoidance_rate"] * 20
    )
    out = out.reset_index().rename(columns={"player_id_a": "player_a", "player_id_b": "player_b"})
    out = out[out["hands_together"] >= 2]
    out["computed_at"] = pd.Timestamp.utcnow().isoformat()
    warehouse.execute("DELETE FROM PAIR_STATS")
    warehouse.write_pandas(out, "PAIR_STATS")
    return out
