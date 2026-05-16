"""Feature engineering — builds per-(hand_id, player_id) numeric feature rows."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd

from pipeline.warehouse import Warehouse


_RANKS = "23456789TJQKA"
_STREETS = ("preflop", "flop", "turn", "river")


CARD_FEATURES = ["rank1", "rank2", "gap", "is_suited", "is_pair", "hand_strength", "high_card", "low_card"]
POS_FEATURES = ["position_idx", "stack_bb", "is_blind", "is_late"]
STREET_FEATURES = [
    f"{st}_{m}"
    for st in _STREETS
    for m in ("bets", "raises", "calls", "checks", "folds", "invested", "aggression")
]
AGG_FEATURES = [
    "total_invested",
    "total_invest_ratio",
    "total_aggression",
    "saw_flop",
    "saw_turn",
    "saw_river",
    "saw_showdown",
]
HAND_FEATURES = ["num_raisers_preflop", "num_saw_flop", "num_players_hand", "pot_to_stack_ratio"]

FEATURE_COLUMNS: list[str] = (
    CARD_FEATURES + POS_FEATURES + STREET_FEATURES + AGG_FEATURES + HAND_FEATURES
)


_POSITION_INDEX = {"UTG": 0, "MP": 1, "CO": 2, "BTN": 3, "SB": 4, "BB": 5}


def _rank_value(card: str | None) -> int:
    if not card:
        return 0
    return _RANKS.index(card[0])


def _hole_features(hole_cards: str | None) -> dict[str, float]:
    if not hole_cards or " " not in hole_cards:
        return dict(rank1=0, rank2=0, gap=0, is_suited=0, is_pair=0, hand_strength=0, high_card=0, low_card=0)
    c1, c2 = hole_cards.split()
    r1, r2 = _rank_value(c1), _rank_value(c2)
    hi, lo = max(r1, r2), min(r1, r2)
    suited = 1.0 if c1[1] == c2[1] else 0.0
    pair = 1.0 if r1 == r2 else 0.0
    strength = (hi + lo) / 24.0 + 0.25 * pair + 0.05 * suited + 0.03 * (1.0 if hi - lo <= 1 else 0.0)
    return dict(
        rank1=float(r1),
        rank2=float(r2),
        gap=float(hi - lo),
        is_suited=suited,
        is_pair=pair,
        hand_strength=min(1.0, strength),
        high_card=float(hi),
        low_card=float(lo),
    )


def compute_features(hands: pd.DataFrame, actions: pd.DataFrame, players: pd.DataFrame) -> pd.DataFrame:
    """Pure-pandas feature engineering. Inputs match RAW_* schemas."""
    if hands.empty or players.empty:
        return pd.DataFrame(columns=["hand_id", "player_id"] + FEATURE_COLUMNS + ["is_suspicious", "computed_at"])

    actions = actions.copy()
    actions["amount"] = actions["amount"].astype(float)

    # Per-street action breakdown
    by_pl_st = actions.groupby(["hand_id", "player_id", "street"], dropna=False)
    counts = by_pl_st["action_type"].value_counts().unstack(fill_value=0)
    for col in ("bet", "raise", "call", "check", "fold"):
        if col not in counts.columns:
            counts[col] = 0
    invested = by_pl_st["amount"].sum().rename("invested").to_frame()
    per_street = counts.join(invested).reset_index()
    per_street.rename(
        columns={"bet": "bets", "raise": "raises", "call": "calls", "check": "checks", "fold": "folds"},
        inplace=True,
    )
    per_street["aggression"] = (per_street["bets"] + per_street["raises"]) / (
        per_street["calls"] + per_street["checks"] + 1e-6
    )

    # Pivot streets into wide columns
    wide_parts = []
    for st in _STREETS:
        sub = per_street[per_street["street"] == st].drop(columns=["street"])
        sub = sub.rename(columns={c: f"{st}_{c}" for c in sub.columns if c not in ("hand_id", "player_id")})
        wide_parts.append(sub)
    wide = wide_parts[0]
    for part in wide_parts[1:]:
        wide = wide.merge(part, on=["hand_id", "player_id"], how="outer")
    wide = wide.fillna(0.0)

    # Aggregates
    invested_total = actions.groupby(["hand_id", "player_id"])["amount"].sum().rename("total_invested")
    saw_street = actions.groupby(["hand_id", "player_id"])["street"].apply(set).rename("streets_seen")
    agg = pd.concat([invested_total, saw_street], axis=1).reset_index()
    agg["saw_flop"] = agg["streets_seen"].apply(lambda s: float("flop" in s))
    agg["saw_turn"] = agg["streets_seen"].apply(lambda s: float("turn" in s))
    agg["saw_river"] = agg["streets_seen"].apply(lambda s: float("river" in s))
    agg["saw_showdown"] = agg["saw_river"]
    agg.drop(columns=["streets_seen"], inplace=True)

    # Hand-level aggregates
    preflop_raises_per_hand = (
        actions[(actions["street"] == "preflop") & (actions["action_type"] == "raise")]
        .groupby("hand_id")["player_id"]
        .nunique()
        .rename("num_raisers_preflop")
    )
    saw_flop_per_hand = (
        actions[actions["street"] == "flop"]
        .groupby("hand_id")["player_id"]
        .nunique()
        .rename("num_saw_flop")
    )
    players_per_hand = players.groupby("hand_id")["player_id"].nunique().rename("num_players_hand")
    pot_per_hand = hands.set_index("hand_id")["pot_size"].rename("pot_size")
    hand_level = pd.concat(
        [preflop_raises_per_hand, saw_flop_per_hand, players_per_hand, pot_per_hand],
        axis=1,
    ).reset_index().fillna(0.0)
    hand_level["pot_to_stack_ratio"] = hand_level["pot_size"] / (100.0 * 1.0 + 1e-6)
    hand_level.drop(columns=["pot_size"], inplace=True)

    # Player-level info
    players_view = players[["hand_id", "player_id", "position", "stack_start", "hole_cards", "is_suspicious"]].copy()
    players_view["position_idx"] = players_view["position"].map(_POSITION_INDEX).fillna(0).astype(float)
    players_view["stack_bb"] = players_view["stack_start"] / hands.set_index("hand_id")["big_blind"].reindex(players_view["hand_id"]).values
    players_view["is_blind"] = players_view["position"].isin(["SB", "BB"]).astype(float)
    players_view["is_late"] = players_view["position"].isin(["BTN", "CO"]).astype(float)

    hole_df = players_view["hole_cards"].apply(_hole_features).apply(pd.Series)
    players_view = pd.concat([players_view.drop(columns=["hole_cards"]), hole_df], axis=1)

    df = players_view.merge(wide, on=["hand_id", "player_id"], how="left")
    df = df.merge(agg, on=["hand_id", "player_id"], how="left")
    df = df.merge(hand_level, on="hand_id", how="left")
    df = df.fillna(0.0)

    # total invest ratio + aggression
    df["total_invest_ratio"] = df["total_invested"] / (df["stack_start"].astype(float) + 1e-6)
    df["total_aggression"] = (
        df[[f"{st}_bets" for st in _STREETS] + [f"{st}_raises" for st in _STREETS]].sum(axis=1)
    ) / (
        df[[f"{st}_calls" for st in _STREETS] + [f"{st}_checks" for st in _STREETS]].sum(axis=1) + 1e-6
    )
    df["is_suspicious"] = df["is_suspicious"].astype(float)
    df["computed_at"] = datetime.now(tz=timezone.utc).isoformat()

    cols = ["hand_id", "player_id"] + FEATURE_COLUMNS + ["is_suspicious", "computed_at"]
    for c in cols:
        if c not in df.columns:
            df[c] = 0.0
    return df[cols]


def build_features_from_warehouse(warehouse: Warehouse) -> pd.DataFrame:
    hands = warehouse.fetch_df("SELECT * FROM RAW_HANDS")
    actions = warehouse.fetch_df("SELECT * FROM RAW_ACTIONS")
    players = warehouse.fetch_df("SELECT * FROM RAW_PLAYERS")
    df = compute_features(hands, actions, players)
    if not df.empty:
        warehouse.execute("DELETE FROM FEATURES")
        warehouse.write_pandas(df, "FEATURES")
    return df


def prepare_matrix(df: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    X = df[FEATURE_COLUMNS].astype("float32").to_numpy()
    y = df["is_suspicious"].astype("int64").to_numpy()
    return X, y
