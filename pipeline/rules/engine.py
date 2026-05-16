from __future__ import annotations

import json
from typing import Iterable

import pandas as pd

from pipeline.warehouse import Warehouse

from . import definitions as d
from .types import PlayerHandStats, RuleFlags, RuleWeights


class RuleEngine:
    """Per-(hand, player) collusion rule scoring.

    Scores are additive over fired rules; range [0, ~120].
    """

    def __init__(self, weights: RuleWeights | None = None, eligibility_min_players: int = 3) -> None:
        self.weights = weights or RuleWeights()
        self.eligibility_min_players = eligibility_min_players

    def check_player_hand(self, stats: PlayerHandStats, num_players_in_hand: int) -> RuleFlags:
        flags = RuleFlags()
        flags.flag_eligible = num_players_in_hand >= self.eligibility_min_players
        if not flags.flag_eligible:
            return flags

        score = 0.0
        triggered: list[str] = []
        for name, predicate, weight, attr in (
            ("PRE_MW", d.check_pre_mw, self.weights.pre_mw, "flag_pre_mw"),
            ("PRE_OFOLD_COMMIT", d.check_pre_ofold_commit, self.weights.pre_ofold_commit, "flag_pre_ofold_commit"),
            ("POST_OFOLD_COMMIT", d.check_post_ofold_commit, self.weights.post_ofold_commit, "flag_post_ofold_commit"),
            ("HIGH_INVEST_FOLD", d.check_high_invest_fold, self.weights.high_invest_fold, "flag_high_invest_fold"),
            ("POSITION_ANOMALY", d.check_position_anomaly, self.weights.position_anomaly, "flag_position_anomaly"),
        ):
            if predicate(stats):
                setattr(flags, attr, True)
                triggered.append(name)
                score += weight
        flags.triggered_rules = triggered
        flags.rule_score = score
        return flags


def _row_to_stats(row: pd.Series) -> tuple[PlayerHandStats, int]:
    folded_street: str | None = None
    for st in ("preflop", "flop", "turn", "river"):
        if row.get(f"{st}_folds", 0.0) > 0:
            folded_street = st
            break
    postflop_invested = sum(row.get(f"{st}_invested", 0.0) for st in ("flop", "turn", "river"))
    postflop_calls = sum(row.get(f"{st}_calls", 0.0) for st in ("flop", "turn", "river"))
    postflop_checks = sum(row.get(f"{st}_checks", 0.0) for st in ("flop", "turn", "river"))
    postflop_bets = sum(row.get(f"{st}_bets", 0.0) for st in ("flop", "turn", "river"))
    postflop_raises = sum(row.get(f"{st}_raises", 0.0) for st in ("flop", "turn", "river"))

    postflop_agg = (postflop_bets + postflop_raises) / (postflop_calls + postflop_checks + 1e-6)
    preflop_agg = (row.get("preflop_bets", 0.0) + row.get("preflop_raises", 0.0)) / (
        row.get("preflop_calls", 0.0) + row.get("preflop_checks", 0.0) + 1e-6
    )

    stats = PlayerHandStats(
        hand_id=str(row["hand_id"]),
        player_id=str(row["player_id"]),
        position=_decode_position(row.get("position_idx", 0)),
        stack_start=100.0,  # rough demo proxy; we use ratios anyway
        total_invested=float(row.get("total_invested", 0.0)),
        preflop_raises_player=int(row.get("preflop_raises", 0)),
        preflop_raises_hand=int(row.get("num_raisers_preflop", 0)),
        players_to_flop=int(row.get("num_saw_flop", 0)),
        saw_flop=bool(row.get("saw_flop", 0.0) > 0),
        saw_river=bool(row.get("saw_river", 0.0) > 0),
        folded_street=folded_street,
        preflop_invested=float(row.get("preflop_invested", 0.0)),
        postflop_invested=float(postflop_invested),
        preflop_aggression=float(preflop_agg),
        postflop_aggression=float(postflop_agg),
    )
    return stats, int(row.get("num_players_hand", 0))


_POSITIONS = ["UTG", "MP", "CO", "BTN", "SB", "BB"]


def _decode_position(idx: float) -> str:
    i = int(idx)
    if 0 <= i < len(_POSITIONS):
        return _POSITIONS[i]
    return "UTG"


def score_dataframe(features_df: pd.DataFrame, engine: RuleEngine | None = None) -> pd.DataFrame:
    engine = engine or RuleEngine()
    out_rows = []
    for _, row in features_df.iterrows():
        stats, num_players = _row_to_stats(row)
        flags = engine.check_player_hand(stats, num_players)
        out_rows.append(
            {
                "hand_id": stats.hand_id,
                "player_id": stats.player_id,
                "flag_eligible": flags.flag_eligible,
                "flag_pre_mw": flags.flag_pre_mw,
                "flag_pre_ofold_commit": flags.flag_pre_ofold_commit,
                "flag_post_ofold_commit": flags.flag_post_ofold_commit,
                "flag_high_invest_fold": flags.flag_high_invest_fold,
                "flag_position_anomaly": flags.flag_position_anomaly,
                "rule_score": flags.rule_score,
                "triggered_rules": flags.triggered_rules,
            }
        )
    return pd.DataFrame(out_rows)


def build_rule_flags_from_warehouse(warehouse: Warehouse, features_df: pd.DataFrame | None = None) -> pd.DataFrame:
    if features_df is None:
        features_df = warehouse.fetch_df("SELECT * FROM FEATURES")
    df = score_dataframe(features_df)
    if not df.empty:
        warehouse.execute("DELETE FROM RULE_FLAGS")
        warehouse.write_pandas(df, "RULE_FLAGS")
    return df
