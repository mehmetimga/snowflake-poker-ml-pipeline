"""Convert generator hand dictionaries into RAW_* DataFrames for the warehouse."""

from __future__ import annotations

from typing import Iterable

import pandas as pd

from pipeline.warehouse.factory import Warehouse


def hands_to_dataframes(hands: Iterable[dict]) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    hands_rows, actions_rows, players_rows = [], [], []
    for h in hands:
        hands_rows.append(
            {
                "hand_id": h["hand_id"],
                "table_id": h["table_id"],
                "played_at": h["played_at"],
                "small_blind": h["small_blind"],
                "big_blind": h["big_blind"],
                "num_players": h["num_players"],
                "pot_size": h["pot_size"],
                "board": h.get("board") or [],
            }
        )
        for a in h["actions"]:
            actions_rows.append(
                {
                    "hand_id": h["hand_id"],
                    "sequence_no": a["sequence_no"],
                    "player_id": a["player_id"],
                    "street": a["street"],
                    "action_type": a["action_type"],
                    "amount": a["amount"],
                }
            )
        for p in h["players"]:
            players_rows.append(
                {
                    "hand_id": h["hand_id"],
                    "player_id": p["player_id"],
                    "name": p["name"],
                    "position": p["position"],
                    "stack_start": p["stack_start"],
                    "hole_cards": p.get("hole_cards"),
                    "won_amount": p["won_amount"],
                    "is_suspicious": p["is_suspicious"],
                    "collusion_pair_id": p.get("collusion_pair_id"),
                }
            )
    return (
        pd.DataFrame(hands_rows),
        pd.DataFrame(actions_rows),
        pd.DataFrame(players_rows),
    )


def load_hands(warehouse: Warehouse, hands: Iterable[dict]) -> int:
    df_hands, df_actions, df_players = hands_to_dataframes(hands)
    if df_hands.empty:
        return 0
    warehouse.write_pandas(df_hands, "RAW_HANDS")
    warehouse.write_pandas(df_actions, "RAW_ACTIONS")
    warehouse.write_pandas(df_players, "RAW_PLAYERS")
    return len(df_hands)
