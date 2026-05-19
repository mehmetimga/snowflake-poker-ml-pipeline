from __future__ import annotations

import json

from pipeline.flink.action_pattern_job import action_pattern_jsons_from_hand_json, action_pattern_key


def _hand() -> dict:
    return {
        "hand_id": "H-flink-action-001",
        "table_id": "table_02",
        "played_at": "2026-05-01T00:00:00+00:00",
        "small_blind": 1.0,
        "big_blind": 2.0,
        "num_players": 3,
        "pot_size": 48.0,
        "board": [],
        "players": [
            {"player_id": "a", "name": "A", "position": "UTG", "stack_start": 200.0, "won_amount": 0.0},
            {"player_id": "b", "name": "B", "position": "CO", "stack_start": 200.0, "won_amount": 0.0},
            {"player_id": "c", "name": "C", "position": "BTN", "stack_start": 200.0, "won_amount": 48.0},
        ],
        "actions": [
            {"sequence_no": 0, "player_id": "a", "street": "preflop", "action_type": "raise", "amount": 6.0},
            {"sequence_no": 1, "player_id": "b", "street": "preflop", "action_type": "call", "amount": 6.0},
            {"sequence_no": 2, "player_id": "c", "street": "preflop", "action_type": "raise", "amount": 18.0},
            {"sequence_no": 3, "player_id": "b", "street": "preflop", "action_type": "fold", "amount": 0.0},
        ],
    }


def test_action_pattern_jsons_from_hand_json_returns_serialized_patterns():
    rows = action_pattern_jsons_from_hand_json(json.dumps(_hand()))

    assert rows
    decoded = [json.loads(row) for row in rows]
    assert {row["pattern_type"] for row in decoded} >= {
        "preflop_squeeze",
        "raise_fold_benefit",
        "call_down_transfer",
    }
    assert action_pattern_key(rows[0]) == decoded[0]["pair_key"]
