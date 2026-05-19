from __future__ import annotations

from pipeline.realtime.action_patterns import (
    action_events_from_hand,
    action_pattern_scores_by_player,
    detect_action_patterns,
)


def _hand() -> dict:
    return {
        "hand_id": "H-action-001",
        "table_id": "table_01",
        "played_at": "2026-05-01T00:00:00+00:00",
        "small_blind": 1.0,
        "big_blind": 2.0,
        "num_players": 3,
        "pot_size": 60.0,
        "board": ["Ah", "Kd", "7c"],
        "players": [
            {
                "player_id": "p1",
                "name": "P1",
                "position": "UTG",
                "stack_start": 200.0,
                "hole_cards": "As Ks",
                "won_amount": 0.0,
                "is_suspicious": False,
                "collusion_pair_id": None,
            },
            {
                "player_id": "p2",
                "name": "P2",
                "position": "CO",
                "stack_start": 200.0,
                "hole_cards": "2c 3c",
                "won_amount": 0.0,
                "is_suspicious": False,
                "collusion_pair_id": None,
            },
            {
                "player_id": "p3",
                "name": "P3",
                "position": "BTN",
                "stack_start": 200.0,
                "hole_cards": "Qs Qh",
                "won_amount": 60.0,
                "is_suspicious": False,
                "collusion_pair_id": None,
            },
        ],
        "actions": [
            {"sequence_no": 0, "player_id": "p1", "street": "preflop", "action_type": "raise", "amount": 6.0},
            {"sequence_no": 1, "player_id": "p2", "street": "preflop", "action_type": "call", "amount": 6.0},
            {"sequence_no": 2, "player_id": "p3", "street": "preflop", "action_type": "raise", "amount": 18.0},
            {"sequence_no": 3, "player_id": "p2", "street": "preflop", "action_type": "fold", "amount": 0.0},
            {"sequence_no": 4, "player_id": "p1", "street": "flop", "action_type": "check", "amount": 0.0},
            {"sequence_no": 5, "player_id": "p3", "street": "flop", "action_type": "check", "amount": 0.0},
        ],
    }


def test_action_events_from_hand_adds_hand_context():
    events = action_events_from_hand(_hand())

    assert len(events) == 6
    assert events[0]["action_event_id"] == "AE-H-action-001-0000"
    assert events[0]["position"] == "UTG"
    assert events[0]["amount_bb"] == 3.0
    assert events[0]["table_id"] == "table_01"


def test_detect_action_patterns_emits_pair_candidates():
    patterns = detect_action_patterns(action_events_from_hand(_hand()))
    pattern_types = {row["pattern_type"] for row in patterns}

    assert "preflop_squeeze" in pattern_types
    assert "raise_fold_benefit" in pattern_types
    assert "call_down_transfer" in pattern_types
    assert "soft_play_passive_chain" in pattern_types
    assert all(0.0 <= row["pattern_score"] <= 1.0 for row in patterns)
    assert all(row["pair_key"] == "|".join(sorted([row["player_a"], row["player_b"]])) for row in patterns)


def test_action_pattern_scores_by_player_keeps_max_score():
    patterns = detect_action_patterns(action_events_from_hand(_hand()))
    scores = action_pattern_scores_by_player(patterns)

    assert scores[("H-action-001", "p1")] > 0.0
    assert scores[("H-action-001", "p2")] > 0.0
    assert scores[("H-action-001", "p3")] > 0.0
