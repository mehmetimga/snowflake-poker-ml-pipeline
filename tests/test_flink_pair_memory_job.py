from __future__ import annotations

import json

from pipeline.flink.pair_memory_job import (
    apply_pair_update_json,
    pair_update_jsons_from_hand_json,
    pair_update_key,
    pair_update_records_from_hand,
)
from pipeline.generator import GeneratorConfig, HandGenerator


def _hand() -> dict:
    return next(
        HandGenerator(
            GeneratorConfig(
                n_hands=1,
                n_players=12,
                n_tables=2,
                n_colluding_pairs=2,
                seed=771,
            )
        ).iter_hands()
    )


def test_pair_update_records_expand_one_hand_to_all_player_pairs():
    rows = pair_update_records_from_hand(_hand())

    assert len(rows) == 15
    assert rows[0]["pair_key"] == f'{rows[0]["player_a"]}|{rows[0]["player_b"]}'
    assert rows[0]["player_a"] < rows[0]["player_b"]


def test_pair_update_json_key_helper():
    row = pair_update_jsons_from_hand_json(json.dumps(_hand()))[0]

    assert pair_update_key(row) == json.loads(row)["pair_key"]


def test_apply_pair_update_json_returns_state_and_output():
    update = {
        "pair_key": "p1|p2",
        "hand_id": "h1",
        "table_id": "t1",
        "played_at": "2026-05-01T00:00:00+00:00",
        "player_a": "p1",
        "player_b": "p2",
        "won_amount_a": -10.0,
        "won_amount_b": 10.0,
    }

    state_json, output_json = apply_pair_update_json(None, json.dumps(update))
    state2_json, output2_json = apply_pair_update_json(state_json, json.dumps(update | {"hand_id": "h2"}))

    state = json.loads(state2_json)
    output = json.loads(output2_json)
    assert state["hands_together"] == 2
    assert output["pair_key"] == "p1|p2"
    assert output["hands_together"] == 2
    assert output["soft_play_frequency"] == 1.0
    assert output["pair_memory_score"] > 0.5
