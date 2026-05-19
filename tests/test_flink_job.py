from __future__ import annotations

import json

from pipeline.flink.job import alerts_from_hand_json
from pipeline.flink.pair_memory_job import apply_pair_update_json, pair_update_jsons_from_hand_json
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.realtime.pair_memory import pair_memory_frame_for_hand
from pipeline.realtime.batch import score_live_hands
from pipeline.realtime.pair_memory import RollingPairMemory


def _hand() -> dict:
    return next(
        HandGenerator(
            GeneratorConfig(
                n_hands=1,
                n_players=12,
                n_tables=2,
                n_colluding_pairs=2,
                seed=321,
            )
        ).iter_hands()
    )


def test_score_live_hands_returns_reusable_frames():
    memory = RollingPairMemory()
    scored = score_live_hands([_hand()], threshold=0.0, pair_memory=memory, log=False)

    assert len(scored.hands) == 1
    assert len(scored.features) == 6
    assert len(scored.rule_flags) == 6
    assert len(scored.alerts) == 6
    assert scored.pair_stats_count == 15
    assert len(memory) == 15


def test_flink_hand_json_helper_emits_alert_json():
    memory = RollingPairMemory()
    rows = alerts_from_hand_json(json.dumps(_hand()), threshold=0.0, pair_memory=memory)

    assert len(rows) == 6
    assert len(memory) == 15
    first = json.loads(rows[0])
    assert first["alert_id"].startswith("A-")
    assert first["risk_score"] >= 0.0
    assert "model_scores" in first


def test_alert_helper_can_use_pair_memory_topic_rows():
    hand = _hand()
    pair_rows = {}
    for update_json in pair_update_jsons_from_hand_json(json.dumps(hand)):
        _, output_json = apply_pair_update_json(None, update_json)
        row = json.loads(output_json)
        pair_rows[row["pair_key"]] = row

    frame = pair_memory_frame_for_hand(hand, pair_rows)
    alerts = alerts_from_hand_json(
        json.dumps(hand),
        threshold=0.0,
        pair_memory_by_key=pair_rows,
    )

    assert len(frame) == 15
    assert len(alerts) == 6
