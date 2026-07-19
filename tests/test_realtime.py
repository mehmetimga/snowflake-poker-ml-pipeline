from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.config import Settings
from pipeline.features.engineer import compute_features
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.inference.scorer import score_live_batch
from pipeline.realtime import RealTimeProcessor
from pipeline.rules.engine import score_dataframe
from pipeline.warehouse.duckdb import DuckDBWarehouse
from pipeline.warehouse.loader import hands_to_dataframes
from pipeline.warehouse.migrate import run_migrations


def _hands():
    return list(
        HandGenerator(
            GeneratorConfig(
                n_hands=8,
                n_players=24,
                n_tables=3,
                n_colluding_pairs=4,
                seed=123,
            )
        ).iter_hands()
    )


def test_realtime_processor_scores_without_warehouse():
    result = RealTimeProcessor(
        threshold=0.0,
        persist_history=False,
        persist_alerts=False,
    ).process_hands(_hands())

    assert result.hands == 8
    assert result.features == 48
    assert result.rule_flags == 48
    assert result.pair_stats == 120
    assert result.alerts == 48


def test_live_scorer_includes_optional_qdrant_pattern_score():
    df_hands, df_actions, df_players = hands_to_dataframes(_hands())
    features = compute_features(df_hands, df_actions, df_players)
    flags = score_dataframe(features)
    key = (str(features.iloc[0]["hand_id"]), str(features.iloc[0]["player_id"]))

    alerts = score_live_batch(
        features=features,
        rule_flags=flags,
        hands=df_hands,
        actions=df_actions,
        players=df_players,
        threshold=0.0,
        pattern_scores={key: 0.88},
        log=False,
    )

    row = alerts[
        (alerts["hand_id"] == key[0])
        & (alerts["suspicious_player_id"] == key[1])
    ].iloc[0]
    assert row["model_scores"]["qdrant_pattern"] == pytest.approx(0.88)


def test_realtime_processor_persists_history_and_alerts_after_scoring(tmp_path: Path):
    settings = Settings(
        _env_file=None,
        WAREHOUSE_BACKEND="duckdb",
        DUCKDB_PATH=tmp_path / "warehouse.duckdb",
        MODELS_DIR=tmp_path / "models",
    )
    wh = DuckDBWarehouse(settings)
    run_migrations(wh)

    result = RealTimeProcessor(warehouse=wh, threshold=0.0).process_hands(_hands())

    assert result.hands == 8
    assert result.features == 48
    assert result.rule_flags == 48
    assert result.pair_stats == 120
    assert result.alerts == 48
    assert wh.fetch_df("SELECT COUNT(*) AS c FROM RAW_HANDS").iloc[0]["c"] == 8
    assert wh.fetch_df("SELECT COUNT(*) AS c FROM FEATURES").iloc[0]["c"] == 48
    assert wh.fetch_df("SELECT COUNT(*) AS c FROM RULE_FLAGS").iloc[0]["c"] == 48
    assert wh.fetch_df("SELECT COUNT(*) AS c FROM ALERTS").iloc[0]["c"] == 48

    wh.close()
