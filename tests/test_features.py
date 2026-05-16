from __future__ import annotations

import pandas as pd

from pipeline.features.engineer import FEATURE_COLUMNS, compute_features
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.warehouse.loader import hands_to_dataframes


def test_features_computed_for_synthetic_hands():
    gen = HandGenerator(GeneratorConfig(n_hands=100, n_players=20, n_tables=4, n_colluding_pairs=4, seed=3))
    hands = list(gen.iter_hands())
    df_hands, df_actions, df_players = hands_to_dataframes(hands)
    features = compute_features(df_hands, df_actions, df_players)
    assert not features.empty
    assert all(col in features.columns for col in FEATURE_COLUMNS)
    assert features["is_suspicious"].sum() > 0, "Expect some suspicious labels for 4 colluding pairs"
