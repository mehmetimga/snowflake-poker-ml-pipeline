"""Reusable in-memory scoring for live hand batches."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from pipeline.features.engineer import compute_features
from pipeline.inference.scorer import score_live_batch
from pipeline.realtime.pair_memory import RollingPairMemory
from pipeline.realtime.pattern_search import PatternSearchConfig, realtime_pattern_scores
from pipeline.rules.engine import score_dataframe
from pipeline.warehouse.loader import hands_to_dataframes
from pipeline.warehouse.sql import unique_strings


@dataclass(frozen=True)
class LiveBatchScore:
    hands: pd.DataFrame
    actions: pd.DataFrame
    players: pd.DataFrame
    features: pd.DataFrame
    rule_flags: pd.DataFrame
    pair_memory: pd.DataFrame
    alerts: pd.DataFrame
    hand_ids: list[str]

    @property
    def pair_stats_count(self) -> int:
        return len(self.pair_memory)


def score_live_hands(
    hands: Iterable[dict],
    threshold: float = 0.5,
    pattern_search: PatternSearchConfig | None = None,
    pair_memory: RollingPairMemory | None = None,
    pair_memory_stats: pd.DataFrame | None = None,
    log: bool = True,
) -> LiveBatchScore:
    """Score live hand dictionaries without reading from the warehouse."""
    batch = list(hands)
    hand_ids = unique_strings(hand["hand_id"] for hand in batch if "hand_id" in hand)
    empty = pd.DataFrame()
    if not batch or not hand_ids:
        return LiveBatchScore(empty, empty, empty, empty, empty, empty, empty, [])

    df_hands, df_actions, df_players = hands_to_dataframes(batch)
    features = compute_features(df_hands, df_actions, df_players)
    flags = score_dataframe(features)
    if pair_memory_stats is not None:
        live_pair_memory_stats = pair_memory_stats
    elif pair_memory is not None:
        live_pair_memory_stats = pair_memory.update_from_players(df_players)
    else:
        live_pair_memory_stats = pd.DataFrame()

    pattern_config = pattern_search or PatternSearchConfig()
    pattern_scores = {}
    if pattern_config.enabled:
        preliminary_alerts = score_live_batch(
            features=features,
            rule_flags=flags,
            hands=df_hands,
            actions=df_actions,
            players=df_players,
            threshold=pattern_config.candidate_risk_score,
            log=False,
        )
        pattern_scores = realtime_pattern_scores(
            players=df_players,
            rule_flags=flags,
            preliminary_alerts=preliminary_alerts,
            config=pattern_config,
            pair_stats=live_pair_memory_stats if not live_pair_memory_stats.empty else None,
        )

    alerts = score_live_batch(
        features=features,
        rule_flags=flags,
        hands=df_hands,
        actions=df_actions,
        players=df_players,
        threshold=threshold,
        pattern_scores=pattern_scores,
        log=log,
    )
    return LiveBatchScore(
        hands=df_hands,
        actions=df_actions,
        players=df_players,
        features=features,
        rule_flags=flags,
        pair_memory=live_pair_memory_stats,
        alerts=alerts,
        hand_ids=hand_ids,
    )
