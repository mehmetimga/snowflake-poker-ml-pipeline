"""Optional Qdrant pattern-search enrichment for realtime scoring."""

from __future__ import annotations

from dataclasses import dataclass
from itertools import combinations
from typing import Iterable

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient

from pipeline.config import get_settings
from pipeline.qdrant.embedding_model import PairFeatureEmbedder
from pipeline.qdrant.pattern_store import PAIR_FEATURE_COLUMNS


@dataclass(frozen=True)
class PatternSearchConfig:
    enabled: bool = False
    candidate_rule_score: float = 1.0
    candidate_risk_score: float = 0.5
    max_pairs: int = 50
    limit: int = 1
    timeout: float = 1.5


def _triggered_count(value: object) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, str) and value:
        return 1
    return 0


def _candidate_hands(rule_flags: pd.DataFrame, preliminary_alerts: pd.DataFrame, config: PatternSearchConfig) -> set[str]:
    hands: set[str] = set()
    if not rule_flags.empty:
        flagged = rule_flags[
            (rule_flags["rule_score"].astype(float) >= config.candidate_rule_score)
            | (rule_flags["triggered_rules"].apply(_triggered_count) > 0)
        ]
        hands.update(flagged["hand_id"].astype(str).tolist())

    if not preliminary_alerts.empty:
        risky = preliminary_alerts[preliminary_alerts["risk_score"].astype(float) >= config.candidate_risk_score]
        hands.update(risky["hand_id"].astype(str).tolist())

    return hands


def _live_pair_stats(players: pd.DataFrame, candidate_hands: Iterable[str], max_pairs: int) -> pd.DataFrame:
    hand_set = set(candidate_hands)
    if not hand_set or players.empty:
        return pd.DataFrame()

    rows = []
    scoped = players[players["hand_id"].astype(str).isin(hand_set)]
    for hand_id, group in scoped.groupby("hand_id"):
        ordered = group.sort_values("player_id")
        for a, b in combinations(ordered.to_dict("records"), 2):
            rows.append(
                {
                    "hand_id": str(hand_id),
                    "player_a": str(a["player_id"]),
                    "player_b": str(b["player_id"]),
                    "won_amount_a": float(a.get("won_amount", 0.0)),
                    "won_amount_b": float(b.get("won_amount", 0.0)),
                }
            )
            if len(rows) >= max_pairs:
                break
        if len(rows) >= max_pairs:
            break

    pairs = pd.DataFrame(rows)
    if pairs.empty:
        return pairs

    pairs["both_won"] = (pairs["won_amount_a"] > 0) & (pairs["won_amount_b"] > 0)
    grouped = pairs.groupby(["hand_id", "player_a", "player_b"])
    out = grouped.size().rename("hands_together").to_frame()
    sum_a = grouped["won_amount_a"].sum()
    sum_b = grouped["won_amount_b"].sum()
    mean_a = grouped["won_amount_a"].mean()
    mean_b = grouped["won_amount_b"].mean()
    co_win = grouped["both_won"].any()

    out["chip_transfer_ratio"] = sum_b / (sum_a.abs() + 1e-6)
    out["soft_play_frequency"] = (~co_win).astype(float)
    out["fold_benefit_ratio"] = mean_b / (mean_a.abs() + 1e-6)
    out["showdown_avoidance_rate"] = out["soft_play_frequency"]
    return out.reset_index()


def _best_score(points) -> float:
    if not points:
        return 0.0
    return max(float(getattr(point, "score", 0.0) or 0.0) for point in points)


def _qdrant_client(url: str, timeout: float) -> QdrantClient:
    try:
        return QdrantClient(url=url, timeout=timeout, check_compatibility=False)
    except TypeError:
        return QdrantClient(url=url, timeout=timeout)


def realtime_pattern_scores(
    players: pd.DataFrame,
    rule_flags: pd.DataFrame,
    preliminary_alerts: pd.DataFrame,
    config: PatternSearchConfig,
) -> dict[tuple[str, str], float]:
    """Return per-(hand_id, player_id) Qdrant scores for selected live pairs.

    This is fail-open by design. If Qdrant is unavailable, collections are
    missing, or a query errors, realtime scoring continues without enrichment.
    """
    if not config.enabled:
        return {}

    try:
        settings = get_settings()
        client = _qdrant_client(settings.qdrant_url, config.timeout)
        if not client.collection_exists(settings.qdrant_collusion_collection):
            print("[qdrant][realtime] collusion collection missing; skipping pattern search")
            return {}

        normal_exists = client.collection_exists(settings.qdrant_normal_collection)
        hands = _candidate_hands(rule_flags, preliminary_alerts, config)
        pair_stats = _live_pair_stats(players, hands, config.max_pairs)
        if pair_stats.empty:
            return {}

        embedder = PairFeatureEmbedder()
        vectors = embedder.encode(pair_stats[PAIR_FEATURE_COLUMNS].to_numpy(dtype=np.float32))
        out: dict[tuple[str, str], float] = {}

        for row, vector in zip(pair_stats.itertuples(index=False), vectors):
            collusion_points = client.query_points(
                collection_name=settings.qdrant_collusion_collection,
                query=vector.tolist(),
                limit=config.limit,
                with_payload=True,
            ).points
            collusion_score = _best_score(collusion_points)
            normal_score = 0.0
            if normal_exists:
                normal_points = client.query_points(
                    collection_name=settings.qdrant_normal_collection,
                    query=vector.tolist(),
                    limit=config.limit,
                    with_payload=True,
                ).points
                normal_score = _best_score(normal_points)

            pattern_score = float(np.clip(max(collusion_score - normal_score, collusion_score * 0.5), 0.0, 1.0))
            for player_id in (row.player_a, row.player_b):
                key = (str(row.hand_id), str(player_id))
                out[key] = max(out.get(key, 0.0), pattern_score)

        if out:
            print(f"[qdrant][realtime] enriched {len(out)} player-hand scores from {len(pair_stats)} pair queries")
        return out
    except Exception as exc:
        print(f"[qdrant][realtime] pattern search skipped: {exc}")
        return {}
