"""Ensemble inference: runs all models and writes ALERTS rows."""

from __future__ import annotations

import json
from collections.abc import Iterable
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch

from pipeline.config import get_settings
from pipeline.meta.dataset import assemble_dataset, assemble_dataset_from_frames
from pipeline.meta.wide_and_deep import WideAndDeep
from pipeline.warehouse import Warehouse, get_warehouse
from pipeline.warehouse.sql import delete_by_values, sql_string_list, unique_strings


class RiskLevel(str, Enum):
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"


def _risk_level(score: float) -> RiskLevel:
    if score >= 0.75:
        return RiskLevel.HIGH
    if score >= 0.5:
        return RiskLevel.MEDIUM
    return RiskLevel.LOW


def _hand_table_map(warehouse: Warehouse, hand_ids: Iterable[object] | None = None) -> dict[str, str]:
    ids_filter = unique_strings(hand_ids or [])
    if hand_ids is not None and not ids_filter:
        return {}
    if ids_filter:
        df = warehouse.fetch_df(
            f"SELECT hand_id, table_id FROM RAW_HANDS WHERE hand_id IN ({sql_string_list(ids_filter)})"
        )
    else:
        df = warehouse.fetch_df("SELECT hand_id, table_id FROM RAW_HANDS")
    return dict(zip(df["hand_id"], df["table_id"]))


def _hand_table_map_from_frame(hands: pd.DataFrame) -> dict[str, str]:
    if hands.empty:
        return {}
    return dict(zip(hands["hand_id"], hands["table_id"]))


def _triggered_rules_map(
    warehouse: Warehouse,
    hand_ids: Iterable[object] | None = None,
) -> dict[tuple[str, str], list[str]]:
    ids_filter = unique_strings(hand_ids or [])
    if hand_ids is not None and not ids_filter:
        return {}
    if ids_filter:
        id_list = sql_string_list(ids_filter)
        df = warehouse.fetch_df(
            "SELECT hand_id, player_id, triggered_rules "
            f"FROM RULE_FLAGS WHERE hand_id IN ({id_list})"
        )
    else:
        df = warehouse.fetch_df("SELECT hand_id, player_id, triggered_rules FROM RULE_FLAGS")
    out: dict[tuple[str, str], list[str]] = {}
    for _, row in df.iterrows():
        out[(row["hand_id"], row["player_id"])] = _parse_triggered_rules(row.get("triggered_rules", None))
    return out


def _parse_triggered_rules(raw: object) -> list[str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = []
    if raw is None:
        raw = []
    return list(raw)


def _triggered_rules_map_from_frame(flags: pd.DataFrame) -> dict[tuple[str, str], list[str]]:
    out: dict[tuple[str, str], list[str]] = {}
    for _, row in flags.iterrows():
        out[(row["hand_id"], row["player_id"])] = _parse_triggered_rules(row.get("triggered_rules", None))
    return out


def _score_vectors(wide: np.ndarray, deep: np.ndarray, models_dir: Path) -> tuple[np.ndarray, str]:
    meta_path = models_dir / "meta_wide_and_deep.pt"
    if meta_path.exists():
        device = torch.device("cpu")
        model = WideAndDeep(wide_dim=wide.shape[1], deep_dim=deep.shape[1]).to(device)
        model.load_state_dict(torch.load(meta_path, map_location=device))
        model.eval()
        with torch.no_grad():
            scores = torch.sigmoid(
                model(torch.from_numpy(wide), torch.from_numpy(deep))
            ).cpu().numpy()
        return scores.reshape(-1), "meta_wide_and_deep"

    # Fallback: hand-weighted ensemble
    weights = np.array([0.23, 0.23, 0.23, 0.09, 0.10, 0.12], dtype=np.float32)
    rule_norm = wide[:, 5] / 120.0  # rule_score is approx 0..120
    wide_norm = wide.copy()
    wide_norm[:, 5] = rule_norm
    return (wide_norm @ weights).clip(0, 1).reshape(-1), "hand_weighted_ensemble"


def _alerts_dataframe(
    ids: list[tuple[str, str]],
    scores: np.ndarray,
    wide: np.ndarray,
    hand_table: dict[str, str],
    triggered: dict[tuple[str, str], list[str]],
    threshold: float,
    source: str,
) -> pd.DataFrame:
    rows = []
    created_at = datetime.now(tz=timezone.utc).isoformat()
    for i, (hand_id, player_id) in enumerate(ids):
        score = float(scores[i])
        if score < threshold:
            continue
        rows.append(
            {
                "alert_id": f"A-{uuid.uuid4().hex[:12]}",
                "hand_id": hand_id,
                "table_id": hand_table.get(hand_id, "unknown"),
                "suspicious_player_id": player_id,
                "risk_score": score,
                "risk_level": _risk_level(score).value,
                "triggered_rules": triggered.get((hand_id, player_id), []),
                "model_scores": {
                    "xgboost": float(wide[i, 0]),
                    "catboost": float(wide[i, 1]),
                    "lightgbm": float(wide[i, 2]),
                    "vgae_anomaly": float(wide[i, 3]),
                    "qdrant_pattern": float(wide[i, 4]),
                    "rule_score": float(wide[i, 5]),
                    "meta": score,
                    "source": source,
                },
                "created_at": created_at,
                "status": "pending",
            }
        )
    return pd.DataFrame(rows)


def score_warehouse(
    warehouse: Warehouse | None = None,
    threshold: float = 0.5,
    hand_ids: Iterable[object] | None = None,
    replace_existing: bool = True,
) -> pd.DataFrame:
    """Score the FEATURES table and emit ALERTS for hands above threshold."""
    settings = get_settings()
    wh = warehouse or get_warehouse()
    models_dir = Path(settings.models_dir)
    ids_filter = unique_strings(hand_ids or [])

    wide, deep, y, ids = assemble_dataset(
        wh,
        models_dir,
        hand_ids=ids_filter if hand_ids is not None else None,
    )
    if len(ids) == 0:
        if replace_existing and hand_ids is not None:
            delete_by_values(wh, "ALERTS", "hand_id", ids_filter)
        print("[inference] no data to score.")
        return pd.DataFrame()

    scores, source = _score_vectors(wide, deep, models_dir)
    hand_table = _hand_table_map(wh, ids_filter if hand_ids is not None else None)
    triggered = _triggered_rules_map(wh, ids_filter if hand_ids is not None else None)
    df = _alerts_dataframe(ids, scores, wide, hand_table, triggered, threshold, source)
    if replace_existing:
        if hand_ids is None:
            wh.execute("DELETE FROM ALERTS")
        else:
            delete_by_values(wh, "ALERTS", "hand_id", ids_filter)
    if not df.empty:
        wh.write_pandas(df, "ALERTS")
        print(f"[inference] wrote {len(df)} alerts (source={source})")
    return df


def score_live_batch(
    features: pd.DataFrame,
    rule_flags: pd.DataFrame,
    hands: pd.DataFrame,
    actions: pd.DataFrame,
    players: pd.DataFrame,
    threshold: float = 0.5,
    pattern_scores: dict[tuple[str, str], float] | None = None,
    log: bool = True,
) -> pd.DataFrame:
    """Score a live Kafka batch without reading from the warehouse."""
    settings = get_settings()
    models_dir = Path(settings.models_dir)

    wide, deep, y, ids = assemble_dataset_from_frames(
        features=features,
        flags=rule_flags,
        models_dir=models_dir,
        actions=actions,
        players=players,
        pattern_scores=pattern_scores,
    )
    if len(ids) == 0:
        if log:
            print("[inference] no data to score.")
        return pd.DataFrame()

    scores, source = _score_vectors(wide, deep, models_dir)
    df = _alerts_dataframe(
        ids=ids,
        scores=scores,
        wide=wide,
        hand_table=_hand_table_map_from_frame(hands),
        triggered=_triggered_rules_map_from_frame(rule_flags),
        threshold=threshold,
        source=source,
    )
    if log and not df.empty:
        print(f"[inference] scored {len(df)} live alerts (source={source})")
    return df


def run_inference(warehouse: Warehouse | None = None, threshold: float = 0.5) -> pd.DataFrame:
    return score_warehouse(warehouse, threshold)
