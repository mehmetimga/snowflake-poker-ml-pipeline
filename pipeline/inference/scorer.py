"""Ensemble inference: runs all models and writes ALERTS rows."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import torch

from pipeline.config import get_settings
from pipeline.meta.dataset import assemble_dataset
from pipeline.meta.wide_and_deep import WideAndDeep
from pipeline.warehouse import Warehouse, get_warehouse


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


def _hand_table_map(warehouse: Warehouse) -> dict[str, str]:
    df = warehouse.fetch_df("SELECT hand_id, table_id FROM RAW_HANDS")
    return dict(zip(df["hand_id"], df["table_id"]))


def _triggered_rules_map(warehouse: Warehouse) -> dict[tuple[str, str], list[str]]:
    df = warehouse.fetch_df("SELECT hand_id, player_id, triggered_rules FROM RULE_FLAGS")
    out: dict[tuple[str, str], list[str]] = {}
    for _, row in df.iterrows():
        raw = row.get("triggered_rules", None)
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except Exception:
                raw = []
        if raw is None:
            raw = []
        out[(row["hand_id"], row["player_id"])] = list(raw)
    return out


def score_warehouse(warehouse: Warehouse | None = None, threshold: float = 0.5) -> pd.DataFrame:
    """Score the FEATURES table and emit ALERTS for hands above threshold."""
    settings = get_settings()
    wh = warehouse or get_warehouse()
    models_dir = Path(settings.models_dir)

    wide, deep, y, ids = assemble_dataset(wh, models_dir)
    if len(ids) == 0:
        print("[inference] no data to score.")
        return pd.DataFrame()

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
        source = "meta_wide_and_deep"
    else:
        # Fallback: hand-weighted ensemble
        weights = np.array([0.25, 0.25, 0.25, 0.10, 0.0, 0.15], dtype=np.float32)
        rule_norm = wide[:, 5] / 120.0  # rule_score is approx 0..120
        wide_norm = wide.copy()
        wide_norm[:, 5] = rule_norm
        scores = (wide_norm @ weights).clip(0, 1)
        source = "hand_weighted_ensemble"

    hand_table = _hand_table_map(wh)
    triggered = _triggered_rules_map(wh)
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
                    "rule_score": float(wide[i, 5]),
                    "meta": score,
                    "source": source,
                },
                "created_at": created_at,
                "status": "pending",
            }
        )
    df = pd.DataFrame(rows)
    if not df.empty:
        wh.execute("DELETE FROM ALERTS")
        wh.write_pandas(df, "ALERTS")
        print(f"[inference] wrote {len(df)} alerts (source={source})")
    return df


def run_inference(warehouse: Warehouse | None = None, threshold: float = 0.5) -> pd.DataFrame:
    return score_warehouse(warehouse, threshold)
