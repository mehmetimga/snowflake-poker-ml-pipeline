"""Streamlit-friendly data access wrappers over the warehouse adapter."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse


def warehouse() -> Warehouse:
    return get_warehouse()


def kpi_counts(wh: Warehouse) -> dict[str, int]:
    counts = {}
    for table in ("RAW_HANDS", "RAW_PLAYERS", "ALERTS"):
        try:
            row = wh.fetch_df(f"SELECT COUNT(*) AS n FROM {table}")
            counts[table.lower()] = int(row.iloc[0]["n"])
        except Exception:
            counts[table.lower()] = 0
    try:
        df = wh.fetch_df("SELECT risk_level, COUNT(*) AS n FROM ALERTS GROUP BY risk_level")
        for _, row in df.iterrows():
            counts[f"alerts_{row['risk_level'].lower()}"] = int(row["n"])
    except Exception:
        pass
    return counts


def alerts(wh: Warehouse, status: Optional[str] = None, risk: Optional[str] = None, limit: int = 200) -> pd.DataFrame:
    where = []
    if status:
        where.append(f"status = '{status}'")
    if risk:
        where.append(f"risk_level = '{risk}'")
    sql = "SELECT * FROM ALERTS"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY risk_score DESC LIMIT {int(limit)}"
    try:
        return wh.fetch_df(sql)
    except Exception:
        return pd.DataFrame()


def hand_detail(wh: Warehouse, hand_id: str) -> dict[str, pd.DataFrame]:
    out = {}
    for table in ("RAW_HANDS", "RAW_PLAYERS", "RAW_ACTIONS"):
        try:
            out[table.lower()] = wh.fetch_df(f"SELECT * FROM {table} WHERE hand_id = '{hand_id}'")
        except Exception:
            out[table.lower()] = pd.DataFrame()
    return out


def model_metrics(wh: Warehouse) -> pd.DataFrame:
    try:
        return wh.fetch_df("SELECT * FROM MODEL_METRICS ORDER BY trained_at DESC")
    except Exception:
        return pd.DataFrame()


def pair_stats(wh: Warehouse, player_id: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
    sql = "SELECT * FROM PAIR_STATS"
    if player_id:
        sql += f" WHERE player_a = '{player_id}' OR player_b = '{player_id}'"
    sql += f" ORDER BY pair_score DESC LIMIT {int(limit)}"
    try:
        return wh.fetch_df(sql)
    except Exception:
        return pd.DataFrame()


def models_dir() -> Path:
    return Path(get_settings().models_dir)


def vgae_scores() -> dict[str, float]:
    path = models_dir() / "vgae_scores.json"
    if not path.exists():
        return {}
    return {k: float(v) for k, v in json.loads(path.read_text()).items()}


def rule_monitoring_artifacts(registry_dir: Path | None = None) -> dict[str, Any]:
    """Load local/generated B5/B6 artifacts without requiring a warehouse."""

    root = registry_dir or (models_dir() / "registry")
    result: dict[str, Any] = {}
    for key, name in (
        ("baseline", "rule_evaluation_report.json"),
        ("window", "rule_monitoring_window.json"),
        ("report", "rule_monitoring_report.json"),
    ):
        path = root / name
        if path.is_file():
            try:
                value = json.loads(path.read_text())
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(value, dict):
                result[key] = value
    return result
