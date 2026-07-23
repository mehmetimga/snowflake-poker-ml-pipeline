"""Streamlit-friendly data access wrappers over the warehouse adapter."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse


def warehouse() -> Warehouse:
    return get_warehouse()


def data_mode() -> str:
    mode = os.environ.get("ADMIN_DATA_MODE", "legacy").strip().lower()
    if mode not in {"canonical", "legacy"}:
        raise ValueError("ADMIN_DATA_MODE must be canonical or legacy")
    return mode


def kpi_counts(wh: Warehouse) -> dict[str, int]:
    if data_mode() == "canonical":
        row = wh.fetch_df(
            """
            SELECT
              (SELECT COUNT(*) FROM POKER_ML_DEMO.SPCS.POKER_HAND_EVENTS)
                AS raw_hands,
              (SELECT COUNT(*) FROM POKER_ML_DEMO.SPCS.POKER_PLAYER_CONTEXT_EVENTS)
                AS raw_players,
              (SELECT COUNT(*) FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V)
                AS alerts,
              (
                SELECT COUNT(*) FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V
                WHERE risk_probability >= 0.80
              ) AS alerts_high
            """
        )
        if row.empty:
            return {
                "raw_hands": 0,
                "raw_players": 0,
                "alerts": 0,
                "alerts_high": 0,
            }
        return {
            name: int(row.iloc[0][name])
            for name in ("raw_hands", "raw_players", "alerts", "alerts_high")
        }

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


def alerts(
    wh: Warehouse,
    status: Optional[str] = None,
    risk: Optional[str] = None,
    limit: int = 200,
) -> pd.DataFrame:
    limit = max(1, min(int(limit), 1_000))
    if data_mode() == "canonical":
        allowed_statuses = {"pending"}
        allowed_risks = {"HIGH", "MEDIUM", "LOW"}
        if status is not None and status not in allowed_statuses:
            return pd.DataFrame()
        if risk is not None and risk not in allowed_risks:
            return pd.DataFrame()
        where = []
        if status:
            where.append(f"status = '{status}'")
        if risk:
            where.append(f"risk_level = '{risk}'")
        sql = """
            SELECT *
            FROM (
              SELECT
                alert_id,
                alert_event_id,
                hand_id,
                table_id,
                highest_risk_pair,
                highest_risk_pair AS suspicious_player_id,
                CASE
                  WHEN risk_probability >= 0.80 THEN 'HIGH'
                  WHEN risk_probability >= 0.50 THEN 'MEDIUM'
                  ELSE 'LOW'
                END AS risk_level,
                risk_probability AS risk_score,
                'pending' AS status,
                policy_outcome,
                review_outcome,
                review_action,
                model_name,
                model_run_id,
                rule_evidence_count,
                played_at,
                ingested_at
              FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V
            ) canonical_alerts
        """
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += f" ORDER BY risk_score DESC, ingested_at DESC LIMIT {limit}"
        return wh.fetch_df(sql)

    where = []
    if status:
        where.append(f"status = '{status}'")
    if risk:
        where.append(f"risk_level = '{risk}'")
    sql = "SELECT * FROM ALERTS"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY risk_score DESC LIMIT {limit}"
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
