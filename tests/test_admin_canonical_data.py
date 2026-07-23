from __future__ import annotations

import pandas as pd

from admin import data_access


class FakeWarehouse:
    kind = "snowflake"

    def __init__(self, frames: list[pd.DataFrame]) -> None:
        self.frames = frames
        self.queries: list[str] = []

    def fetch_df(self, sql: str, params=None) -> pd.DataFrame:
        self.queries.append(" ".join(sql.split()))
        return self.frames.pop(0)


def test_canonical_kpis_read_only_new_spcs_tables(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_DATA_MODE", "canonical")
    warehouse = FakeWarehouse(
        [
            pd.DataFrame(
                [
                    {
                        "raw_hands": 16,
                        "raw_players": 96,
                        "alerts": 14,
                        "alerts_high": 10,
                    }
                ]
            )
        ]
    )

    assert data_access.kpi_counts(warehouse) == {
        "raw_hands": 16,
        "raw_players": 96,
        "alerts": 14,
        "alerts_high": 10,
    }
    assert "POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V" in warehouse.queries[0]
    assert "FROM ALERTS" not in warehouse.queries[0]


def test_canonical_alert_reader_is_bounded_and_lineage_rich(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_DATA_MODE", "canonical")
    expected = pd.DataFrame([{"alert_id": "alert-1", "risk_level": "HIGH"}])
    warehouse = FakeWarehouse([expected])

    actual = data_access.alerts(
        warehouse, status="pending", risk="HIGH", limit=100_000
    )

    assert actual.equals(expected)
    query = warehouse.queries[0]
    assert "POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V" in query
    assert "highest_risk_pair" in query
    assert "rule_evidence_count" in query
    assert "LIMIT 1000" in query


def test_legacy_reader_remains_explicit_compatibility_mode(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_DATA_MODE", "legacy")
    expected = pd.DataFrame([{"alert_id": "legacy-alert"}])
    warehouse = FakeWarehouse([expected])

    assert data_access.alerts(warehouse, limit=25).equals(expected)
    assert "SELECT * FROM ALERTS" in warehouse.queries[0]


def test_canonical_reader_rejects_legacy_status_without_query(monkeypatch) -> None:
    monkeypatch.setenv("ADMIN_DATA_MODE", "canonical")
    warehouse = FakeWarehouse([])

    assert data_access.alerts(warehouse, status="confirmed").empty
    assert warehouse.queries == []
