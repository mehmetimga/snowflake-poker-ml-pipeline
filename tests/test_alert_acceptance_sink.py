from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from scripts.verify_alert_acceptance_sink import (
    TABLES,
    expected_admin_ids,
    read_required_offsets,
    wait_for_sink_rows,
)


class FakeWarehouse:
    kind = "snowflake"

    def __init__(self, expected: dict[str, int], admin_ids: set[str]) -> None:
        self.expected = expected
        self.admin_ids = admin_ids

    def fetch_df(self, sql: str, params=None) -> pd.DataFrame:
        normalized = " ".join(sql.split())
        for name, table in TABLES.items():
            if f"SPCS.{table}" in normalized:
                return pd.DataFrame([{"n": self.expected[name]}])
        if "POKER_ALERT_REVIEW_V" in normalized:
            return pd.DataFrame(
                [{"alert_id": value} for value in sorted(self.admin_ids)]
            )
        if "POKER_EVENT_ENVELOPES" in normalized:
            return pd.DataFrame(
                [
                    {
                        "source_topic": "poker.synthetic.risk-scores.v1",
                        "source_partition": 1,
                        "required_offset": 42,
                    }
                ]
            )
        raise AssertionError(normalized)


def manifest(tmp_path: Path) -> dict:
    oracle = tmp_path / "private_oracle/score_expectations.jsonl"
    oracle.parent.mkdir(parents=True)
    oracle.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "expected_admin_visible": True,
                        "expected_admin_row_id": "alert-1",
                    }
                ),
                json.dumps(
                    {
                        "expected_admin_visible": False,
                        "expected_admin_row_id": None,
                    }
                ),
            ]
        )
        + "\n"
    )
    return {
        "acceptance_pack": str(tmp_path),
        "dataset_id": "acceptance-d7",
        "expected_counts": {
            "hands": 16,
            "player_context": 96,
            "pair_features": 240,
            "risk_scores": 16,
            "rule_evidence": 176,
            "review_decisions": 16,
            "risk_alerts": 14,
        },
    }


def test_sink_reconciliation_requires_exact_counts_and_admin_ids(
    tmp_path: Path,
) -> None:
    value = manifest(tmp_path)
    expected = {
        name: int(value["expected_counts"][name]) for name in TABLES
    }
    expected["hands"] = 16
    warehouse = FakeWarehouse(expected, {"alert-1"})

    result = wait_for_sink_rows(warehouse, value, timeout_seconds=0.1)

    assert result["status"] == "passed"
    assert result["counts"] == expected
    assert result["admin_alert_ids"] == ["alert-1"]
    assert expected_admin_ids(value) == {"alert-1"}


def test_sink_progress_uses_persisted_source_offsets(tmp_path: Path) -> None:
    value = manifest(tmp_path)
    expected = {
        name: int(value["expected_counts"][name]) for name in TABLES
    }
    expected["hands"] = 16
    warehouse = FakeWarehouse(expected, {"alert-1"})

    assert read_required_offsets(warehouse, value["dataset_id"]) == {
        ("poker.synthetic.risk-scores.v1", 1): 42
    }
