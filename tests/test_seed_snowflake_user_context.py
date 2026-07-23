from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.seed_snowflake_user_context import (
    CURRENT_VIEW_DDL,
    DDL,
    build_acceptance_seed_data,
    build_seed_data,
    seed_context_table,
)


def test_seed_data_matches_generated_pokerkit_hand_players() -> None:
    frame, hands = build_seed_data(
        dataset_id="seed-contract-v1",
        tenant_id="tenant-a",
        product_id="poker",
        players=24,
        hands=8,
        tables=3,
        pairs=4,
        seed=909,
        hand_start_at=datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc),
    )

    assert len(frame) == 24
    assert len(hands) == 8
    context_players = set(frame["user_id"])
    hand_players = {
        player["player_id"] for hand in hands for player in hand["payload"]["players"]
    }
    assert hand_players <= context_players
    assert set(frame["tenant_id"]) == {"tenant-a"}
    assert set(frame["product_id"]) == {"poker"}
    assert frame["effective_at"].max() < pd.Timestamp(
        min(hand["payload"]["played_at"] for hand in hands)
    )


def test_seed_creates_history_and_current_view_before_writing() -> None:
    class Warehouse:
        kind = "snowflake"

        def __init__(self) -> None:
            self.statements: list[tuple[str, tuple | None]] = []
            self.writes: list[tuple[pd.DataFrame, str]] = []

        def execute(self, sql: str, params: tuple | None = None) -> None:
            self.statements.append((sql, params))

        def write_pandas(
            self, frame: pd.DataFrame, table: str, mode: str = "append"
        ) -> None:
            assert mode == "append"
            self.writes.append((frame.copy(), table))

        def fetch_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
            self.statements.append((sql, params))
            return pd.DataFrame([{"row_count": 2}])

    warehouse = Warehouse()
    frame = pd.DataFrame(
        [
            {"dataset_id": "seed-v1", "user_id": "A"},
            {"dataset_id": "seed-v1", "user_id": "B"},
        ]
    )

    assert seed_context_table(warehouse, frame, dataset_id="seed-v1") == 2
    sql = "\n".join(statement for statement, _ in warehouse.statements)
    assert DDL in sql
    assert CURRENT_VIEW_DDL in sql
    assert "DELETE FROM POKER_USER_CONTEXT_HISTORY" in sql
    assert warehouse.writes[0][1] == "POKER_USER_CONTEXT_HISTORY"


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(
            json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
            for row in rows
        )
    )


def _hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_acceptance_seed_loads_only_observed_users(tmp_path: Path) -> None:
    users = [
        {
            "user_id": user_id,
            "context_version": 1,
            "effective_at": "2026-08-31T00:00:00Z",
            "account_created_at": "2025-01-01T00:00:00Z",
            "country_bucket": "TR",
            "timezone": "Europe/Istanbul",
            "acquisition_channel": "organic",
            "kyc_level": "verified",
            "account_status": "active",
            "bankroll_bucket": "medium",
            "preferred_stake_bucket": "low",
            "skill_rating": 0.5,
            "device_id": f"device-{user_id}",
            "network_cluster_id": f"network-{user_id}",
        }
        for user_id in ("A", "B")
    ]
    hands = [
        {
            "tenant_id": "demo",
            "product_id": "poker",
            "dataset_id": "acceptance-v1",
            "payload": {
                "played_at": "2026-09-01T00:00:00Z",
                "players": [{"player_id": "A"}, {"player_id": "B"}],
            },
        }
    ]
    _write_jsonl(tmp_path / "snapshots/users.jsonl", users)
    _write_jsonl(tmp_path / "events/hands.jsonl", hands)
    artifacts = {
        "snapshots/users.jsonl": _hash(tmp_path / "snapshots/users.jsonl"),
        "events/hands.jsonl": _hash(tmp_path / "events/hands.jsonl"),
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "product_type": "alert_acceptance",
                "training_allowed": False,
                "dataset_id": "acceptance-v1",
                "counts": {"players": 2},
                "artifacts": artifacts,
            }
        )
    )

    frame, loaded_hands, metadata = build_acceptance_seed_data(tmp_path)

    assert len(loaded_hands) == 1
    assert set(frame["user_id"]) == {"A", "B"}
    assert set(frame["dataset_id"]) == {"acceptance-v1"}
    assert set(frame["tenant_id"]) == {"demo"}
    assert metadata["players"] == 2
    assert metadata["hands"] == 1


def test_acceptance_seed_rejects_mutated_artifact(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "product_type": "alert_acceptance",
                "training_allowed": False,
                "dataset_id": "acceptance-v1",
                "counts": {"players": 0},
                "artifacts": {"snapshots/users.jsonl": "0" * 64},
            }
        )
    )
    (tmp_path / "snapshots").mkdir()
    (tmp_path / "snapshots/users.jsonl").write_text("{}\n")

    with pytest.raises(ValueError, match="artifact hash mismatch"):
        build_acceptance_seed_data(tmp_path)
