from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from scripts.seed_snowflake_user_context import (
    CURRENT_VIEW_DDL,
    DDL,
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
        player["player_id"]
        for hand in hands
        for player in hand["payload"]["players"]
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

        def fetch_df(
            self, sql: str, params: tuple | None = None
        ) -> pd.DataFrame:
            self.statements.append((sql, params))
            return pd.DataFrame([{"row_count": 2}])

    warehouse = Warehouse()
    frame = pd.DataFrame(
        [
            {"dataset_id": "seed-v1", "user_id": "A"},
            {"dataset_id": "seed-v1", "user_id": "B"},
        ]
    )

    assert seed_context_table(
        warehouse, frame, dataset_id="seed-v1"
    ) == 2
    sql = "\n".join(statement for statement, _ in warehouse.statements)
    assert DDL in sql
    assert CURRENT_VIEW_DDL in sql
    assert "DELETE FROM POKER_USER_CONTEXT_HISTORY" in sql
    assert warehouse.writes[0][1] == "POKER_USER_CONTEXT_HISTORY"
