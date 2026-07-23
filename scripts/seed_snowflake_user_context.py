#!/usr/bin/env python3
"""Create and seed the internal Snowflake context projection with PokerKit users."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

import pandas as pd

from pipeline.generator import SyntheticPokerWorld
from pipeline.warehouse.factory import Warehouse, get_warehouse


TABLE = "POKER_ML_DEMO.SPCS.POKER_USER_CONTEXT_HISTORY"
LOCAL_TABLE = "POKER_USER_CONTEXT_HISTORY"
CURRENT_VIEW = "POKER_USER_CONTEXT_CURRENT"
DEFAULT_DATASET_ID = "spcs-snowflake-context-v1"
DEFAULT_HANDS_OUTPUT = Path("data/runs/spcs-snowflake-context-v1/hands.jsonl")

DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
    tenant_id                 STRING       NOT NULL,
    product_id                STRING       NOT NULL,
    user_id                   STRING       NOT NULL,
    context_version           INT          NOT NULL,
    effective_at              TIMESTAMP_TZ NOT NULL,
    account_created_at        TIMESTAMP_TZ NOT NULL,
    country_bucket            STRING       NOT NULL,
    timezone                  STRING       NOT NULL,
    acquisition_channel       STRING       NOT NULL,
    kyc_level                 STRING       NOT NULL,
    account_status            STRING       NOT NULL,
    bankroll_bucket           STRING       NOT NULL,
    preferred_stake_bucket    STRING       NOT NULL,
    skill_rating              FLOAT        NOT NULL,
    device_id                 STRING       NOT NULL,
    network_cluster_id        STRING       NOT NULL,
    dataset_id                STRING       NOT NULL,
    loaded_at                 TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (tenant_id, product_id, user_id, context_version)
)
""".strip()

CURRENT_VIEW_DDL = f"""
CREATE OR REPLACE VIEW POKER_ML_DEMO.SPCS.{CURRENT_VIEW} AS
SELECT
    tenant_id, product_id, user_id, context_version, effective_at,
    account_created_at, country_bucket, timezone, acquisition_channel,
    kyc_level, account_status, bankroll_bucket, preferred_stake_bucket,
    skill_rating, device_id, network_cluster_id, dataset_id, loaded_at
FROM {TABLE}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY tenant_id, product_id, user_id
    ORDER BY effective_at DESC, context_version DESC
) = 1
""".strip()


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def build_seed_data(
    *,
    dataset_id: str,
    tenant_id: str,
    product_id: str,
    players: int,
    hands: int,
    tables: int,
    pairs: int,
    seed: int,
    hand_start_at: datetime,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    world = SyntheticPokerWorld(
        dataset_id=dataset_id,
        split="realtime",
        hand_count=hands,
        n_players=players,
        n_tables=tables,
        n_colluding_pairs=pairs,
        seed=seed,
        tenant_id=tenant_id,
        product_id=product_id,
        hand_start_at=hand_start_at,
        context_start_at=hand_start_at - timedelta(days=1),
    )
    rows: list[dict[str, object]] = []
    for event in world.context_events():
        payload = dict(event["payload"])
        payload.update(
            tenant_id=event["tenant_id"],
            product_id=event["product_id"],
            dataset_id=event["dataset_id"],
        )
        rows.append(payload)
    frame = pd.DataFrame(rows)
    frame["effective_at"] = pd.to_datetime(frame["effective_at"], utc=True)
    frame["account_created_at"] = pd.to_datetime(
        frame["account_created_at"], utc=True
    )
    hand_events = [
        hand for hand, _player_labels, _pair_labels in world.iter_hands_with_labels()
    ]
    return frame, hand_events


def seed_context_table(
    warehouse: Warehouse,
    frame: pd.DataFrame,
    *,
    dataset_id: str,
) -> int:
    if warehouse.kind != "snowflake":
        raise RuntimeError("Snowflake context seed requires WAREHOUSE_BACKEND=snowflake")
    warehouse.execute("USE ROLE SYSADMIN")
    warehouse.execute("USE DATABASE POKER_ML_DEMO")
    warehouse.execute("USE SCHEMA SPCS")
    warehouse.execute(DDL)
    warehouse.execute(CURRENT_VIEW_DDL)
    warehouse.execute(
        "DELETE FROM POKER_USER_CONTEXT_HISTORY WHERE dataset_id = %s",
        (dataset_id,),
    )
    warehouse.write_pandas(frame, LOCAL_TABLE)
    result = warehouse.fetch_df(
        "SELECT COUNT(*) AS row_count "
        "FROM POKER_USER_CONTEXT_HISTORY WHERE dataset_id = %s",
        (dataset_id,),
    )
    return int(result.iloc[0]["row_count"])


def write_hands(path: Path, hand_events: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as stream:
        for event in hand_events:
            stream.write(
                json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
            )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", default=DEFAULT_DATASET_ID)
    parser.add_argument("--tenant-id", default="demo")
    parser.add_argument("--product-id", default="poker")
    parser.add_argument("--players", type=int, default=60)
    parser.add_argument("--hands", type=int, default=20)
    parser.add_argument("--tables", type=int, default=6)
    parser.add_argument("--pairs", type=int, default=10)
    parser.add_argument("--seed", type=int, default=20_260_723)
    parser.add_argument(
        "--hand-start-at",
        type=_timestamp,
        default=datetime(2026, 7, 23, 12, 0, tzinfo=timezone.utc),
    )
    parser.add_argument(
        "--hands-output", type=Path, default=DEFAULT_HANDS_OUTPUT
    )
    args = parser.parse_args()

    frame, hand_events = build_seed_data(
        dataset_id=args.dataset_id,
        tenant_id=args.tenant_id,
        product_id=args.product_id,
        players=args.players,
        hands=args.hands,
        tables=args.tables,
        pairs=args.pairs,
        seed=args.seed,
        hand_start_at=args.hand_start_at,
    )
    warehouse = get_warehouse()
    try:
        row_count = seed_context_table(
            warehouse, frame, dataset_id=args.dataset_id
        )
    finally:
        warehouse.close()
    write_hands(args.hands_output, hand_events)
    print(
        json.dumps(
            {
                "status": "seeded",
                "table": TABLE,
                "current_view": (
                    f"POKER_ML_DEMO.SPCS.{CURRENT_VIEW}"
                ),
                "dataset_id": args.dataset_id,
                "context_rows": row_count,
                "generated_hands": len(hand_events),
                "hands_file": str(args.hands_output),
                "tenant_id": args.tenant_id,
                "product_id": args.product_id,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
