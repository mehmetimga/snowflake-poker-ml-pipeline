"""Restore an exact legacy hand range from a verified Snowflake Time Travel snapshot."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from pipeline.warehouse import get_warehouse
from pipeline.warehouse.sql import sql_string_list


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True, help="ISO timestamp before the accidental write")
    parser.add_argument("--hand-prefix", default="TRAIN-H-")
    parser.add_argument("--count", type=int, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if args.count < 1 or args.count > 10_000:
        parser.error("--count must be in [1, 10000]")
    snapshot = datetime.fromisoformat(args.snapshot).astimezone(timezone.utc)
    snapshot_literal = "'" + snapshot.isoformat().replace("'", "''") + "'::TIMESTAMP_TZ"
    hand_ids = [f"{args.hand_prefix}{index:08d}" for index in range(args.count)]
    ids = sql_string_list(hand_ids)
    tables = ("RAW_HANDS", "RAW_ACTIONS", "RAW_PLAYERS")

    warehouse = get_warehouse()
    if warehouse.kind != "snowflake":
        warehouse.close()
        raise RuntimeError("legacy Time Travel restoration requires Snowflake")
    try:
        snapshot_counts = {
            table: int(
                warehouse.fetch_df(
                    f"SELECT COUNT(*) AS n FROM {table} "
                    f"AT (TIMESTAMP => {snapshot_literal}) WHERE hand_id IN ({ids})"
                ).iloc[0]["n"]
            )
            for table in tables
        }
        print(f"[restore] snapshot={snapshot.isoformat()} counts={snapshot_counts}")
        if not args.apply:
            print("[restore] dry run only; pass --apply to restore these exact IDs")
            return
        if snapshot_counts["RAW_HANDS"] != args.count:
            raise RuntimeError(
                f"snapshot has {snapshot_counts['RAW_HANDS']} hands, expected {args.count}"
            )

        warehouse.execute("BEGIN")
        try:
            for table in ("RAW_ACTIONS", "RAW_PLAYERS", "RAW_HANDS"):
                warehouse.execute(f"DELETE FROM {table} WHERE hand_id IN ({ids})")
            for table in tables:
                warehouse.execute(
                    f"INSERT INTO {table} SELECT * FROM {table} "
                    f"AT (TIMESTAMP => {snapshot_literal}) WHERE hand_id IN ({ids})"
                )
            warehouse.execute("COMMIT")
        except Exception:
            warehouse.execute("ROLLBACK")
            raise

        restored_counts = {
            table: int(
                warehouse.fetch_df(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE hand_id IN ({ids})"
                ).iloc[0]["n"]
            )
            for table in tables
        }
        if restored_counts != snapshot_counts:
            raise RuntimeError(
                f"restore verification failed: restored={restored_counts} "
                f"snapshot={snapshot_counts}"
            )
        print(f"[restore] verified={restored_counts}")
    finally:
        warehouse.close()


if __name__ == "__main__":
    main()
