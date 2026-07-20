"""Delete one dataset from canonical audit/context tables, never legacy hand tables."""

from __future__ import annotations

import argparse

from pipeline.warehouse import get_warehouse
from pipeline.warehouse.sql import sql_string_literal


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset-id", required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    dataset = sql_string_literal(args.dataset_id)
    tables = (
        "USER_CONTEXT_HISTORY",
        "USER_CONTEXT_EVENTS",
        "USER_SESSION_EVENTS",
        "ACCOUNT_LINK_EVENTS",
        "RAW_EVENT_ENVELOPES",
    )
    warehouse = get_warehouse()
    try:
        before = {
            table: int(
                warehouse.fetch_df(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE dataset_id = {dataset}"
                ).iloc[0]["n"]
            )
            for table in tables
        }
        print(f"[canonical-cleanup] dataset={args.dataset_id} before={before}")
        if not args.apply:
            print("[canonical-cleanup] dry run only; pass --apply to delete these rows")
            return
        warehouse.execute("BEGIN")
        try:
            for table in tables:
                warehouse.execute(f"DELETE FROM {table} WHERE dataset_id = {dataset}")
            warehouse.execute("COMMIT")
        except Exception:
            warehouse.execute("ROLLBACK")
            raise
        after = {
            table: int(
                warehouse.fetch_df(
                    f"SELECT COUNT(*) AS n FROM {table} WHERE dataset_id = {dataset}"
                ).iloc[0]["n"]
            )
            for table in tables
        }
        if any(after.values()):
            raise RuntimeError(f"canonical cleanup verification failed: {after}")
        print(f"[canonical-cleanup] verified={after}")
    finally:
        warehouse.close()


if __name__ == "__main__":
    main()
