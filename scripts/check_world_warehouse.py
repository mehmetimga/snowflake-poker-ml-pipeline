"""Report canonical-event table counts and basic temporal invariants."""

from __future__ import annotations

import json

from pipeline.warehouse import get_warehouse


def main() -> None:
    warehouse = get_warehouse()
    tables = (
        "RAW_EVENT_ENVELOPES",
        "RAW_HANDS",
        "RAW_ACTIONS",
        "RAW_PLAYERS",
        "USER_CONTEXT_EVENTS",
        "USER_SESSION_EVENTS",
        "ACCOUNT_LINK_EVENTS",
        "USER_CONTEXT_HISTORY",
        "USER_CONTEXT_CURRENT",
    )
    try:
        counts = {
            table: int(
                warehouse.fetch_df(f"SELECT COUNT(*) AS n FROM {table}").iloc[0]["n"]
            )
            for table in tables
        }
        event_types = warehouse.fetch_df(
            "SELECT event_type, COUNT(*) AS n FROM RAW_EVENT_ENVELOPES "
            "GROUP BY event_type ORDER BY event_type"
        )
        invalid_intervals = int(
            warehouse.fetch_df(
                "SELECT COUNT(*) AS n FROM USER_CONTEXT_HISTORY "
                "WHERE effective_to IS NOT NULL AND effective_to <= effective_from"
            ).iloc[0]["n"]
        )
    finally:
        warehouse.close()
    print(
        json.dumps(
            {
                "counts": counts,
                "event_types": event_types.to_dict("records"),
                "invalid_context_intervals": invalid_intervals,
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )


if __name__ == "__main__":
    main()
