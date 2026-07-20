"""Consume canonical poker world topics into Snowflake or DuckDB."""

from __future__ import annotations

import argparse

from pipeline.kafka.event_producer import WorldTopics
from pipeline.kafka.world_sink import WorldWarehouseSink
from pipeline.warehouse import get_warehouse
from pipeline.warehouse.migrate import run_migrations


def _topics(args: argparse.Namespace) -> WorldTopics:
    configured = WorldTopics.from_settings()
    return WorldTopics(
        hands=args.hands_topic or configured.hands,
        user_context=args.user_context_topic or configured.user_context,
        sessions=args.session_context_topic or configured.sessions,
        account_links=args.account_links_topic or configured.account_links,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--group-id", default="poker-world-warehouse-sink-v1")
    parser.add_argument("--batch-size", type=int, default=200)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--consumer-timeout-ms", type=int, default=-1)
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument(
        "--assign-from-beginning",
        action="store_true",
        help="Read every partition without committing a consumer-group offset; intended for bounded audits.",
    )
    parser.add_argument("--migrate", action="store_true")
    parser.add_argument("--hands-topic", default=None)
    parser.add_argument("--user-context-topic", default=None)
    parser.add_argument("--session-context-topic", default=None)
    parser.add_argument("--account-links-topic", default=None)
    args = parser.parse_args()

    warehouse = get_warehouse()
    if args.migrate:
        run_migrations(warehouse)
    sink = WorldWarehouseSink(
        warehouse=warehouse,
        bootstrap_servers=args.bootstrap_servers,
        topics=_topics(args),
        batch_size=args.batch_size,
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
        consumer_timeout_ms=args.consumer_timeout_ms,
        manual_assign_from_beginning=args.assign_from_beginning,
    )
    try:
        result = sink.run(max_messages=args.max_messages)
    finally:
        sink.close()
        warehouse.close()
    print(
        "[world-ingest] "
        f"events={result.events} hands={result.hands} contexts={result.contexts} "
        f"sessions={result.sessions} links={result.account_links} "
        f"context_users={result.affected_context_users}"
    )


if __name__ == "__main__":
    main()
