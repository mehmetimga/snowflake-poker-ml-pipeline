"""Consume pair-feature snapshots into DuckDB or Snowflake."""

from __future__ import annotations

import argparse

from pipeline.kafka.pair_feature_sink import PairFeatureWarehouseSink
from pipeline.warehouse.factory import get_warehouse
from pipeline.warehouse.migrate import run_migrations


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--group-id", default="poker-pair-feature-warehouse-sink-v1")
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--migrate", action="store_true")
    args = parser.parse_args()

    warehouse = get_warehouse()
    if args.migrate:
        run_migrations(warehouse)
    sink = PairFeatureWarehouseSink(
        warehouse=warehouse,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        batch_size=args.batch_size,
        manual_assign_from_beginning=args.from_beginning,
    )
    try:
        result = sink.run(max_messages=args.max_messages)
    finally:
        sink.close()
        warehouse.close()
    print(
        f"[pair-feature-ingest] events={result.events} "
        f"hands={result.hands} pairs={result.pairs}"
    )


if __name__ == "__main__":
    main()
