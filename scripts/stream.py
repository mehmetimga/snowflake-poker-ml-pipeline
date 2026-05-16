"""Consume from Kafka and write hands to the warehouse."""

from __future__ import annotations

import argparse

from pipeline.kafka.consumer import WarehouseSink


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=200)
    args = parser.parse_args()

    sink = WarehouseSink(batch_size=args.batch_size)
    total = sink.run(max_messages=args.max_messages)
    sink.close()
    print(f"[stream] total ingested {total} hands")


if __name__ == "__main__":
    main()
