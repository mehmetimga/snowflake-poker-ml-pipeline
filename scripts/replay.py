"""Replay a frozen, label-free hand stream to local or managed Kafka."""

from __future__ import annotations

import argparse
import time
from pathlib import Path

from pipeline.generator.dataset import iter_jsonl
from pipeline.kafka.producer import HandProducer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--rate", type=float, default=0.0, help="Hands/second; 0 publishes as fast as possible")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--topic", default=None)
    args = parser.parse_args()
    if args.rate < 0:
        parser.error("--rate cannot be negative")

    producer = HandProducer(topic=args.topic)
    count = 0
    started = time.monotonic()
    try:
        for hand in iter_jsonl(args.events):
            if args.limit is not None and count >= args.limit:
                break
            producer.publish(hand)
            count += 1
            if args.rate:
                deadline = started + count / args.rate
                delay = deadline - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
    finally:
        producer.close()
    elapsed = max(time.monotonic() - started, 1e-9)
    print(f"[replay] published {count} hands in {elapsed:.2f}s ({count / elapsed:.1f}/s)")


if __name__ == "__main__":
    main()
