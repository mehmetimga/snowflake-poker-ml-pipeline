"""Destructively reset only the four managed synthetic world topics."""

from __future__ import annotations

import argparse

from pipeline.kafka.event_producer import WorldTopics
from pipeline.kafka.topics import reset_world_topics


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--wait-seconds", type=float, default=30.0)
    parser.add_argument("--confirm-reset-world-topics", action="store_true")
    args = parser.parse_args()
    if not args.confirm_reset_world_topics:
        parser.error("pass --confirm-reset-world-topics to delete and recreate the four managed topics")
    result = reset_world_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=WorldTopics.from_settings(),
        partitions=args.partitions,
        replication_factor=args.replication_factor,
        wait_seconds=args.wait_seconds,
    )
    print(f"[world-topics-reset] deleted={result['deleted']} created={result['created']}")


if __name__ == "__main__":
    main()
