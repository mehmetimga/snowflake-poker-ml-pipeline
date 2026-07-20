"""Create missing canonical Kafka topics with their planned cleanup policies."""

from __future__ import annotations

import argparse

from pipeline.kafka.event_producer import WorldTopics
from pipeline.kafka.topics import ensure_world_topics, world_topic_specs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    topics = WorldTopics.from_settings()

    if args.dry_run:
        for spec in world_topic_specs(
            topics,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        ):
            print(
                f"[world-topics] name={spec.name} partitions={spec.partitions} "
                f"replication={spec.replication_factor} configs={spec.configs}"
            )
        return

    result = ensure_world_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(
        f"[world-topics] created={result['created']} existing={result['existing']}"
    )


if __name__ == "__main__":
    main()
