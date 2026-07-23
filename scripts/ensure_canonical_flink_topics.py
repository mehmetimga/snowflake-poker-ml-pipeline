#!/usr/bin/env python3
"""Create the hands-only canonical synthetic topics used by POKER_FLINK."""

from __future__ import annotations

import argparse

from pipeline.kafka.topics import (
    CanonicalFlinkTopics,
    canonical_flink_topic_specs,
    ensure_canonical_flink_topics,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    topics = CanonicalFlinkTopics()

    if args.dry_run:
        for spec in canonical_flink_topic_specs(
            topics,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        ):
            print(
                f"[canonical-flink-topics] name={spec.name} "
                f"partitions={spec.partitions} "
                f"replication={spec.replication_factor} "
                f"configs={spec.configs}"
            )
        return

    result = ensure_canonical_flink_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(
        f"[canonical-flink-topics] created={result['created']} "
        f"existing={result['existing']}"
    )


if __name__ == "__main__":
    main()
