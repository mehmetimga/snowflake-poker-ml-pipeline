#!/usr/bin/env python3
"""Create the isolated managed-Kafka topics for Flink/risk shadow replay."""

from __future__ import annotations

import argparse

from pipeline.kafka.topics import (
    ShadowSimulationTopics,
    ensure_shadow_simulation_topics,
    shadow_simulation_topic_specs,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    topics = ShadowSimulationTopics()

    if args.dry_run:
        for spec in shadow_simulation_topic_specs(
            topics,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        ):
            print(
                f"[shadow-sim-topics] name={spec.name} "
                f"partitions={spec.partitions} "
                f"replication={spec.replication_factor} configs={spec.configs}"
            )
        return

    result = ensure_shadow_simulation_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(
        f"[shadow-sim-topics] created={result['created']} "
        f"existing={result['existing']}"
    )


if __name__ == "__main__":
    main()
