#!/usr/bin/env python3
"""Create the three isolated Confluent topics for the C2 SPCS adapter."""

from __future__ import annotations

import argparse

from pipeline.kafka.topics import (
    CdcSimulationTopics,
    cdc_simulation_topic_specs,
    ensure_cdc_simulation_topics,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--output-partitions", type=int, default=3)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    topics = CdcSimulationTopics()

    if args.dry_run:
        for spec in cdc_simulation_topic_specs(
            topics,
            output_partitions=args.output_partitions,
            replication_factor=args.replication_factor,
        ):
            print(
                f"[cdc-sim-topics] name={spec.name} "
                f"partitions={spec.partitions} "
                f"replication={spec.replication_factor} configs={spec.configs}"
            )
        return

    result = ensure_cdc_simulation_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        output_partitions=args.output_partitions,
        replication_factor=args.replication_factor,
    )
    print(
        f"[cdc-sim-topics] created={result['created']} "
        f"existing={result['existing']}"
    )


if __name__ == "__main__":
    main()
