"""Create Flink context, pair-feature, and dead-letter topics."""

from __future__ import annotations

import argparse

from pipeline.kafka.topics import (
    EnrichmentTopics,
    enrichment_topic_specs,
    ensure_enrichment_topics,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--partitions", type=int, default=6)
    parser.add_argument("--replication-factor", type=int, default=3)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    topics = EnrichmentTopics.from_settings()

    if args.dry_run:
        for spec in enrichment_topic_specs(
            topics,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
        ):
            print(
                f"[enrichment-topics] name={spec.name} partitions={spec.partitions} "
                f"replication={spec.replication_factor} configs={spec.configs}"
            )
        return

    result = ensure_enrichment_topics(
        bootstrap_servers=args.bootstrap_servers,
        topics=topics,
        partitions=args.partitions,
        replication_factor=args.replication_factor,
    )
    print(
        f"[enrichment-topics] created={result['created']} "
        f"existing={result['existing']}"
    )


if __name__ == "__main__":
    main()
