"""Validate player-context enrichment records already present in Kafka."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter

from pipeline.config import get_settings
from pipeline.events import PlayerHandContextEvent
from pipeline.kafka.config import kafka_client_kwargs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--timeout-ms", type=int, default=10_000)
    parser.add_argument("--minimum-records", type=int, default=1)
    parser.add_argument("--settle-ms", type=int, default=1_000)
    parser.add_argument("--dataset-id", default=None)
    args = parser.parse_args()
    if args.timeout_ms <= 0 or args.minimum_records <= 0 or args.settle_ms <= 0:
        parser.error("timeout-ms, minimum-records, and settle-ms must be positive")

    from kafka import KafkaConsumer, TopicPartition

    settings = get_settings()
    topic = args.topic or settings.kafka_player_context_topic
    consumer = KafkaConsumer(
        bootstrap_servers=(
            args.bootstrap_servers or settings.kafka_bootstrap_servers
        ).split(","),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        group_id=None,
        enable_auto_commit=False,
        **kafka_client_kwargs(),
    )
    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise RuntimeError(f"Kafka topic has no readable partitions: {topic}")
    assignments = [TopicPartition(topic, partition) for partition in partitions]
    consumer.assign(assignments)
    consumer.seek_to_beginning(*assignments)

    records: dict[str, PlayerHandContextEvent] = {}
    statuses: Counter[str] = Counter()
    deadline = time.monotonic() + args.timeout_ms / 1_000
    last_received_at: float | None = None
    try:
        while time.monotonic() < deadline:
            batches = consumer.poll(timeout_ms=500, max_records=500)
            received = False
            for messages in batches.values():
                for message in messages:
                    event = PlayerHandContextEvent.model_validate(message.value)
                    if args.dataset_id and event.dataset_id != args.dataset_id:
                        continue
                    expected_key = event.payload.player.player_id.encode("utf-8")
                    if message.key != expected_key:
                        raise ValueError(f"incorrect player key for event {event.event_id}")
                    if (
                        event.payload.context_effective_at is not None
                        and event.payload.context_effective_at > event.payload.played_at
                    ):
                        raise ValueError(f"future context leaked into event {event.event_id}")
                    event_id = str(event.event_id)
                    if event_id not in records:
                        records[event_id] = event
                        statuses[event.payload.context_status] += 1
                        received = True
            if received:
                last_received_at = time.monotonic()
            if (
                len(records) >= args.minimum_records
                and last_received_at is not None
                and (time.monotonic() - last_received_at) * 1_000 >= args.settle_ms
            ):
                break
    finally:
        consumer.close()

    if len(records) < args.minimum_records:
        raise RuntimeError(
            f"timed out with {len(records)} records; expected at least "
            f"{args.minimum_records} on {topic}"
        )
    identities = {
        (event.payload.hand_id, event.payload.player.player_id)
        for event in records.values()
    }
    print(
        f"[context-enrichment-check] valid_records={len(records)} "
        f"player_hands={len(identities)} statuses={dict(sorted(statuses.items()))}"
    )


if __name__ == "__main__":
    main()
