"""Verify that a frozen world's canonical events can be decoded from Kafka."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from pipeline.config import get_settings
from pipeline.events import event_partition_key, validate_event
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.event_producer import WorldTopics
from pipeline.replay import iter_world_events


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/datasets/context-v1"))
    parser.add_argument(
        "--split",
        action="append",
        choices=("train", "validation", "test", "challenge"),
        dest="splits",
    )
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--timeout-ms", type=int, default=20_000)
    parser.add_argument("--report", type=Path, default=None)
    args = parser.parse_args()
    splits = tuple(args.splits or ("train", "validation", "test", "challenge"))

    expected_events = list(iter_world_events(args.dataset, splits))
    expected = {event.event_id: event for event in expected_events}
    topics = WorldTopics.from_settings().by_event_type()

    from kafka import KafkaConsumer, TopicPartition

    settings = get_settings()
    consumer = KafkaConsumer(
        bootstrap_servers=(
            args.bootstrap_servers or settings.kafka_bootstrap_servers
        ).split(","),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        group_id=None,
        enable_auto_commit=False,
        **kafka_client_kwargs(),
    )
    assignments = []
    for topic in sorted(set(topics.values())):
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"Kafka topic has no readable partitions: {topic}")
        assignments.extend(TopicPartition(topic, partition) for partition in partitions)
    consumer.assign(assignments)
    consumer.seek_to_beginning(*assignments)
    found: set[str] = set()
    counts: dict[str, int] = {}
    deadline = time.monotonic() + args.timeout_ms / 1_000
    try:
        while len(found) < len(expected) and time.monotonic() < deadline:
            remaining_ms = max(1, int((deadline - time.monotonic()) * 1_000))
            batches = consumer.poll(timeout_ms=min(1_000, remaining_ms), max_records=500)
            for messages in batches.values():
                for message in messages:
                    envelope = validate_event(message.value)
                    event_id = str(envelope.event_id)
                    if event_id not in expected:
                        continue
                    expected_event = expected[event_id]
                    expected_topic = topics[envelope.event_type]
                    if message.topic != expected_topic:
                        raise ValueError(
                            f"event {event_id} arrived on {message.topic}, "
                            f"expected {expected_topic}"
                        )
                    expected_key = event_partition_key(envelope)
                    if message.key != expected_key or expected_key != expected_event.partition_key:
                        raise ValueError(f"event {event_id} has incorrect Kafka key")
                    if envelope.model_dump(mode="json") != expected_event.envelope:
                        raise ValueError(f"event {event_id} payload differs from frozen source")
                    if event_id not in found:
                        found.add(event_id)
                        counts[message.topic] = counts.get(message.topic, 0) + 1
    finally:
        consumer.close()

    missing = set(expected) - found
    if missing:
        sample = sorted(missing)[:5]
        raise RuntimeError(
            f"Kafka verification timed out: found={len(found)} expected={len(expected)} "
            f"counts={dict(sorted(counts.items()))} missing_sample={sample}"
        )
    if args.report is not None:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(
            json.dumps(
                {
                    "dataset_id": expected_events[0].envelope["dataset_id"],
                    "splits": list(splits),
                    "verified": len(found),
                    "expected": len(expected),
                    "verified_by_topic": dict(sorted(counts.items())),
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        )
        print(f"[world-verify] report={args.report}")
    print(
        f"[world-verify] verified={len(found)} splits={list(splits)} "
        f"topics={dict(sorted(counts.items()))}"
    )


if __name__ == "__main__":
    main()
