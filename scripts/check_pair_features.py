"""Validate Kafka pair features and optionally prove offline/online parity."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from math import comb

from pipeline.config import get_settings
from pipeline.events import PairFeatureEvent, PlayerHandContextEvent
from pipeline.features import PairFeatureCore
from pipeline.kafka.config import kafka_client_kwargs


def _value_differences(left: object, right: object, path: str = "payload") -> list[str]:
    if isinstance(left, dict) and isinstance(right, dict):
        output: list[str] = []
        for key in sorted(set(left) | set(right)):
            output.extend(
                _value_differences(left.get(key), right.get(key), f"{path}.{key}")
            )
        return output
    if isinstance(left, list) and isinstance(right, list):
        output = []
        for index, (left_value, right_value) in enumerate(zip(left, right)):
            output.extend(
                _value_differences(left_value, right_value, f"{path}[{index}]")
            )
        if len(left) != len(right):
            output.append(f"{path}.length expected={len(left)!r} actual={len(right)!r}")
        return output
    if left != right:
        return [f"{path} expected={left!r} actual={right!r}"]
    return []


def _read_topic(
    topic: str,
    *,
    bootstrap_servers: str,
    timeout_ms: int,
    settle_ms: int,
    dataset_id: str | None,
) -> list[tuple[bytes | None, dict]]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
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
    records: list[tuple[bytes | None, dict]] = []
    deadline = time.monotonic() + timeout_ms / 1_000
    last_received: float | None = None
    try:
        while time.monotonic() < deadline:
            batches = consumer.poll(timeout_ms=500, max_records=1_000)
            received = False
            for messages in batches.values():
                for message in messages:
                    if dataset_id and message.value.get("dataset_id") != dataset_id:
                        continue
                    records.append((message.key, message.value))
                    received = True
            if received:
                last_received = time.monotonic()
            if (
                records
                and last_received is not None
                and (time.monotonic() - last_received) * 1_000 >= settle_ms
            ):
                break
    finally:
        consumer.close()
    return records


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--input-topic", default=None)
    parser.add_argument("--dataset-id", default=None)
    parser.add_argument("--minimum-records", type=int, default=1)
    parser.add_argument("--timeout-ms", type=int, default=15_000)
    parser.add_argument("--settle-ms", type=int, default=1_000)
    args = parser.parse_args()
    if args.minimum_records < 1 or args.timeout_ms < 1 or args.settle_ms < 1:
        parser.error("minimum-records, timeout-ms, and settle-ms must be positive")

    settings = get_settings()
    bootstrap = args.bootstrap_servers or settings.kafka_bootstrap_servers
    topic = args.topic or settings.kafka_pair_features_topic
    raw_output = _read_topic(
        topic,
        bootstrap_servers=bootstrap,
        timeout_ms=args.timeout_ms,
        settle_ms=args.settle_ms,
        dataset_id=args.dataset_id,
    )
    output: dict[str, PairFeatureEvent] = {}
    for key, raw in raw_output:
        event = PairFeatureEvent.model_validate(raw)
        if key != event.payload.pair_key.encode("utf-8"):
            raise ValueError(f"incorrect pair key for event {event.event_id}")
        previous = output.get(str(event.event_id))
        if previous is not None and previous != event:
            raise ValueError(f"event_id collision in Kafka: {event.event_id}")
        output[str(event.event_id)] = event
    if len(output) < args.minimum_records:
        raise RuntimeError(
            f"found {len(output)} records; expected at least {args.minimum_records} on {topic}"
        )

    latest: dict[tuple[str, str], PairFeatureEvent] = {}
    for event in output.values():
        identity = (event.payload.hand_id, event.payload.pair_key)
        previous = latest.get(identity)
        if previous is None or event.payload.snapshot_revision > previous.payload.snapshot_revision:
            latest[identity] = event
    by_hand = Counter(hand_id for hand_id, _ in latest)
    incomplete_hands: list[str] = []
    for hand_id, count in by_hand.items():
        sample = next(event for (candidate, _), event in latest.items() if candidate == hand_id)
        expected = comb(sample.payload.num_players, 2)
        if count != expected:
            incomplete_hands.append(
                f"{hand_id}:{count}/{expected}"
            )

    parity = "not-requested"
    if args.input_topic:
        raw_input = _read_topic(
            args.input_topic,
            bootstrap_servers=bootstrap,
            timeout_ms=args.timeout_ms,
            settle_ms=args.settle_ms,
            dataset_id=args.dataset_id,
        )
        enriched: dict[str, PlayerHandContextEvent] = {}
        for key, raw in raw_input:
            event = PlayerHandContextEvent.model_validate(raw)
            if key != event.payload.player.player_id.encode("utf-8"):
                raise ValueError(f"incorrect player key for source event {event.event_id}")
            enriched[str(event.event_id)] = event
        ordered = sorted(
            enriched.values(),
            key=lambda event: (
                event.payload.played_at,
                event.payload.revision,
                event.payload.player.player_id,
                event.emitted_at,
                str(event.event_id),
            ),
        )
        expected = {
            str(event.event_id): event
            for event in PairFeatureCore().process_many(ordered)
        }
        missing = sorted(set(expected) - set(output))
        unexpected = sorted(set(output) - set(expected))
        mismatched = sorted(
            event_id
            for event_id in set(expected) & set(output)
            if expected[event_id].payload != output[event_id].payload
        )
        if missing or unexpected or mismatched or incomplete_hands:
            details: list[str] = []
            if mismatched:
                first = mismatched[0]
                details = _value_differences(
                    expected[first].payload.model_dump(mode="json"),
                    output[first].payload.model_dump(mode="json"),
                )[:8]
            raise ValueError(
                "online/offline parity failed: "
                f"missing={missing[:5]} unexpected={unexpected[:5]} "
                f"mismatched={mismatched[:5]} incomplete={incomplete_hands[:5]} "
                f"details={details}"
            )
        parity = "passed"

    if incomplete_hands:
        raise ValueError(f"incomplete pair sets: {incomplete_hands[:5]}")
    revisions = Counter(event.payload.snapshot_revision for event in output.values())
    print(
        f"[pair-feature-check] valid_records={len(output)} "
        f"latest_pairs={len(latest)} hands={len(by_hand)} "
        f"revisions={dict(sorted(revisions.items()))} parity={parity}"
    )


if __name__ == "__main__":
    main()
