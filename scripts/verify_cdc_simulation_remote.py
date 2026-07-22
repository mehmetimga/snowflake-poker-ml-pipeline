#!/usr/bin/env python3
"""Verify one offset-bounded Confluent -> SPCS simulation replay."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.events import HAND_COMPLETED, validate_event
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import CdcSimulationTopics


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    topics = CdcSimulationTopics()
    expected_topics = {
        "source": topics.source,
        "canonical": topics.canonical,
        "dead_letters": topics.dead_letters,
    }
    if manifest.get("schema_version") != 1 or manifest.get("topics") != expected_topics:
        raise ValueError("remote simulation manifest contract is invalid")
    records = manifest.get("records")
    if not isinstance(records, list) or len(records) != 6:
        raise ValueError("remote simulation manifest must contain six scenarios")
    names = {record.get("scenario") for record in records}
    if len(names) != 6:
        raise ValueError("remote simulation manifest scenarios must be unique")
    published = [record for record in records if record["published_to_managed_kafka"]]
    if len(published) != 5:
        raise ValueError("remote simulation manifest must publish exactly five inputs")
    return manifest


def wait_for_commit(
    manifest: dict[str, Any],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> dict[int, int]:
    from kafka import KafkaAdminClient, TopicPartition

    topic = manifest["topics"]["source"]
    expected: dict[int, int] = {}
    for record in manifest["records"]:
        if record["published_to_managed_kafka"]:
            partition = int(record["remote_partition"])
            expected[partition] = max(
                expected.get(partition, 0), int(record["remote_offset"]) + 1
            )
    admin = KafkaAdminClient(
        client_id="poker-cdc-remote-simulation-verifier-v1", **client_kwargs
    )
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            offsets = admin.list_consumer_group_offsets(manifest["adapter_group_id"])
            committed = {
                item.partition: metadata.offset
                for item, metadata in offsets.items()
                if item.topic == topic and metadata is not None
            }
            if all(committed.get(partition, -1) >= offset for partition, offset in expected.items()):
                return committed
            time.sleep(0.5)
    finally:
        admin.close()
    raise TimeoutError(
        f"adapter group {manifest['adapter_group_id']} did not commit {expected}"
    )


def read_bounded_topic(
    topic: str,
    starts: dict[str, int],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> list[Any]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(group_id=None, enable_auto_commit=False, **client_kwargs)
    records: list[Any] = []
    try:
        assignments = [
            TopicPartition(topic, int(partition)) for partition in sorted(starts, key=int)
        ]
        consumer.assign(assignments)
        endings = consumer.end_offsets(assignments)
        for item in assignments:
            start = int(starts[str(item.partition)])
            if start > endings[item]:
                raise ValueError(f"invalid start offset for {topic}[{item.partition}]")
            consumer.seek(item, start)
        expected = sum(endings[item] - consumer.position(item) for item in assignments)
        deadline = time.monotonic() + timeout_seconds
        while len(records) < expected and time.monotonic() < deadline:
            for messages in consumer.poll(timeout_ms=500, max_records=500).values():
                records.extend(messages)
        if len(records) != expected:
            raise TimeoutError(f"read {len(records)} of {expected} records from {topic}")
        return records
    finally:
        consumer.close()


def read_source_values(
    manifest: dict[str, Any], *, client_kwargs: dict[str, Any], timeout_seconds: float
) -> dict[tuple[int, int], bytes]:
    starts: dict[str, int] = {}
    wanted: set[tuple[int, int]] = set()
    for record in manifest["records"]:
        if not record["published_to_managed_kafka"]:
            continue
        partition = int(record["remote_partition"])
        offset = int(record["remote_offset"])
        wanted.add((partition, offset))
        starts[str(partition)] = min(starts.get(str(partition), offset), offset)
    values = {}
    for message in read_bounded_topic(
        manifest["topics"]["source"],
        starts,
        client_kwargs=client_kwargs,
        timeout_seconds=timeout_seconds,
    ):
        position = (message.partition, message.offset)
        if position in wanted:
            values[position] = message.value
    if set(values) != wanted:
        raise ValueError("managed source records are no longer readable")
    return values


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    settings = get_settings()
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    committed = wait_for_commit(
        manifest,
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    source_values = read_source_values(
        manifest,
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    records_by_hand = {record["hand_id"]: record for record in manifest["records"]}
    records_by_position = {
        (int(record["remote_partition"]), int(record["remote_offset"])): record
        for record in manifest["records"]
        if record["published_to_managed_kafka"]
    }

    canonical_by_scenario: dict[str, list[Any]] = defaultdict(list)
    canonical_records = read_bounded_topic(
        manifest["topics"]["canonical"],
        manifest["output_start_offsets"][manifest["topics"]["canonical"]],
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    for message in canonical_records:
        event = validate_event(json.loads(message.value))
        record = records_by_hand.get(event.payload["hand_id"])
        if record is None:
            continue
        if record["expected_outcome"] != "canonical":
            raise ValueError(f"non-canonical scenario reached output: {record['scenario']}")
        if event.event_type != HAND_COMPLETED or event.dataset_id != manifest["adapter_dataset_id"]:
            raise ValueError("canonical event contract changed")
        headers = {key: value.decode() for key, value in message.headers}
        source_position = (
            int(headers["cdc_source_partition"]),
            int(headers["cdc_source_offset"]),
        )
        expected_position = (
            int(record["remote_partition"]),
            int(record["remote_offset"]),
        )
        if source_position != expected_position:
            raise ValueError("canonical lineage does not match managed source offset")
        canonical_by_scenario[record["scenario"]].append(message)

    dlq_by_scenario: dict[str, list[Any]] = defaultdict(list)
    errors: Counter[str] = Counter()
    dlq_records = read_bounded_topic(
        manifest["topics"]["dead_letters"],
        manifest["output_start_offsets"][manifest["topics"]["dead_letters"]],
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    for message in dlq_records:
        document = json.loads(message.value)
        payload = document.get("payload", {})
        position = (
            int(payload.get("source_partition", -1)),
            int(payload.get("source_offset", -1)),
        )
        record = records_by_position.get(position)
        if record is None:
            continue
        if record["expected_outcome"] != "dead_letter":
            raise ValueError(f"non-poison scenario reached DLQ: {record['scenario']}")
        error_code = payload.get("error_code")
        if error_code != record["expected_error_code"]:
            raise ValueError(f"wrong DLQ error for {record['scenario']}: {error_code}")
        if payload.get("source_value_sha256") != record["source_value_sha256"]:
            raise ValueError("DLQ source digest does not match managed input")
        if payload.get("service_build_version") != manifest["adapter_build_version"]:
            raise ValueError("DLQ service build does not match released image")
        raw_source = source_values[position]
        if raw_source in message.value or record["hand_id"].encode() in message.value:
            raise ValueError(f"raw source identity leaked to DLQ: {record['scenario']}")
        dlq_by_scenario[record["scenario"]].append(message)
        errors[error_code] += 1

    expected_canonical = {
        record["scenario"]
        for record in manifest["records"]
        if record["expected_outcome"] == "canonical"
    }
    expected_dlq = {
        record["scenario"]
        for record in manifest["records"]
        if record["expected_outcome"] == "dead_letter"
    }
    if set(canonical_by_scenario) != expected_canonical or any(
        len(messages) != 1 for messages in canonical_by_scenario.values()
    ):
        raise ValueError("canonical outputs do not exactly match this replay")
    if set(dlq_by_scenario) != expected_dlq or any(
        len(messages) != 1 for messages in dlq_by_scenario.values()
    ):
        raise ValueError("dead-letter outputs do not exactly match this replay")

    print(
        json.dumps(
            {
                "status": "passed",
                "source_dataset_id": manifest["source_dataset_id"],
                "adapter_build_version": manifest["adapter_build_version"],
                "managed_inputs": len(records_by_position),
                "filtered_before_managed_kafka": 1,
                "canonical_records": sum(map(len, canonical_by_scenario.values())),
                "dead_letter_records": sum(map(len, dlq_by_scenario.values())),
                "error_codes": dict(sorted(errors.items())),
                "committed_offsets": committed,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
