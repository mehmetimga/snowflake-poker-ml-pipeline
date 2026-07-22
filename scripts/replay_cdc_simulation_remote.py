#!/usr/bin/env python3
"""Replay one real local Debezium fault run into isolated managed Kafka topics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import CdcSimulationTopics


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"


def database_scenarios(dsn: str, dataset_id: str) -> dict[str, dict[str, Any]]:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT h.simulation_scenario, h.hand_id, h.game_type,
                       o.id IS NOT NULL AS eligible
                FROM public.hand_history h
                LEFT JOIN public.hand_completed_outbox o ON o.id = h.outbox_id
                WHERE h.simulation_dataset_id = %s
                ORDER BY h.simulation_scenario
                """,
                (dataset_id,),
            )
            rows = cursor.fetchall()
    if len(rows) != 6 or len({row[0] for row in rows}) != len(rows):
        raise ValueError("remote fault replay requires six unique source scenarios")
    return {
        scenario: {
            "hand_id": hand_id,
            "game_type": game_type,
            "eligible": eligible,
        }
        for scenario, hand_id, game_type, eligible in rows
    }


def read_local_cdc_records(
    bootstrap_servers: str,
    scenarios: dict[str, dict[str, Any]],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    topic = CdcSimulationTopics().source
    hand_to_scenario = {
        value["hand_id"]: name
        for name, value in scenarios.items()
        if value["eligible"]
    }
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=None,
        enable_auto_commit=False,
        security_protocol="PLAINTEXT",
    )
    matched: dict[str, Any] = {}
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"local source topic is unavailable: {topic}")
        assignments = [TopicPartition(topic, part) for part in sorted(partitions)]
        consumer.assign(assignments)
        consumer.seek_to_beginning(*assignments)
        deadline = time.monotonic() + timeout_seconds
        while set(matched) != set(hand_to_scenario.values()):
            if time.monotonic() >= deadline:
                missing = sorted(set(hand_to_scenario.values()) - set(matched))
                raise TimeoutError(f"local Debezium records not found: {missing}")
            for messages in consumer.poll(timeout_ms=500, max_records=500).values():
                for message in messages:
                    try:
                        envelope = json.loads(message.value)
                    except (TypeError, json.JSONDecodeError):
                        continue
                    after = envelope.get("after") if isinstance(envelope, dict) else None
                    hand_id = after.get("aggregate_id") if isinstance(after, dict) else None
                    scenario = hand_to_scenario.get(hand_id)
                    if scenario is not None:
                        if scenario in matched:
                            raise ValueError(
                                f"duplicate local Debezium record for {scenario}"
                            )
                        matched[scenario] = message
    finally:
        consumer.close()
    return matched


def topic_end_offsets(topic: str, *, client_kwargs: dict[str, Any]) -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(group_id=None, enable_auto_commit=False, **client_kwargs)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"managed Kafka topic is unavailable: {topic}")
        assignments = [TopicPartition(topic, part) for part in sorted(partitions)]
        return {
            str(item.partition): offset
            for item, offset in consumer.end_offsets(assignments).items()
        }
    finally:
        consumer.close()


def _sha256(value: bytes | None) -> str:
    return hashlib.sha256(value or b"").hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--adapter-dataset-id", default="sim-cdc-v1")
    parser.add_argument(
        "--adapter-group-id", default="poker-go-hand-adapter-sim-v1"
    )
    parser.add_argument("--adapter-build-version", required=True)
    parser.add_argument("--postgres-dsn", default=os.getenv("CDC_SIM_POSTGRES_DSN", DEFAULT_DSN))
    parser.add_argument("--local-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()
    if not args.source_dataset_id.startswith("sim-"):
        parser.error("--source-dataset-id must start with sim-")
    if not args.adapter_dataset_id.startswith("sim-"):
        parser.error("--adapter-dataset-id must start with sim-")

    scenarios = database_scenarios(args.postgres_dsn, args.source_dataset_id)
    local_records = read_local_cdc_records(
        args.local_bootstrap_servers,
        scenarios,
        timeout_seconds=args.timeout_seconds,
    )
    topics = CdcSimulationTopics()
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("remote replay requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    remote_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    output_baselines = {
        topics.canonical: topic_end_offsets(topics.canonical, client_kwargs=remote_kwargs),
        topics.dead_letters: topic_end_offsets(
            topics.dead_letters, client_kwargs=remote_kwargs
        ),
    }

    from kafka import KafkaProducer

    producer = KafkaProducer(
        client_id="poker-cdc-remote-simulation-replay-v1",
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        **remote_kwargs,
    )
    published: dict[str, dict[str, int]] = {}
    try:
        ordered = sorted(
            local_records.items(),
            key=lambda item: (item[1].partition, item[1].offset),
        )
        for scenario, message in ordered:
            metadata = producer.send(
                topics.source,
                key=message.key,
                value=message.value,
                headers=list(message.headers),
                timestamp_ms=message.timestamp,
            ).get(timeout=args.timeout_seconds)
            published[scenario] = {
                "partition": int(metadata.partition),
                "offset": int(metadata.offset),
            }
        producer.flush(timeout=args.timeout_seconds)
    finally:
        producer.close(timeout=args.timeout_seconds)

    expected = {
        "valid_cash": ("canonical", None),
        "filtered_play_money": ("filtered", None),
        "checksum_mismatch": ("dead_letter", "checksum_mismatch"),
        "malformed_protobuf": ("dead_letter", "invalid_binary_payload"),
        "game_type_mismatch": ("dead_letter", "game_type_mismatch"),
        "unknown_codec_version": ("dead_letter", "unknown_codec_version"),
    }
    records = []
    for scenario in sorted(scenarios):
        row = scenarios[scenario]
        outcome, error_code = expected[scenario]
        record: dict[str, Any] = {
            "scenario": scenario,
            "hand_id": row["hand_id"],
            "game_type": row["game_type"],
            "expected_outcome": outcome,
            "expected_error_code": error_code,
            "published_to_managed_kafka": scenario in published,
        }
        if scenario in published:
            source = local_records[scenario]
            record.update(
                {
                    "remote_partition": published[scenario]["partition"],
                    "remote_offset": published[scenario]["offset"],
                    "source_key_sha256": _sha256(source.key),
                    "source_value_sha256": _sha256(source.value),
                }
            )
        records.append(record)

    manifest = {
        "schema_version": 1,
        "source_dataset_id": args.source_dataset_id,
        "adapter_dataset_id": args.adapter_dataset_id,
        "adapter_group_id": args.adapter_group_id,
        "adapter_build_version": args.adapter_build_version,
        "topics": {
            "source": topics.source,
            "canonical": topics.canonical,
            "dead_letters": topics.dead_letters,
        },
        "output_start_offsets": output_baselines,
        "records": records,
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": "published",
                "source_dataset_id": args.source_dataset_id,
                "managed_inputs": len(published),
                "filtered_before_managed_kafka": len(scenarios) - len(published),
                "manifest": str(args.manifest),
                "remote_positions": published,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
