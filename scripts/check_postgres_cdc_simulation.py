#!/usr/bin/env python3
"""Verify the local PostgreSQL -> Debezium -> Kafka -> Go simulation."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter

from pipeline.events import HAND_COMPLETED, validate_event


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"
SOURCE_TOPIC = "poker.sim.cdc-hand-outbox.v1"
CANONICAL_TOPIC = "poker.sim.hands.raw.v1"
DEAD_LETTER_TOPIC = "poker.sim.pipeline.dead-letter.v1"
DEFAULT_ALLOWED_GAME_TYPES = ("NLH_CASH_6MAX", "NLH_TOURNAMENT_6MAX")


def _csv(value: str) -> tuple[str, ...]:
    parsed = tuple(part.strip() for part in value.split(",") if part.strip())
    if not parsed:
        raise argparse.ArgumentTypeError("allowlist must not be empty")
    return parsed


def _assign_topic(consumer, topic: str):
    from kafka import TopicPartition

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        raise RuntimeError(f"Kafka topic has no readable partitions: {topic}")
    assignments = [TopicPartition(topic, partition) for partition in sorted(partitions)]
    consumer.assign(assignments)
    return assignments


def topic_size(bootstrap_servers: str, topic: str) -> int:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=None,
        enable_auto_commit=False,
    )
    try:
        assignments = _assign_topic(consumer, topic)
        beginnings = consumer.beginning_offsets(assignments)
        endings = consumer.end_offsets(assignments)
        return sum(endings[item] - beginnings[item] for item in assignments)
    finally:
        consumer.close()


def read_topic_records(
    bootstrap_servers: str,
    topic: str,
    *,
    timeout_seconds: float,
) -> list:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=None,
        enable_auto_commit=False,
    )
    records = []
    try:
        assignments = _assign_topic(consumer, topic)
        consumer.seek_to_beginning(*assignments)
        endings = consumer.end_offsets(assignments)
        expected = sum(endings[item] - consumer.position(item) for item in assignments)
        deadline = time.monotonic() + timeout_seconds
        while len(records) < expected and time.monotonic() < deadline:
            batches = consumer.poll(timeout_ms=500, max_records=500)
            for messages in batches.values():
                records.extend(messages)
        if len(records) != expected:
            raise RuntimeError(
                f"read {len(records)} of {expected} records from {topic}"
            )
        return records
    finally:
        consumer.close()


def database_counts(dsn: str, source_dataset_id: str) -> tuple[
    int,
    int,
    dict[str, tuple[int, int]],
    set[str],
    set[str],
]:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT h.game_type, count(*) AS source_rows, count(o.id) AS outbox_rows
                FROM public.hand_history h
                LEFT JOIN public.hand_completed_outbox o ON o.id = h.outbox_id
                WHERE h.simulation_dataset_id = %s
                GROUP BY h.game_type
                ORDER BY h.game_type
                """,
                (source_dataset_id,),
            )
            by_game_type = {
                game_type: (source_rows, outbox_rows)
                for game_type, source_rows, outbox_rows in cursor.fetchall()
            }
            cursor.execute(
                """
                SELECT h.hand_id, o.id IS NOT NULL AS eligible
                FROM public.hand_history h
                LEFT JOIN public.hand_completed_outbox o ON o.id = h.outbox_id
                WHERE h.simulation_dataset_id = %s
                """,
                (source_dataset_id,),
            )
            hand_eligibility = dict(cursor.fetchall())
    source_rows = sum(item[0] for item in by_game_type.values())
    outbox_rows = sum(item[1] for item in by_game_type.values())
    all_hand_ids = set(hand_eligibility)
    eligible_hand_ids = {
        hand_id for hand_id, eligible in hand_eligibility.items() if eligible
    }
    return source_rows, outbox_rows, by_game_type, all_hand_ids, eligible_hand_ids


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--postgres-dsn", default=DEFAULT_DSN)
    parser.add_argument("--source-dataset-id", default="sim-cdc-smoke-v1")
    parser.add_argument("--dataset-id", default="sim-cdc-v1")
    parser.add_argument("--expected-database", default="poker_sim")
    parser.add_argument("--expected-source-rows", type=int, default=8)
    parser.add_argument("--expected-canonical-records", type=int, default=4)
    parser.add_argument(
        "--allowed-game-types", type=_csv, default=DEFAULT_ALLOWED_GAME_TYPES
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    args = parser.parse_args()

    (
        source_rows,
        outbox_rows,
        by_game_type,
        all_hand_ids,
        eligible_hand_ids,
    ) = database_counts(args.postgres_dsn, args.source_dataset_id)
    if source_rows != args.expected_source_rows:
        raise ValueError(
            f"PostgreSQL source rows {source_rows} != {args.expected_source_rows}"
        )
    if outbox_rows != args.expected_canonical_records:
        raise ValueError(
            f"PostgreSQL outbox rows {outbox_rows} != "
            f"{args.expected_canonical_records}"
        )
    allowed = set(args.allowed_game_types)
    for game_type, (game_source_rows, game_outbox_rows) in by_game_type.items():
        expected = game_source_rows if game_type in allowed else 0
        if game_outbox_rows != expected:
            raise ValueError(
                f"database filter mismatch for {game_type}: "
                f"outbox={game_outbox_rows}, expected={expected}"
            )

    source_topic_records = topic_size(args.bootstrap_servers, SOURCE_TOPIC)
    canonical_topic_records = topic_size(args.bootstrap_servers, CANONICAL_TOPIC)
    dead_letters = topic_size(args.bootstrap_servers, DEAD_LETTER_TOPIC)

    source_matched_hand_ids: set[str] = set()
    source_positions: set[tuple[int, int]] = set()
    for message in read_topic_records(
        args.bootstrap_servers,
        SOURCE_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        envelope = json.loads(message.value)
        after = envelope.get("after") if isinstance(envelope, dict) else None
        aggregate_id = after.get("aggregate_id") if isinstance(after, dict) else None
        if aggregate_id in eligible_hand_ids:
            source_matched_hand_ids.add(aggregate_id)
            source_positions.add((message.partition, message.offset))
    if source_matched_hand_ids != eligible_hand_ids:
        raise ValueError("CDC source topic does not match the run's eligible hands")

    dead_letters_for_run = 0
    for message in read_topic_records(
        args.bootstrap_servers,
        DEAD_LETTER_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        value = json.loads(message.value)
        payload = value.get("payload", {}) if isinstance(value, dict) else {}
        position = (payload.get("source_partition"), payload.get("source_offset"))
        if position in source_positions:
            dead_letters_for_run += 1
    if dead_letters_for_run:
        raise ValueError(
            f"accepted simulation run produced {dead_letters_for_run} dead letters"
        )

    seen_event_ids: set[str] = set()
    canonical_matched_hand_ids: set[str] = set()
    game_types: Counter[str] = Counter()
    for message in read_topic_records(
        args.bootstrap_servers,
        CANONICAL_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        event = validate_event(json.loads(message.value))
        hand_id = event.payload["hand_id"]
        if hand_id not in all_hand_ids:
            continue
        if hand_id not in eligible_hand_ids:
            raise ValueError(f"filtered hand reached canonical Kafka: {hand_id}")
        if event.event_type != HAND_COMPLETED or event.dataset_id != args.dataset_id:
            raise ValueError(f"unexpected canonical event {event.event_id}")
        table_id = event.payload["table_id"]
        if message.key != table_id.encode():
            raise ValueError(f"incorrect Kafka key for hand {hand_id}")
        headers = {key: value.decode() for key, value in message.headers}
        required_headers = {
            "event_id",
            "cdc_database",
            "cdc_source_lsn",
            "cdc_source_tx_id",
            "cdc_payload_sha256",
            "cdc_game_type",
        }
        if not required_headers.issubset(headers):
            raise ValueError(f"missing CDC lineage headers for hand {hand_id}")
        if headers["event_id"] != str(event.event_id):
            raise ValueError(f"event ID header mismatch for hand {hand_id}")
        if headers["cdc_database"] != args.expected_database:
            raise ValueError(f"database lineage mismatch for hand {hand_id}")
        if headers["cdc_game_type"] not in allowed:
            raise ValueError(f"disallowed game reached canonical Kafka: {hand_id}")
        if str(event.event_id) in seen_event_ids:
            raise ValueError(f"duplicate canonical event ID: {event.event_id}")
        seen_event_ids.add(str(event.event_id))
        canonical_matched_hand_ids.add(hand_id)
        game_types[headers["cdc_game_type"]] += 1
    if canonical_matched_hand_ids != eligible_hand_ids:
        raise ValueError("canonical topic does not match the run's eligible hands")

    print(
        json.dumps(
            {
                "status": "passed",
                "source_dataset_id": args.source_dataset_id,
                "postgres_source_rows": source_rows,
                "postgres_outbox_rows": outbox_rows,
                "filtered_before_kafka": source_rows - outbox_rows,
                "cdc_source_topic_records": source_topic_records,
                "cdc_source_records_for_run": len(source_matched_hand_ids),
                "canonical_topic_records": canonical_topic_records,
                "canonical_records_for_run": len(canonical_matched_hand_ids),
                "dead_letter_topic_records": dead_letters,
                "dead_letters_for_run": dead_letters_for_run,
                "canonical_game_types": dict(sorted(game_types.items())),
                "database_game_types": {
                    key: {"source_rows": value[0], "outbox_rows": value[1]}
                    for key, value in by_game_type.items()
                },
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
