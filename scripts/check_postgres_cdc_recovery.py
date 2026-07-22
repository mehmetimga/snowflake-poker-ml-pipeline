#!/usr/bin/env python3
"""Verify live replay after an injected post-publish commit failure."""

from __future__ import annotations

import argparse
import json

from check_postgres_cdc_simulation import (
    CANONICAL_TOPIC,
    DEAD_LETTER_TOPIC,
    DEFAULT_DSN,
    SOURCE_TOPIC,
    read_topic_records,
)
from pipeline.events import HAND_COMPLETED, validate_event


def database_hand(dsn: str, dataset_id: str) -> str:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT h.hand_id, o.id IS NOT NULL AS eligible
                FROM public.hand_history h
                LEFT JOIN public.hand_completed_outbox o ON o.id = h.outbox_id
                WHERE h.simulation_dataset_id = %s
                """,
                (dataset_id,),
            )
            rows = cursor.fetchall()
    if len(rows) != 1 or not rows[0][1]:
        raise ValueError("recovery dataset must contain one eligible source hand")
    return rows[0][0]


def committed_offset(
    bootstrap_servers: str, group_id: str, partition: int
) -> int | None:
    from kafka import KafkaAdminClient, TopicPartition

    admin = KafkaAdminClient(
        bootstrap_servers=bootstrap_servers.split(","),
        client_id="poker-cdc-recovery-offset-checker",
    )
    try:
        offsets = admin.list_consumer_group_offsets(group_id)
        value = offsets.get(TopicPartition(SOURCE_TOPIC, partition))
        return None if value is None else value.offset
    finally:
        admin.close()


def _fingerprint(message) -> tuple[bytes | None, bytes, tuple[tuple[str, bytes], ...]]:
    return message.key, message.value, tuple(message.headers)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--postgres-dsn", default=DEFAULT_DSN)
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--dataset-id", default="sim-cdc-v1")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--phase", choices=("after-failure", "complete"), required=True)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    args = parser.parse_args()

    hand_id = database_hand(args.postgres_dsn, args.source_dataset_id)
    source_matches = []
    for message in read_topic_records(
        args.bootstrap_servers,
        SOURCE_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        envelope = json.loads(message.value)
        after = envelope.get("after") if isinstance(envelope, dict) else None
        if isinstance(after, dict) and after.get("aggregate_id") == hand_id:
            source_matches.append(message)
    if len(source_matches) != 1:
        raise ValueError(
            f"expected one recovery source record, found {len(source_matches)}"
        )
    source = source_matches[0]

    canonical = []
    event_ids = set()
    for message in read_topic_records(
        args.bootstrap_servers,
        CANONICAL_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        event = validate_event(json.loads(message.value))
        if event.payload["hand_id"] != hand_id:
            continue
        if event.event_type != HAND_COMPLETED or event.dataset_id != args.dataset_id:
            raise ValueError("recovery canonical envelope changed")
        canonical.append(message)
        event_ids.add(str(event.event_id))

    expected_canonical = 1 if args.phase == "after-failure" else 2
    if len(canonical) != expected_canonical:
        raise ValueError(
            f"recovery canonical occurrences {len(canonical)} != {expected_canonical}"
        )
    if len(event_ids) != 1 or len({_fingerprint(item) for item in canonical}) != 1:
        raise ValueError("replayed canonical output is not byte-identical")

    matching_dead_letters = 0
    for message in read_topic_records(
        args.bootstrap_servers,
        DEAD_LETTER_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        document = json.loads(message.value)
        payload = document.get("payload", {}) if isinstance(document, dict) else {}
        if (
            payload.get("source_partition") == source.partition
            and payload.get("source_offset") == source.offset
        ):
            matching_dead_letters += 1
    if matching_dead_letters:
        raise ValueError("valid recovery hand reached the DLQ")

    committed = committed_offset(
        args.bootstrap_servers,
        args.group_id,
        source.partition,
    )
    if args.phase == "after-failure":
        if committed is not None and committed > source.offset:
            raise ValueError("source offset was committed despite injected failure")
    elif committed is None or committed < source.offset + 1:
        raise ValueError("source offset was not committed after recovery")

    print(
        json.dumps(
            {
                "status": "passed",
                "phase": args.phase,
                "source_dataset_id": args.source_dataset_id,
                "group_id": args.group_id,
                "source_partition": source.partition,
                "source_offset": source.offset,
                "committed_offset": committed,
                "canonical_occurrences": len(canonical),
                "unique_event_ids": len(event_ids),
                "byte_identical_replay": len(canonical) == 2
                and len({_fingerprint(item) for item in canonical}) == 1,
                "dead_letters_for_source": matching_dead_letters,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
