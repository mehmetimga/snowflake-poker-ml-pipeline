#!/usr/bin/env python3
"""Verify deterministic canonical/filter/DLQ outcomes for the CDC fault suite."""

from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict

from check_postgres_cdc_simulation import (
    CANONICAL_TOPIC,
    DEAD_LETTER_TOPIC,
    DEFAULT_DSN,
    SOURCE_TOPIC,
    read_topic_records,
    topic_size,
)
from pipeline.cdc.simulation_scenarios import FAULT_SCENARIOS
from pipeline.events import HAND_COMPLETED, validate_event


def database_scenarios(dsn: str, dataset_id: str) -> dict[str, dict]:
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
    if len({row[0] for row in rows}) != len(rows):
        raise ValueError("fault dataset contains duplicate scenario names")
    return {
        scenario: {
            "hand_id": hand_id,
            "game_type": game_type,
            "eligible": eligible,
        }
        for scenario, hand_id, game_type, eligible in rows
    }


def _fingerprint(message) -> tuple[bytes | None, bytes, tuple[tuple[str, bytes], ...]]:
    return message.key, message.value, tuple(message.headers)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--postgres-dsn", default=DEFAULT_DSN)
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--dataset-id", default="sim-cdc-v1")
    parser.add_argument("--expected-occurrences", type=int, default=1)
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    args = parser.parse_args()
    if args.expected_occurrences < 1:
        parser.error("--expected-occurrences must be positive")

    definitions = {item.name: item for item in FAULT_SCENARIOS}
    database = database_scenarios(args.postgres_dsn, args.source_dataset_id)
    if set(database) != set(definitions):
        raise ValueError(
            f"database scenarios {sorted(database)} != {sorted(definitions)}"
        )
    for name, definition in definitions.items():
        row = database[name]
        if row["game_type"] != definition.game_type:
            raise ValueError(f"game type changed for scenario {name}")
        expected_eligible = definition.expected_outcome != "filtered"
        if row["eligible"] is not expected_eligible:
            raise ValueError(f"database routing mismatch for scenario {name}")

    by_hand_id = {row["hand_id"]: name for name, row in database.items()}
    source_by_scenario = {}
    source_message_by_position = {}
    for message in read_topic_records(
        args.bootstrap_servers,
        SOURCE_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        envelope = json.loads(message.value)
        after = envelope.get("after") if isinstance(envelope, dict) else None
        hand_id = after.get("aggregate_id") if isinstance(after, dict) else None
        scenario = by_hand_id.get(hand_id)
        if scenario is None:
            continue
        if scenario in source_by_scenario:
            raise ValueError(f"duplicate Debezium source record for {scenario}")
        position = (message.partition, message.offset)
        source_by_scenario[scenario] = position
        source_message_by_position[position] = message
    expected_source = {
        name
        for name, definition in definitions.items()
        if definition.expected_outcome != "filtered"
    }
    if set(source_by_scenario) != expected_source:
        raise ValueError(
            f"CDC source scenarios {sorted(source_by_scenario)} != "
            f"{sorted(expected_source)}"
        )

    canonical_by_scenario = defaultdict(list)
    for message in read_topic_records(
        args.bootstrap_servers,
        CANONICAL_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        event = validate_event(json.loads(message.value))
        hand_id = event.payload["hand_id"]
        scenario = by_hand_id.get(hand_id)
        if scenario is None:
            continue
        definition = definitions[scenario]
        if definition.expected_outcome != "canonical":
            raise ValueError(f"poison/filtered scenario reached canonical: {scenario}")
        if event.event_type != HAND_COMPLETED or event.dataset_id != args.dataset_id:
            raise ValueError(f"canonical envelope mismatch for {scenario}")
        if message.key != event.payload["table_id"].encode():
            raise ValueError(f"canonical Kafka key mismatch for {scenario}")
        canonical_by_scenario[scenario].append(message)

    expected_canonical = {
        name
        for name, definition in definitions.items()
        if definition.expected_outcome == "canonical"
    }
    if set(canonical_by_scenario) != expected_canonical:
        raise ValueError("canonical scenarios do not match the fault manifest")
    for scenario, messages in canonical_by_scenario.items():
        if len(messages) != args.expected_occurrences:
            raise ValueError(
                f"canonical {scenario} occurrences {len(messages)} != "
                f"{args.expected_occurrences}"
            )
        if len({_fingerprint(message) for message in messages}) != 1:
            raise ValueError(f"canonical replay changed bytes for {scenario}")

    dlq_by_scenario = defaultdict(list)
    error_codes = Counter()
    position_to_scenario = {
        position: scenario for scenario, position in source_by_scenario.items()
    }
    for message in read_topic_records(
        args.bootstrap_servers,
        DEAD_LETTER_TOPIC,
        timeout_seconds=args.timeout_seconds,
    ):
        document = json.loads(message.value)
        payload = document.get("payload", {}) if isinstance(document, dict) else {}
        position = (payload.get("source_partition"), payload.get("source_offset"))
        scenario = position_to_scenario.get(position)
        if scenario is None:
            continue
        definition = definitions[scenario]
        if definition.expected_outcome != "dead_letter":
            raise ValueError(f"non-poison scenario reached DLQ: {scenario}")
        error_code = payload.get("error_code")
        if error_code != definition.expected_error_code:
            raise ValueError(
                f"DLQ error for {scenario}: {error_code!r} != "
                f"{definition.expected_error_code!r}"
            )
        if document.get("event_type") != "poker.cdc-hand.dead-lettered":
            raise ValueError(f"DLQ envelope mismatch for {scenario}")
        if len(payload.get("source_value_sha256", "")) != 64:
            raise ValueError(f"DLQ source hash missing for {scenario}")
        headers = {key: value.decode() for key, value in message.headers}
        if headers.get("error_code") != error_code:
            raise ValueError(f"DLQ header mismatch for {scenario}")
        if message.key != document["event_id"].encode():
            raise ValueError(f"DLQ key mismatch for {scenario}")
        source_message = source_message_by_position[position]
        if source_message.value in message.value:
            raise ValueError(f"raw CDC value leaked into DLQ for {scenario}")
        if database[scenario]["hand_id"].encode() in message.value:
            raise ValueError(f"hand identity leaked into DLQ for {scenario}")
        dlq_by_scenario[scenario].append(message)
        error_codes[error_code] += 1

    expected_dlq = {
        name
        for name, definition in definitions.items()
        if definition.expected_outcome == "dead_letter"
    }
    if set(dlq_by_scenario) != expected_dlq:
        raise ValueError("DLQ scenarios do not match the fault manifest")
    for scenario, messages in dlq_by_scenario.items():
        if len(messages) != args.expected_occurrences:
            raise ValueError(
                f"DLQ {scenario} occurrences {len(messages)} != "
                f"{args.expected_occurrences}"
            )
        if len({_fingerprint(message) for message in messages}) != 1:
            raise ValueError(f"DLQ replay changed bytes for {scenario}")

    print(
        json.dumps(
            {
                "status": "passed",
                "source_dataset_id": args.source_dataset_id,
                "source_rows": len(database),
                "filtered_rows": sum(not row["eligible"] for row in database.values()),
                "cdc_records_for_run": len(source_by_scenario),
                "canonical_occurrences": sum(map(len, canonical_by_scenario.values())),
                "dead_letter_occurrences": sum(map(len, dlq_by_scenario.values())),
                "expected_occurrences_per_output": args.expected_occurrences,
                "error_codes": dict(sorted(error_codes.items())),
                "source_topic_records_total": topic_size(
                    args.bootstrap_servers, SOURCE_TOPIC
                ),
                "canonical_topic_records_total": topic_size(
                    args.bootstrap_servers, CANONICAL_TOPIC
                ),
                "dead_letter_topic_records_total": topic_size(
                    args.bootstrap_servers, DEAD_LETTER_TOPIC
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
