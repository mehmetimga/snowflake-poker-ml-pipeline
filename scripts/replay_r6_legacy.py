#!/usr/bin/env python3
"""Publish the sealed D7 hand payloads to the isolated legacy ``hands.raw`` path."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.ops.realtime_retirement import (
    LEGACY_GROUP_ID,
    LEGACY_HANDS_TOPIC,
    R6_RUN_TYPE,
    expected_legacy_counts,
    load_r6_source,
)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def clean_source_commit() -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError("R6 replay requires a clean committed worktree")
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--spcs-report", type=Path, required=True)
    parser.add_argument("--sink-report", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")

    source = load_r6_source(
        args.manifest,
        args.spcs_report,
        args.sink_report,
    )
    source_commit = clean_source_commit()
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("R6 managed replay requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }

    from kafka import KafkaConsumer, KafkaProducer, TopicPartition

    inspector = KafkaConsumer(
        group_id=None,
        enable_auto_commit=False,
        **client_kwargs,
    )
    group = KafkaConsumer(
        group_id=LEGACY_GROUP_ID,
        enable_auto_commit=False,
        **client_kwargs,
    )
    try:
        partitions = sorted(inspector.partitions_for_topic(LEGACY_HANDS_TOPIC) or [])
        if not partitions:
            raise RuntimeError(f"legacy topic does not exist: {LEGACY_HANDS_TOPIC}")
        assignments = [
            TopicPartition(LEGACY_HANDS_TOPIC, partition)
            for partition in partitions
        ]
        starts = inspector.end_offsets(assignments)
        commits_before = {
            item: group.committed(item)
            for item in assignments
        }
    finally:
        group.close()
        inspector.close()

    producer = KafkaProducer(
        client_id="poker-r6-legacy-replay-v1",
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        **client_kwargs,
    )
    records: list[dict[str, Any]] = []
    started_at = _iso_now()
    try:
        for payload in sorted(
            source["hand_payloads"],
            key=lambda hand: (
                hand["played_at"],
                hand["table_id"],
                hand["hand_id"],
            ),
        ):
            metadata = producer.send(
                LEGACY_HANDS_TOPIC,
                key=str(payload["hand_id"]).encode(),
                value=_json_bytes(payload),
            ).get(timeout=args.timeout_seconds)
            records.append(
                {
                    "hand_id": str(payload["hand_id"]),
                    "partition": int(metadata.partition),
                    "offset": int(metadata.offset),
                }
            )
        producer.flush(timeout=args.timeout_seconds)
    finally:
        producer.close(timeout=args.timeout_seconds)
    completed_at = _iso_now()

    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "legacy_replay",
        "status": "published",
        "training_allowed": False,
        "dataset_id": source["dataset_id"],
        "target_hand_ids": list(source["target_hand_ids"]),
        "topic": LEGACY_HANDS_TOPIC,
        "consumer_group": LEGACY_GROUP_ID,
        "topic_start_offsets": {
            str(item.partition): int(starts[item]) for item in assignments
        },
        "consumer_commits_before": {
            str(item.partition): (
                None if commits_before[item] is None else int(commits_before[item])
            )
            for item in assignments
        },
        "records": records,
        "expected_legacy_counts": expected_legacy_counts(
            source["hand_payloads"]
        ),
        "source_commit": source_commit,
        "source_hashes": source["source_hashes"],
        "started_at": started_at,
        "completed_at": completed_at,
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
