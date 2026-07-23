#!/usr/bin/env python3
"""Publish one sealed hand-only acceptance pack to canonical Confluent topics."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.events import (
    HAND_COMPLETED,
    HandCompletedPayload,
    build_event,
    validate_event,
)
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import canonical_spcs_topics


def _json_bytes(value: Any) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _source_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unavailable"


def load_public_pack(pack_dir: Path) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Validate the sealed manifest without opening private oracle contents."""
    manifest_path = pack_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if (
        manifest.get("schema_version") != 1
        or manifest.get("product_type") != "alert_acceptance"
        or manifest.get("training_allowed") is not False
        or manifest.get("private_oracle_after_scoring_only") is not True
    ):
        raise ValueError("SPCS replay requires a sealed training-excluded pack")
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("acceptance artifact bindings are missing")
    for relative, expected_hash in artifacts.items():
        artifact = pack_dir / relative
        if not artifact.is_file() or _sha256(artifact) != expected_hash:
            raise ValueError(f"alert-acceptance artifact hash mismatch: {relative}")
    hands = [
        json.loads(line)
        for line in (pack_dir / "events" / "hands.jsonl").read_text().splitlines()
        if line.strip()
    ]
    if len(hands) != int(manifest["counts"]["hands"]):
        raise ValueError("acceptance hand count changed")
    events = [validate_event(document) for document in hands]
    if any(
        event.event_type != HAND_COMPLETED
        or event.dataset_id != manifest["dataset_id"]
        or event.dataset_split != manifest["dataset_split"]
        for event in events
    ):
        raise ValueError("acceptance public hand boundary changed")
    return manifest, hands


def topic_end_offsets(topic: str, *, client_kwargs: dict[str, Any]) -> dict[str, int]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(group_id=None, enable_auto_commit=False, **client_kwargs)
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"managed Kafka topic is unavailable: {topic}")
        assignments = [
            TopicPartition(topic, partition) for partition in sorted(partitions)
        ]
        return {
            str(item.partition): offset
            for item, offset in consumer.end_offsets(assignments).items()
        }
    finally:
        consumer.close()


def build_watermark_events(
    hand_document: dict[str, Any],
    *,
    partitions: list[int],
    marker_dataset_id: str,
) -> list[tuple[str, int, Any]]:
    """Build two later hand markers per partition for both Flink event-time jobs."""
    source = validate_event(hand_document)
    payload = HandCompletedPayload.model_validate(source.payload)
    result: list[tuple[str, int, Any]] = []
    for role, suffix, delta in (
        ("context-flush", "context-watermark", timedelta(minutes=3)),
        ("pair-flush", "pair-watermark", timedelta(minutes=6)),
    ):
        for partition in partitions:
            played_at = payload.played_at + delta
            marker_payload = payload.model_copy(
                update={
                    "hand_id": (f"{payload.hand_id}-{suffix}-partition-{partition}"),
                    "table_id": (f"{payload.table_id}-{suffix}-partition-{partition}"),
                    "played_at": played_at,
                }
            )
            result.append(
                (
                    role,
                    partition,
                    build_event(
                        event_type=HAND_COMPLETED,
                        aggregate_id=marker_payload.hand_id,
                        payload=marker_payload,
                        dataset_id=marker_dataset_id,
                        dataset_split="acceptance-watermark",
                        occurred_at=played_at,
                        tenant_id=source.tenant_id,
                        product_id=source.product_id,
                    ),
                )
            )
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/multitable-alert-acceptance-v1"),
    )
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()
    pack_dir = args.dataset.resolve()
    pack, hands = load_public_pack(pack_dir)
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("SPCS replay requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    topics = canonical_spcs_topics()
    output_topics = tuple(
        topics[name]
        for name in (
            "player_context",
            "pair_features",
            "risk_scores",
            "rule_evidence",
            "review_decisions",
            "risk_alerts",
            "dead_letters",
        )
    )
    starts = {
        topic: topic_end_offsets(topic, client_kwargs=client_kwargs)
        for topic in output_topics
    }
    input_starts = topic_end_offsets(topics["hands"], client_kwargs=client_kwargs)
    partitions = sorted(int(value) for value in input_starts)
    if not partitions:
        raise RuntimeError("canonical hands topic has no partitions")
    marker_dataset_id = f"{pack['dataset_id']}-watermark-v1"
    last_hand = max(
        hands,
        key=lambda event: event["payload"]["played_at"],
    )
    markers = build_watermark_events(
        last_hand,
        partitions=partitions,
        marker_dataset_id=marker_dataset_id,
    )

    from kafka import KafkaProducer

    producer = KafkaProducer(
        client_id="poker-alert-acceptance-spcs-replay-v1",
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        **client_kwargs,
    )
    published_hands: list[dict[str, Any]] = []
    published_markers: list[dict[str, Any]] = []
    try:
        for document in sorted(
            hands,
            key=lambda event: (
                event["payload"]["played_at"],
                event["payload"]["table_id"],
                event["payload"]["hand_id"],
            ),
        ):
            payload = document["payload"]
            metadata = producer.send(
                topics["hands"],
                key=str(payload["table_id"]).encode(),
                value=_json_bytes(document),
            ).get(timeout=args.timeout_seconds)
            published_hands.append(
                {
                    "event_id": document["event_id"],
                    "hand_id": payload["hand_id"],
                    "table_id": payload["table_id"],
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                }
            )
        for role in ("context-flush", "pair-flush"):
            for marker_role, partition, event in markers:
                if marker_role != role:
                    continue
                metadata = producer.send(
                    topics["hands"],
                    partition=partition,
                    key=event.payload["table_id"].encode(),
                    value=_json_bytes(event),
                ).get(timeout=args.timeout_seconds)
                published_markers.append(
                    {
                        "role": role,
                        "event_id": str(event.event_id),
                        "hand_id": event.payload["hand_id"],
                        "dataset_id": event.dataset_id,
                        "partition": metadata.partition,
                        "offset": metadata.offset,
                    }
                )
        producer.flush(timeout=args.timeout_seconds)
    finally:
        producer.close(timeout=args.timeout_seconds)

    hand_ids = sorted(event["payload"]["hand_id"] for event in hands)
    player_ids = sorted(
        {
            player["player_id"]
            for event in hands
            for player in event["payload"]["players"]
        }
    )
    run_manifest = {
        "schema_version": 1,
        "run_type": "alert_acceptance_spcs",
        "created_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_commit": _source_commit(),
        "acceptance_pack": str(pack_dir),
        "acceptance_manifest_sha256": _sha256(pack_dir / "manifest.json"),
        "dataset_id": pack["dataset_id"],
        "dataset_split": pack["dataset_split"],
        "training_allowed": False,
        "target_hand_ids": hand_ids,
        "target_player_ids": player_ids,
        "expected_counts": {
            "hands": pack["counts"]["hands"],
            "player_context": pack["counts"]["player_context"],
            "pair_features": pack["counts"]["pair_features"],
            "risk_scores": pack["counts"]["score_expectations"],
            "rule_evidence": pack["counts"]["rule_evidence_events"],
            "review_decisions": pack["counts"]["score_expectations"],
            "risk_alerts": pack["counts"]["expected_model_alerts"],
        },
        "bindings": pack["bindings"],
        "topics": topics,
        "consumer_groups": {
            "context": "flink-active-context-synthetic-v2",
            "pair_features": "flink-pair-features-context-synthetic-v2",
            "risk": "poker-go-risk-scorer-v1",
        },
        "input_start_offsets": input_starts,
        "output_start_offsets": starts,
        "published_hands": published_hands,
        "watermarks": {
            "dataset_id": marker_dataset_id,
            "records": published_markers,
        },
        "downstream": {
            "kafka_spcs": "pending_verification",
            "snowflake_sinks": "not_run",
            "admin": "not_run",
        },
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(run_manifest, indent=2, sort_keys=True) + "\n")
    print(
        json.dumps(
            {
                "status": "published",
                "manifest": str(args.manifest),
                "hands": len(published_hands),
                "watermarks": len(published_markers),
                "contexts_published": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
