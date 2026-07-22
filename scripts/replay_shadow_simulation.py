#!/usr/bin/env python3
"""Publish one accepted PostgreSQL CDC hand plus context into the shadow path."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import time
import uuid
from datetime import timedelta
from pathlib import Path
from typing import Any

from pipeline.cdc import SimulationProtobufV1Decoder
from pipeline.cdc.hand_history import CdcAdapterConfig, HandCompletedOutboxRow
from pipeline.config import get_settings
from pipeline.events import (
    HAND_COMPLETED,
    USER_CONTEXT_UPDATED,
    HandCompletedPayload,
    UserContextPayload,
    build_event,
    validate_event,
)
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import CdcSimulationTopics, ShadowSimulationTopics
from scripts.replay_cdc_simulation_remote import topic_end_offsets


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"


def _json_bytes(value: object) -> bytes:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _sha256(value: bytes | None) -> str:
    return hashlib.sha256(value or b"").hexdigest()


def load_source_hand(dsn: str, source_dataset_id: str) -> HandCompletedPayload:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT h.outbox_id, h.hand_id, h.tenant_id, h.product_id,
                       h.game_type, h.payload_schema_version, h.codec_version,
                       h.payload_sha256, h.payload, h.occurred_at, h.emitted_at
                FROM public.hand_history h
                JOIN public.hand_completed_outbox o ON o.id = h.outbox_id
                WHERE h.simulation_dataset_id = %s
                ORDER BY h.occurred_at, h.hand_id
                """,
                (source_dataset_id,),
            )
            rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError("shadow replay requires exactly one PostgreSQL source hand")
    (
        outbox_id,
        hand_id,
        tenant_id,
        product_id,
        game_type,
        schema_version,
        codec_version,
        payload_sha256,
        payload,
        occurred_at,
        emitted_at,
    ) = rows[0]
    raw = bytes(payload)
    if _sha256(raw) != payload_sha256:
        raise ValueError("PostgreSQL payload checksum changed")
    row = HandCompletedOutboxRow(
        id=outbox_id,
        aggregate_type="poker-hand",
        aggregate_id=hand_id,
        event_type="poker.hand.completed",
        payload_schema_version=schema_version,
        tenant_id=tenant_id,
        product_id=product_id,
        game_type=game_type,
        occurred_at=occurred_at,
        emitted_at=emitted_at,
        codec_version=codec_version,
        payload_sha256=payload_sha256,
        payload=base64.b64encode(raw).decode(),
    )
    return SimulationProtobufV1Decoder().decode(
        raw,
        row=row,
        config=CdcAdapterConfig(dataset_id="sim-cdc-v1"),
    )


def read_local_source_record(
    bootstrap_servers: str,
    hand_id: str,
    *,
    timeout_seconds: float,
) -> Any:
    from kafka import KafkaConsumer, TopicPartition

    topic = CdcSimulationTopics().source
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers.split(","),
        group_id=None,
        enable_auto_commit=False,
        security_protocol="PLAINTEXT",
    )
    try:
        partitions = consumer.partitions_for_topic(topic)
        if not partitions:
            raise RuntimeError(f"local source topic is unavailable: {topic}")
        assignments = [TopicPartition(topic, part) for part in sorted(partitions)]
        consumer.assign(assignments)
        consumer.seek_to_beginning(*assignments)
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            for messages in consumer.poll(timeout_ms=500, max_records=500).values():
                for message in messages:
                    try:
                        document = json.loads(message.value)
                    except (TypeError, json.JSONDecodeError):
                        continue
                    after = document.get("after") if isinstance(document, dict) else None
                    if isinstance(after, dict) and after.get("aggregate_id") == hand_id:
                        return message
    finally:
        consumer.close()
    raise TimeoutError(f"local Debezium record not found for hand {hand_id}")


def build_context_events(
    hand: HandCompletedPayload,
    *,
    dataset_id: str,
    tenant_id: str = "demo",
    product_id: str = "poker",
) -> list[Any]:
    countries = (("TR", "Europe/Istanbul"), ("DE", "Europe/Berlin"))
    channels = ("organic", "affiliate", "paid", "referral")
    events = []
    for index, player in enumerate(hand.players):
        effective_at = hand.played_at - timedelta(hours=1)
        country, timezone_name = countries[index % len(countries)]
        payload = UserContextPayload(
            user_id=player.player_id,
            context_version=1,
            effective_at=effective_at,
            account_created_at=effective_at - timedelta(days=180 + index * 31),
            country_bucket=country,
            timezone=timezone_name,
            acquisition_channel=channels[index % len(channels)],
            kyc_level="verified" if index % 3 else "basic",
            account_status="active",
            bankroll_bucket=("low", "medium", "high")[index % 3],
            preferred_stake_bucket=("micro", "low", "medium")[index % 3],
            skill_rating=round(0.2 + index * 0.1, 6),
            device_id=str(uuid.uuid5(uuid.NAMESPACE_URL, f"{dataset_id}:device:{index}")),
            network_cluster_id=f"{dataset_id}-network-{index // 2}",
        )
        events.append(
            build_event(
                event_type=USER_CONTEXT_UPDATED,
                aggregate_id=f"{player.player_id}:context:1",
                payload=payload,
                dataset_id=dataset_id,
                dataset_split="live",
                occurred_at=payload.effective_at,
                tenant_id=tenant_id,
                product_id=product_id,
            )
        )
    return events


def build_context_watermark(
    *, dataset_id: str, partition: int, occurred_at: Any
) -> Any:
    user_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{dataset_id}:watermark:{partition}"))
    payload = UserContextPayload(
        user_id=user_id,
        context_version=1,
        effective_at=occurred_at,
        account_created_at=occurred_at - timedelta(days=365),
        country_bucket="TR",
        timezone="Europe/Istanbul",
        acquisition_channel="organic",
        kyc_level="verified",
        account_status="active",
        bankroll_bucket="medium",
        preferred_stake_bucket="low",
        skill_rating=0.5,
        device_id=f"watermark-device-{partition}",
        network_cluster_id=f"watermark-network-{partition}",
    )
    return build_event(
        event_type=USER_CONTEXT_UPDATED,
        aggregate_id=f"{user_id}:context:1",
        payload=payload,
        dataset_id=dataset_id,
        dataset_split="live",
        occurred_at=occurred_at,
    )


def wait_for_target_canonical(
    topic: str,
    starts: dict[str, int],
    hand_id: str,
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> tuple[Any, Any]:
    from scripts.verify_cdc_simulation_remote import read_bounded_topic

    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        for message in read_bounded_topic(
            topic, starts, client_kwargs=client_kwargs, timeout_seconds=5
        ):
            try:
                event = validate_event(json.loads(message.value))
            except (ValueError, TypeError, json.JSONDecodeError):
                continue
            if event.event_type == HAND_COMPLETED and event.payload["hand_id"] == hand_id:
                return message, event
        time.sleep(0.5)
    raise TimeoutError(f"adapter did not publish canonical hand {hand_id}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--adapter-dataset-id", default="sim-cdc-v1")
    parser.add_argument("--adapter-group-id", default="poker-go-hand-adapter-sim-v1")
    parser.add_argument("--adapter-build-version", required=True)
    parser.add_argument("--flink-build-version", required=True)
    parser.add_argument("--risk-build-version", required=True)
    parser.add_argument("--model-run-id", required=True)
    parser.add_argument("--postgres-dsn", default=os.getenv("CDC_SIM_POSTGRES_DSN", DEFAULT_DSN))
    parser.add_argument("--local-bootstrap-servers", default="localhost:9092")
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=180.0)
    args = parser.parse_args()
    if not args.source_dataset_id.startswith("sim-"):
        parser.error("--source-dataset-id must start with sim-")
    if not args.adapter_dataset_id.startswith("sim-"):
        parser.error("--adapter-dataset-id must start with sim-")

    hand = load_source_hand(args.postgres_dsn, args.source_dataset_id)
    source = read_local_source_record(
        args.local_bootstrap_servers,
        hand.hand_id,
        timeout_seconds=args.timeout_seconds,
    )
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("shadow replay requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    remote_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    cdc_topics = CdcSimulationTopics()
    shadow_topics = ShadowSimulationTopics()
    observed_topics = (
        cdc_topics.canonical,
        shadow_topics.player_context,
        shadow_topics.pair_features,
        shadow_topics.risk_scores,
        shadow_topics.rule_evidence,
        shadow_topics.review_decisions,
        shadow_topics.risk_alerts,
        shadow_topics.dead_letters,
    )
    starts = {
        topic: topic_end_offsets(topic, client_kwargs=remote_kwargs)
        for topic in observed_topics
    }

    from kafka import KafkaProducer

    producer = KafkaProducer(
        client_id="poker-shadow-simulation-replay-v1",
        acks="all",
        retries=10,
        max_in_flight_requests_per_connection=1,
        **remote_kwargs,
    )
    context_events = build_context_events(hand, dataset_id=args.adapter_dataset_id)
    watermark_at = hand.played_at + timedelta(minutes=2)
    context_positions = []
    try:
        for event in context_events:
            metadata = producer.send(
                shadow_topics.user_context,
                key=event.payload["user_id"].encode(),
                value=_json_bytes(event),
            ).get(timeout=args.timeout_seconds)
            context_positions.append(
                {"event_id": str(event.event_id), "partition": metadata.partition,
                 "offset": metadata.offset}
            )
        context_partition_count = len(
            topic_end_offsets(shadow_topics.user_context, client_kwargs=remote_kwargs)
        )
        context_watermarks = []
        for partition in range(context_partition_count):
            event = build_context_watermark(
                dataset_id=args.adapter_dataset_id,
                partition=partition,
                occurred_at=watermark_at,
            )
            metadata = producer.send(
                shadow_topics.user_context,
                partition=partition,
                key=event.payload["user_id"].encode(),
                value=_json_bytes(event),
            ).get(timeout=args.timeout_seconds)
            context_watermarks.append(
                {"event_id": str(event.event_id), "partition": metadata.partition,
                 "offset": metadata.offset}
            )
        source_metadata = producer.send(
            cdc_topics.source,
            key=source.key,
            value=source.value,
            headers=list(source.headers),
            timestamp_ms=source.timestamp,
        ).get(timeout=args.timeout_seconds)
        producer.flush(timeout=args.timeout_seconds)

        canonical_message, canonical_event = wait_for_target_canonical(
            cdc_topics.canonical,
            starts[cdc_topics.canonical],
            hand.hand_id,
            client_kwargs=remote_kwargs,
            timeout_seconds=args.timeout_seconds,
        )
        canonical_payload = HandCompletedPayload.model_validate(canonical_event.payload)
        watermark_payload = canonical_payload.model_copy(
            update={
                "hand_id": f"{canonical_payload.hand_id}-watermark",
                "played_at": watermark_at,
            }
        )
        watermark_event = build_event(
            event_type=HAND_COMPLETED,
            aggregate_id=watermark_payload.hand_id,
            payload=watermark_payload,
            dataset_id=args.adapter_dataset_id,
            dataset_split="live",
            occurred_at=watermark_at,
            tenant_id=canonical_event.tenant_id,
            product_id=canonical_event.product_id,
        )
        watermark_metadata = producer.send(
            cdc_topics.canonical,
            partition=canonical_message.partition,
            key=watermark_payload.table_id.encode(),
            value=_json_bytes(watermark_event),
        ).get(timeout=args.timeout_seconds)
        producer.flush(timeout=args.timeout_seconds)
    finally:
        producer.close(timeout=args.timeout_seconds)

    manifest = {
        "schema_version": 1,
        "source_dataset_id": args.source_dataset_id,
        "adapter_dataset_id": args.adapter_dataset_id,
        "target_hand_id": hand.hand_id,
        "target_player_ids": sorted(player.player_id for player in hand.players),
        "build_versions": {
            "adapter": args.adapter_build_version,
            "flink": args.flink_build_version,
            "risk": args.risk_build_version,
        },
        "model_run_id": args.model_run_id,
        "consumer_groups": {
            "adapter": args.adapter_group_id,
            "context": "flink-context-enrichment-sim-v1",
            "pair_features": "flink-pair-features-sim-v1",
            "risk": "poker-go-risk-scorer-sim-v1",
        },
        "topics": {
            "source": cdc_topics.source,
            "canonical": cdc_topics.canonical,
            **shadow_topics.__dict__,
        },
        "output_start_offsets": starts,
        "source": {
            "partition": source_metadata.partition,
            "offset": source_metadata.offset,
            "key_sha256": _sha256(source.key),
            "value_sha256": _sha256(source.value),
        },
        "contexts": context_positions,
        "watermarks": {
            "context": context_watermarks,
            "hand_event_id": str(watermark_event.event_id),
            "hand_id": watermark_payload.hand_id,
            "partition": watermark_metadata.partition,
            "offset": watermark_metadata.offset,
        },
    }
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"status": "published", "manifest": str(args.manifest),
                      "target_hand_id": hand.hand_id,
                      "contexts": len(context_events)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
