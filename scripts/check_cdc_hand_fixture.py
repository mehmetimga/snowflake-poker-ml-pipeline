"""Verify the frozen future Debezium-to-canonical hand mapping."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
from datetime import datetime
from pathlib import Path

from pipeline.cdc import (
    CdcAdapterConfig,
    KafkaSourcePosition,
    adapt_debezium_hand_change,
)
from pipeline.events import HAND_COMPLETED, HandCompletedPayload, build_event


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--cdc-fixture",
        type=Path,
        default=Path("schemas/examples/debezium.hand-completed-outbox.v1.json"),
    )
    parser.add_argument(
        "--hand-fixture",
        type=Path,
        default=Path("schemas/examples/cdc-canonical-hand-payload-v1.json"),
    )
    parser.add_argument("--dataset-id", default="prod-cdc-v1")
    parser.add_argument("--dataset-split", default="live")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    record = json.loads(args.cdc_fixture.read_text())
    raw_hand = json.loads(args.hand_fixture.read_text())
    canonical_binary = json.dumps(
        raw_hand, sort_keys=True, separators=(",", ":")
    ).encode()
    row = record["after"]
    if base64.b64decode(row["payload"], validate=True) != canonical_binary:
        raise RuntimeError("CDC fixture bytes differ from the direct hand fixture")
    if hashlib.sha256(canonical_binary).hexdigest() != row["payload_sha256"]:
        raise RuntimeError("CDC fixture checksum differs from the direct hand fixture")

    config = CdcAdapterConfig(
        dataset_id=args.dataset_id,
        dataset_split=args.dataset_split,
        expected_database="poker",
        allowed_tenants=("demo",),
    )
    adapted = adapt_debezium_hand_change(
        record,
        config=config,
        source_position=KafkaSourcePosition(partition=2, offset=41),
    )
    payload = HandCompletedPayload.model_validate(raw_hand)
    direct = build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id=args.dataset_id,
        dataset_split=args.dataset_split,
        occurred_at=datetime.fromisoformat(row["occurred_at"]),
        emitted_at=datetime.fromisoformat(row["emitted_at"]),
        tenant_id=row["tenant_id"],
        product_id=row["product_id"],
    )
    if adapted.event != direct:
        raise RuntimeError("direct and CDC paths produced different canonical events")

    report = {
        "status": "passed",
        "canonical_equivalent": True,
        "source_topic": adapted.source_topic,
        "target_topic": adapted.target_topic,
        "partition_key": adapted.partition_key,
        "event_id": str(adapted.event.event_id),
        "trace_id": str(adapted.event.trace_id),
        "codec_version": row["codec_version"],
        "payload_sha256": row["payload_sha256"],
        "source_lsn": adapted.lineage.source_lsn,
        "source_tx_id": adapted.lineage.source_tx_id,
        "source_position": {
            "topic": adapted.lineage.kafka_topic,
            "partition": adapted.lineage.kafka_partition,
            "offset": adapted.lineage.kafka_offset,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
