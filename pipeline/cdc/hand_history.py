"""Translate a versioned Debezium outbox record into a canonical hand event.

The real poker-server binary codec is intentionally not implemented here: its
format is owned by the external poker platform and has not been supplied.  A
small JSON codec is included only for contract fixtures.  Production adds a
decoder under a new explicit ``codec_version`` without changing the canonical
event consumed by Flink, Snowflake, or the scorer.
"""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from pipeline.events import (
    HAND_COMPLETED,
    EventEnvelope,
    HandCompletedPayload,
    build_event,
    event_partition_key,
)
from pipeline.events.contracts import TOPIC_BY_EVENT_TYPE
from pipeline.kafka.headers import KafkaHeaders, canonical_event_headers


CDC_HAND_OUTBOX_TOPIC = "cdc.poker.hand-outbox.v1"
FIXTURE_CODEC_VERSION = "canonical-hand-json-v1"


class CdcRecordRejected(ValueError):
    """A poison or unsupported CDC record that must not reach canonical Kafka."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class _DebeziumModel(BaseModel):
    # Connector versions can add unrelated metadata.  The adapter validates
    # every field that participates in identity or lineage and ignores others.
    model_config = ConfigDict(extra="ignore", frozen=True, populate_by_name=True)


class DebeziumSource(_DebeziumModel):
    version: str = Field(min_length=1)
    connector: Literal["postgresql"]
    name: str = Field(min_length=1)
    ts_ms: int = Field(ge=0)
    snapshot: bool | str | None
    db: str = Field(min_length=1)
    schema_name: str = Field(alias="schema", min_length=1)
    table: str = Field(min_length=1)
    tx_id: int | None = Field(default=None, alias="txId", ge=0)
    lsn: int = Field(ge=0)

    @field_validator("snapshot")
    @classmethod
    def validate_snapshot(cls, value: bool | str | None) -> bool | str | None:
        allowed = {"true", "false", "initial", "first", "last", "incremental"}
        if isinstance(value, str) and value.lower() not in allowed:
            raise ValueError(f"unsupported Debezium snapshot marker: {value!r}")
        return value.lower() if isinstance(value, str) else value


class DebeziumTransaction(_DebeziumModel):
    id: str = Field(min_length=1)
    total_order: str = Field(min_length=1)
    data_collection_order: str = Field(min_length=1)


class HandCompletedOutboxRow(_StrictModel):
    """Proposed v1 row written atomically with the immutable hand history."""

    id: uuid.UUID
    aggregate_type: Literal["poker-hand"]
    aggregate_id: str = Field(min_length=1)
    event_type: Literal["poker.hand.completed"]
    payload_schema_version: Literal[1]
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    codec_version: str = Field(min_length=1)
    payload_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    payload: str = Field(min_length=1, description="Base64 from PostgreSQL BYTEA")

    @field_validator("occurred_at", "emitted_at")
    @classmethod
    def require_timezone(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("outbox timestamps must include timezone information")
        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_causality(self) -> "HandCompletedOutboxRow":
        if self.emitted_at < self.occurred_at:
            raise ValueError("emitted_at must not precede occurred_at")
        return self


class DebeziumHandChange(_DebeziumModel):
    before: dict[str, Any] | None
    after: HandCompletedOutboxRow | None
    source: DebeziumSource
    op: Literal["c", "r", "u", "d"]
    ts_ms: int = Field(ge=0)
    ts_us: int | None = Field(default=None, ge=0)
    ts_ns: int | None = Field(default=None, ge=0)
    transaction: DebeziumTransaction | None = None


class CdcAdapterConfig(_StrictModel):
    """Deployment-owned values; the poker server does not choose ML splits."""

    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(default="live", min_length=1)
    expected_database: str | None = None
    expected_schema: str = Field(default="public", min_length=1)
    expected_table: str = Field(default="hand_completed_outbox", min_length=1)
    allowed_tenants: tuple[str, ...] = ()


class KafkaSourcePosition(_StrictModel):
    topic: str = Field(default=CDC_HAND_OUTBOX_TOPIC, min_length=1)
    partition: int = Field(ge=0)
    offset: int = Field(ge=0)


class CdcLineage(_StrictModel):
    connector: Literal["postgresql"]
    connector_name: str
    database: str
    schema_name: str
    table: str
    source_lsn: int
    source_tx_id: int | None
    transaction_id: str | None
    transaction_total_order: str | None
    transaction_collection_order: str | None
    operation: Literal["c", "r"]
    snapshot: str
    source_ts_ms: int
    connector_ts_ms: int
    outbox_id: uuid.UUID
    payload_sha256: str
    kafka_topic: str | None = None
    kafka_partition: int | None = None
    kafka_offset: int | None = None


class HandHistoryDecoder(Protocol):
    """Versioned seam implemented later for the poker-server binary format."""

    codec_version: str

    def decode(
        self,
        payload: bytes,
        *,
        row: HandCompletedOutboxRow,
        config: CdcAdapterConfig,
    ) -> HandCompletedPayload: ...


class CanonicalHandJsonV1Decoder:
    """Fixture-only codec; this is not the future poker-server binary decoder."""

    codec_version = FIXTURE_CODEC_VERSION

    def decode(
        self,
        payload: bytes,
        *,
        row: HandCompletedOutboxRow,
        config: CdcAdapterConfig,
    ) -> HandCompletedPayload:
        del row, config
        try:
            value = json.loads(payload.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CdcRecordRejected(
                "invalid_binary_payload",
                "fixture codec payload is not a UTF-8 JSON object",
            ) from exc
        if not isinstance(value, dict):
            raise CdcRecordRejected(
                "invalid_binary_payload", "decoded hand payload must be an object"
            )
        try:
            return HandCompletedPayload.model_validate(value)
        except ValidationError as exc:
            raise CdcRecordRejected(
                "invalid_canonical_payload", "decoded hand violates canonical v1"
            ) from exc


@dataclass(frozen=True)
class CdcAdaptedHand:
    event: EventEnvelope
    lineage: CdcLineage
    source_topic: str = CDC_HAND_OUTBOX_TOPIC

    @property
    def target_topic(self) -> str:
        return TOPIC_BY_EVENT_TYPE[self.event.event_type]

    @property
    def partition_key(self) -> str:
        return event_partition_key(self.event)

    @property
    def canonical_headers(self) -> KafkaHeaders:
        return canonical_event_headers(self.event)

    @property
    def kafka_headers(self) -> KafkaHeaders:
        return self.canonical_headers + cdc_lineage_headers(self.lineage)


def _snapshot_text(value: bool | str | None) -> str:
    if value is None:
        return "false"
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def cdc_lineage_headers(lineage: CdcLineage) -> KafkaHeaders:
    """Serialize audit lineage without changing the canonical event value."""

    values: tuple[tuple[str, object | None], ...] = (
        ("cdc_connector", lineage.connector),
        ("cdc_connector_name", lineage.connector_name),
        ("cdc_database", lineage.database),
        ("cdc_schema", lineage.schema_name),
        ("cdc_table", lineage.table),
        ("cdc_source_lsn", lineage.source_lsn),
        ("cdc_source_tx_id", lineage.source_tx_id),
        ("cdc_transaction_id", lineage.transaction_id),
        ("cdc_transaction_total_order", lineage.transaction_total_order),
        (
            "cdc_transaction_collection_order",
            lineage.transaction_collection_order,
        ),
        ("cdc_operation", lineage.operation),
        ("cdc_snapshot", lineage.snapshot),
        ("cdc_source_ts_ms", lineage.source_ts_ms),
        ("cdc_connector_ts_ms", lineage.connector_ts_ms),
        ("cdc_outbox_id", lineage.outbox_id),
        ("cdc_payload_sha256", lineage.payload_sha256),
        ("cdc_source_topic", lineage.kafka_topic),
        ("cdc_source_partition", lineage.kafka_partition),
        ("cdc_source_offset", lineage.kafka_offset),
    )
    return tuple(
        (key, str(value).encode("utf-8"))
        for key, value in values
        if value is not None
    )


def _unwrap_connect_json(value: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if value is None:
        raise CdcRecordRejected("tombstone", "Kafka tombstones are not hand events")
    if "schema" in value and "payload" in value:
        payload = value["payload"]
        if payload is None:
            raise CdcRecordRejected(
                "tombstone", "schema-wrapped Kafka tombstone is not a hand event"
            )
        if not isinstance(payload, Mapping):
            raise CdcRecordRejected(
                "invalid_envelope", "Kafka Connect payload must be an object"
            )
        return payload
    return value


def _decode_binary(row: HandCompletedOutboxRow) -> bytes:
    try:
        payload = base64.b64decode(row.payload, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise CdcRecordRejected(
            "invalid_base64", "outbox payload is not strict base64"
        ) from exc
    digest = hashlib.sha256(payload).hexdigest()
    if digest != row.payload_sha256:
        raise CdcRecordRejected(
            "checksum_mismatch",
            f"payload SHA-256 {digest} does not match {row.payload_sha256}",
        )
    return payload


def _validate_source(
    change: DebeziumHandChange,
    config: CdcAdapterConfig,
) -> None:
    source = change.source
    expected = (config.expected_schema, config.expected_table)
    actual = (source.schema_name, source.table)
    if actual != expected:
        raise CdcRecordRejected(
            "unexpected_source_table", f"expected {expected!r}, received {actual!r}"
        )
    if config.expected_database is not None and source.db != config.expected_database:
        raise CdcRecordRejected(
            "unexpected_source_database",
            f"expected {config.expected_database!r}, received {source.db!r}",
        )


def adapt_debezium_hand_change(
    value: Mapping[str, Any] | None,
    *,
    config: CdcAdapterConfig,
    decoders: Mapping[str, HandHistoryDecoder] | None = None,
    source_position: KafkaSourcePosition | None = None,
) -> CdcAdaptedHand:
    """Validate one immutable CDC record and produce its canonical hand event."""

    try:
        change = DebeziumHandChange.model_validate(_unwrap_connect_json(value))
    except CdcRecordRejected:
        raise
    except ValidationError as exc:
        raise CdcRecordRejected(
            "invalid_envelope", "record violates the Debezium hand-outbox v1 contract"
        ) from exc

    if change.op not in ("c", "r"):
        raise CdcRecordRejected(
            "immutable_outbox_operation",
            f"operation {change.op!r} is forbidden; hand-completed outbox rows are insert-only",
        )
    if change.before is not None:
        raise CdcRecordRejected(
            "invalid_before_image", "create/snapshot records must not carry a before row"
        )
    if change.after is None:
        raise CdcRecordRejected("missing_after_image", "record has no completed outbox row")
    if change.op == "r" and _snapshot_text(change.source.snapshot) == "false":
        raise CdcRecordRejected(
            "invalid_snapshot_marker", "snapshot read operation requires snapshot lineage"
        )
    if change.op == "c" and change.source.tx_id is None:
        raise CdcRecordRejected(
            "missing_transaction_lineage", "live creates require PostgreSQL txId"
        )

    _validate_source(change, config)
    row = change.after
    if config.allowed_tenants and row.tenant_id not in config.allowed_tenants:
        raise CdcRecordRejected(
            "tenant_not_allowed", f"tenant {row.tenant_id!r} is not allowlisted"
        )

    raw_payload = _decode_binary(row)
    decoder_map: Mapping[str, HandHistoryDecoder] = (
        {FIXTURE_CODEC_VERSION: CanonicalHandJsonV1Decoder()}
        if decoders is None
        else decoders
    )
    decoder = decoder_map.get(row.codec_version)
    if decoder is None:
        raise CdcRecordRejected(
            "unknown_codec_version", f"no decoder registered for {row.codec_version!r}"
        )
    if decoder.codec_version != row.codec_version:
        raise CdcRecordRejected(
            "decoder_identity_mismatch",
            f"decoder declares {decoder.codec_version!r}, row declares {row.codec_version!r}",
        )
    payload = decoder.decode(raw_payload, row=row, config=config)

    if payload.hand_id != row.aggregate_id:
        raise CdcRecordRejected(
            "aggregate_identity_mismatch",
            f"row aggregate_id {row.aggregate_id!r} != hand_id {payload.hand_id!r}",
        )
    if payload.dataset_split != config.dataset_split:
        raise CdcRecordRejected(
            "dataset_split_mismatch",
            f"payload split {payload.dataset_split!r} != adapter split {config.dataset_split!r}",
        )
    if payload.played_at.astimezone(timezone.utc) != row.occurred_at:
        raise CdcRecordRejected(
            "event_time_mismatch", "payload played_at must equal outbox occurred_at"
        )

    event = build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=row.aggregate_id,
        payload=payload,
        dataset_id=config.dataset_id,
        dataset_split=config.dataset_split,
        occurred_at=row.occurred_at,
        emitted_at=row.emitted_at,
        tenant_id=row.tenant_id,
        product_id=row.product_id,
    )
    transaction = change.transaction
    lineage = CdcLineage(
        connector=change.source.connector,
        connector_name=change.source.name,
        database=change.source.db,
        schema_name=change.source.schema_name,
        table=change.source.table,
        source_lsn=change.source.lsn,
        source_tx_id=change.source.tx_id,
        transaction_id=transaction.id if transaction else None,
        transaction_total_order=transaction.total_order if transaction else None,
        transaction_collection_order=(
            transaction.data_collection_order if transaction else None
        ),
        operation=change.op,
        snapshot=_snapshot_text(change.source.snapshot),
        source_ts_ms=change.source.ts_ms,
        connector_ts_ms=change.ts_ms,
        outbox_id=row.id,
        payload_sha256=row.payload_sha256,
        kafka_topic=source_position.topic if source_position else None,
        kafka_partition=source_position.partition if source_position else None,
        kafka_offset=source_position.offset if source_position else None,
    )
    return CdcAdaptedHand(event=event, lineage=lineage)
