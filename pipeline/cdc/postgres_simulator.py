"""Transactional PostgreSQL writer for the local CDC hand simulation."""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping, Protocol

from pipeline.events import HandCompletedPayload

from .simulation_codec import (
    SIMULATION_PROTOBUF_CODEC_VERSION,
    encode_simulation_hand,
    public_hand_payload,
)


DEFAULT_SIMULATION_GAME_TYPES = (
    "NLH_CASH_6MAX",
    "NLH_TOURNAMENT_6MAX",
    "PLAY_MONEY_NLH_6MAX",
    "INTERNAL_TEST_NLH_6MAX",
)
DEFAULT_ALLOWED_GAME_TYPES = (
    "NLH_CASH_6MAX",
    "NLH_TOURNAMENT_6MAX",
)
_GAME_TYPE = re.compile(r"^[A-Z0-9_]+$")


class _Transaction(Protocol):
    def __enter__(self) -> object: ...

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None: ...


class PostgresConnection(Protocol):
    def transaction(self) -> _Transaction: ...

    def execute(self, query: str, parameters: tuple[object, ...]) -> Any: ...


@dataclass(frozen=True)
class SimulationHandInsert:
    history_id: uuid.UUID
    outbox_id: uuid.UUID
    simulation_dataset_id: str
    hand_id: str
    tenant_id: str
    product_id: str
    game_type: str
    payload_schema_version: int
    codec_version: str
    payload_sha256: str
    payload: bytes
    occurred_at: datetime
    emitted_at: datetime
    canonical_payload: HandCompletedPayload


def validate_game_types(game_types: tuple[str, ...]) -> tuple[str, ...]:
    if not game_types:
        raise ValueError("at least one game type is required")
    if len(set(game_types)) != len(game_types):
        raise ValueError("game types must be unique")
    if any(not _GAME_TYPE.fullmatch(value) for value in game_types):
        raise ValueError("game types must match ^[A-Z0-9_]+$")
    return game_types


def build_simulation_insert(
    hand: Mapping[str, Any],
    *,
    game_type: str,
    dataset_id: str,
    tenant_id: str = "demo",
    product_id: str = "poker",
    emitted_at: datetime | None = None,
) -> SimulationHandInsert:
    validate_game_types((game_type,))
    if not dataset_id.startswith("sim-"):
        raise ValueError("simulation dataset ID must start with sim-")
    canonical = public_hand_payload(hand)
    occurred_at = canonical.played_at.astimezone(timezone.utc)
    if emitted_at is None:
        emitted_at = occurred_at + timedelta(milliseconds=1)
    if emitted_at.tzinfo is None or emitted_at.utcoffset() is None:
        raise ValueError("emitted_at must include timezone information")
    emitted_at = emitted_at.astimezone(timezone.utc)
    if emitted_at < occurred_at:
        raise ValueError("emitted_at must not precede occurred_at")
    payload = encode_simulation_hand(canonical, game_type=game_type)
    identity = f"{dataset_id}:{tenant_id}:{product_id}:{canonical.hand_id}"
    return SimulationHandInsert(
        history_id=uuid.uuid5(uuid.NAMESPACE_URL, f"sim-hand-history:{identity}"),
        outbox_id=uuid.uuid5(uuid.NAMESPACE_URL, f"sim-hand-outbox:{identity}"),
        simulation_dataset_id=dataset_id,
        hand_id=canonical.hand_id,
        tenant_id=tenant_id,
        product_id=product_id,
        game_type=game_type,
        payload_schema_version=1,
        codec_version=SIMULATION_PROTOBUF_CODEC_VERSION,
        payload_sha256=hashlib.sha256(payload).hexdigest(),
        payload=payload,
        occurred_at=occurred_at,
        emitted_at=emitted_at,
        canonical_payload=canonical,
    )


class PostgresSimulationSink:
    """Insert source hands; the database trigger owns CDC game filtering."""

    def __init__(self, connection: PostgresConnection) -> None:
        self.connection = connection

    def insert(self, record: SimulationHandInsert) -> bool:
        with self.connection.transaction():
            cursor = self.connection.execute(
                """
                INSERT INTO public.hand_history (
                    id, outbox_id, simulation_dataset_id, hand_id, tenant_id,
                    product_id, game_type, payload_schema_version, codec_version,
                    payload_sha256, payload, occurred_at, emitted_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (tenant_id, product_id, hand_id) DO NOTHING
                """,
                (
                    record.history_id,
                    record.outbox_id,
                    record.simulation_dataset_id,
                    record.hand_id,
                    record.tenant_id,
                    record.product_id,
                    record.game_type,
                    record.payload_schema_version,
                    record.codec_version,
                    record.payload_sha256,
                    record.payload,
                    record.occurred_at,
                    record.emitted_at,
                ),
            )
        return cursor.rowcount == 1


def connect_postgres(dsn: str) -> Any:
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError(
            "PostgreSQL simulation requires psycopg[binary]; run make install"
        ) from exc
    return psycopg.connect(dsn)
