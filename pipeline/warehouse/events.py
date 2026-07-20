"""Idempotent normalization of canonical Kafka envelopes into warehouse tables."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

from pipeline.events import (
    ACCOUNT_LINK_UPDATED,
    HAND_COMPLETED,
    SESSION_STARTED,
    USER_CONTEXT_UPDATED,
    validate_event,
)
from pipeline.warehouse.factory import Warehouse
from pipeline.warehouse.loader import load_hands
from pipeline.warehouse.sql import delete_by_values, sql_string_list


@dataclass(frozen=True)
class IngestRecord:
    envelope: dict[str, Any]
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    kafka_timestamp_ms: int | None = None


@dataclass(frozen=True)
class CanonicalLoadResult:
    events: int = 0
    hands: int = 0
    contexts: int = 0
    sessions: int = 0
    account_links: int = 0
    affected_context_users: int = 0

    def __add__(self, other: "CanonicalLoadResult") -> "CanonicalLoadResult":
        return CanonicalLoadResult(
            events=self.events + other.events,
            hands=self.hands + other.hands,
            contexts=self.contexts + other.contexts,
            sessions=self.sessions + other.sessions,
            account_links=self.account_links + other.account_links,
            affected_context_users=self.affected_context_users
            + other.affected_context_users,
        )


def _deduplicate_records(records: Iterable[IngestRecord]) -> list[tuple[IngestRecord, Any]]:
    unique: dict[str, tuple[IngestRecord, Any, str]] = {}
    for record in records:
        envelope = validate_event(record.envelope)
        event_id = str(envelope.event_id)
        canonical = json.dumps(envelope.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        previous = unique.get(event_id)
        if previous is not None and previous[2] != canonical:
            raise ValueError(f"event_id collision with different payload: {event_id}")
        unique[event_id] = (record, envelope, canonical)
    return [(record, envelope) for record, envelope, _ in unique.values()]


def _audit_frame(validated: list[tuple[IngestRecord, Any]]) -> pd.DataFrame:
    ingested_at = datetime.now(timezone.utc)
    return pd.DataFrame(
        [
            {
                "event_id": str(envelope.event_id),
                "event_type": envelope.event_type,
                "schema_version": envelope.schema_version,
                "tenant_id": envelope.tenant_id,
                "product_id": envelope.product_id,
                "dataset_id": envelope.dataset_id,
                "dataset_split": envelope.dataset_split,
                "occurred_at": envelope.occurred_at,
                "emitted_at": envelope.emitted_at,
                "trace_id": str(envelope.trace_id),
                "kafka_topic": record.topic,
                "kafka_partition": record.partition,
                "kafka_offset": record.offset,
                "kafka_timestamp_ms": record.kafka_timestamp_ms,
                "payload": json.dumps(
                    envelope.payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "ingested_at": ingested_at,
            }
            for record, envelope in validated
        ]
    )


def _context_frame(envelopes: Iterable[Any]) -> pd.DataFrame:
    rows = []
    for envelope in envelopes:
        payload = envelope.payload
        rows.append(
            {
                "event_id": str(envelope.event_id),
                "dataset_id": envelope.dataset_id,
                "dataset_split": envelope.dataset_split,
                "user_id": payload["user_id"],
                "context_version": payload["context_version"],
                "effective_at": payload["effective_at"],
                "account_created_at": payload["account_created_at"],
                "country_bucket": payload["country_bucket"],
                "timezone_name": payload["timezone"],
                "acquisition_channel": payload["acquisition_channel"],
                "kyc_level": payload["kyc_level"],
                "account_status": payload["account_status"],
                "bankroll_bucket": payload["bankroll_bucket"],
                "preferred_stake_bucket": payload["preferred_stake_bucket"],
                "skill_rating": payload["skill_rating"],
                "device_id": payload["device_id"],
                "network_cluster_id": payload["network_cluster_id"],
            }
        )
    return pd.DataFrame(rows)


def _session_frame(envelopes: Iterable[Any]) -> pd.DataFrame:
    rows = []
    for envelope in envelopes:
        payload = envelope.payload
        rows.append(
            {
                "event_id": str(envelope.event_id),
                "dataset_id": envelope.dataset_id,
                "dataset_split": envelope.dataset_split,
                "session_id": payload["session_id"],
                "user_id": payload["user_id"],
                "device_id": payload["device_id"],
                "network_cluster_id": payload["network_cluster_id"],
                "started_at": payload["started_at"],
                "status": payload["status"],
            }
        )
    return pd.DataFrame(rows)


def _account_link_frame(envelopes: Iterable[Any]) -> pd.DataFrame:
    rows = []
    for envelope in envelopes:
        payload = envelope.payload
        rows.append(
            {
                "event_id": str(envelope.event_id),
                "dataset_id": envelope.dataset_id,
                "dataset_split": envelope.dataset_split,
                "link_id": payload["link_id"],
                "user_id": payload["user_id"],
                "related_user_id": payload["related_user_id"],
                "link_type": payload["link_type"],
                "confidence_bucket": payload["confidence_bucket"],
                "link_version": payload["link_version"],
                "effective_at": payload["effective_at"],
            }
        )
    return pd.DataFrame(rows)


def _rebuild_context_history(warehouse: Warehouse, user_ids: Iterable[str]) -> int:
    users = sorted(set(user_ids))
    if not users:
        return 0
    if warehouse.kind != "duckdb":
        delete_by_values(warehouse, "USER_CONTEXT_HISTORY", "user_id", users)
    source = warehouse.fetch_df(
        "SELECT * FROM USER_CONTEXT_EVENTS "
        f"WHERE user_id IN ({sql_string_list(users)}) "
        "ORDER BY user_id, effective_at, context_version, event_id"
    )
    if source.empty:
        return 0
    source.columns = [str(column).lower() for column in source.columns]
    source["effective_at"] = pd.to_datetime(source["effective_at"], utc=True)
    source = source.drop_duplicates(subset=["user_id", "context_version"], keep="last")
    source = source.sort_values(
        ["user_id", "effective_at", "context_version", "event_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    source["effective_to"] = source.groupby("user_id")["effective_at"].shift(-1)
    source["is_current"] = source["effective_to"].isna()
    history = source.rename(
        columns={
            "event_id": "source_event_id",
            "effective_at": "effective_from",
        }
    )[
        [
            "user_id",
            "context_version",
            "source_event_id",
            "dataset_id",
            "dataset_split",
            "effective_from",
            "effective_to",
            "is_current",
            "account_created_at",
            "country_bucket",
            "timezone_name",
            "acquisition_channel",
            "kyc_level",
            "account_status",
            "bankroll_bucket",
            "preferred_stake_bucket",
            "skill_rating",
            "device_id",
            "network_cluster_id",
        ]
    ]
    warehouse.write_pandas(history, "USER_CONTEXT_HISTORY")
    return len(users)


def load_canonical_events(
    warehouse: Warehouse,
    records: Iterable[IngestRecord],
) -> CanonicalLoadResult:
    """Atomically replace replayed event IDs and rebuild affected SCD2 users."""
    validated = _deduplicate_records(records)
    if not validated:
        return CanonicalLoadResult()

    event_ids = [str(envelope.event_id) for _, envelope in validated]
    by_type: dict[str, list[Any]] = {
        HAND_COMPLETED: [],
        USER_CONTEXT_UPDATED: [],
        SESSION_STARTED: [],
        ACCOUNT_LINK_UPDATED: [],
    }
    for _, envelope in validated:
        by_type[envelope.event_type].append(envelope)

    contexts = _context_frame(by_type[USER_CONTEXT_UPDATED])
    sessions = _session_frame(by_type[SESSION_STARTED])
    links = _account_link_frame(by_type[ACCOUNT_LINK_UPDATED])
    hands = [envelope.payload for envelope in by_type[HAND_COMPLETED]]

    warehouse.execute("BEGIN")
    try:
        if warehouse.kind != "duckdb":
            delete_by_values(warehouse, "RAW_EVENT_ENVELOPES", "event_id", event_ids)
        for table, frame in (
            ("USER_CONTEXT_EVENTS", contexts),
            ("USER_SESSION_EVENTS", sessions),
            ("ACCOUNT_LINK_EVENTS", links),
        ):
            if not frame.empty and warehouse.kind != "duckdb":
                delete_by_values(warehouse, table, "event_id", frame["event_id"])
        warehouse.write_pandas(_audit_frame(validated), "RAW_EVENT_ENVELOPES")
        warehouse.write_pandas(contexts, "USER_CONTEXT_EVENTS")
        warehouse.write_pandas(sessions, "USER_SESSION_EVENTS")
        warehouse.write_pandas(links, "ACCOUNT_LINK_EVENTS")
        loaded_hands = load_hands(warehouse, hands)
        affected_users = _rebuild_context_history(
            warehouse,
            contexts["user_id"].tolist() if not contexts.empty else [],
        )
        warehouse.execute("COMMIT")
    except Exception:
        warehouse.execute("ROLLBACK")
        raise

    return CanonicalLoadResult(
        events=len(validated),
        hands=loaded_hands,
        contexts=len(contexts),
        sessions=len(sessions),
        account_links=len(links),
        affected_context_users=affected_users,
    )
