"""Idempotent rule-evidence persistence and score/model lineage."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

import pandas as pd

from pipeline.events import RiskScoreEvent, RuleEvidenceEvent
from pipeline.warehouse.factory import Warehouse
from pipeline.warehouse.sql import (
    delete_by_values,
    sql_string_list,
    sql_string_literal,
)


@dataclass(frozen=True)
class RuleEvidenceIngestRecord:
    envelope: Mapping[str, Any]
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    kafka_timestamp_ms: int | None = None


@dataclass(frozen=True)
class RuleEvidenceLoadResult:
    events: int = 0
    hands: int = 0
    entities: int = 0


@dataclass(frozen=True)
class RuleEvidenceReferenceLoadResult:
    scores: int = 0
    references: int = 0
    model_runs: int = 0


def _validated_rule_events(
    records: Iterable[RuleEvidenceIngestRecord],
) -> list[tuple[RuleEvidenceIngestRecord, RuleEvidenceEvent, str]]:
    unique: dict[str, tuple[RuleEvidenceIngestRecord, RuleEvidenceEvent, str]] = {}
    for record in records:
        event = RuleEvidenceEvent.model_validate(record.envelope)
        canonical = json.dumps(
            event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        event_id = str(event.event_id)
        previous = unique.get(event_id)
        if previous is not None and previous[2] != canonical:
            raise ValueError(f"event_id collision with different payload: {event_id}")
        unique[event_id] = (record, event, canonical)
    return [
        (record, event, hashlib.sha256(canonical.encode()).hexdigest())
        for record, event, canonical in unique.values()
    ]


def _reject_persisted_collisions(
    warehouse: Warehouse,
    validated: list[tuple[RuleEvidenceIngestRecord, RuleEvidenceEvent, str]],
) -> None:
    event_ids = [str(event.event_id) for _, event, _ in validated]
    existing = warehouse.fetch_df(
        "SELECT event_id, event_sha256 FROM RULE_EVIDENCE_EVENTS "
        f"WHERE event_id IN ({sql_string_list(event_ids)})"
    )
    if existing.empty:
        return
    existing.columns = [str(column).lower() for column in existing.columns]
    hashes = dict(zip(existing["event_id"], existing["event_sha256"], strict=True))
    for _, event, event_hash in validated:
        previous = hashes.get(str(event.event_id))
        if previous is not None and previous != event_hash:
            raise ValueError(
                f"event_id collision with persisted payload: {event.event_id}"
            )


def load_rule_evidence_events(
    warehouse: Warehouse,
    records: Iterable[RuleEvidenceIngestRecord],
) -> RuleEvidenceLoadResult:
    """Validate and idempotently persist rule observations."""

    validated = _validated_rule_events(records)
    if not validated:
        return RuleEvidenceLoadResult()
    _reject_persisted_collisions(warehouse, validated)
    ingested_at = datetime.now(timezone.utc)
    frame = pd.DataFrame(
        [
            {
                "event_id": str(event.event_id),
                "event_type": event.event_type,
                "schema_version": event.schema_version,
                "tenant_id": event.tenant_id,
                "product_id": event.product_id,
                "dataset_id": event.dataset_id,
                "dataset_split": event.dataset_split,
                "occurred_at": event.occurred_at,
                "emitted_at": event.emitted_at,
                "trace_id": str(event.trace_id),
                "rule_event_id": str(event.payload.rule_event_id),
                "rule_id": event.payload.rule_id,
                "rule_version": event.payload.rule_version,
                "rule_owner": event.payload.rule_owner,
                "entity_type": event.payload.entity_type,
                "entity_key": event.payload.entity_key,
                "hand_id": event.payload.hand_id,
                "observation_revision": event.payload.observation_revision,
                "severity": event.payload.severity,
                "raw_score": event.payload.raw_score,
                "evidence": json.dumps(
                    event.payload.evidence, sort_keys=True, separators=(",", ":")
                ),
                "effective_at": event.payload.effective_at,
                "feature_definition_version": (
                    event.payload.feature_definition_version
                ),
                "payload": json.dumps(
                    event.payload.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "event_sha256": event_hash,
                "kafka_topic": record.topic,
                "kafka_partition": record.partition,
                "kafka_offset": record.offset,
                "kafka_timestamp_ms": record.kafka_timestamp_ms,
                "ingested_at": ingested_at,
            }
            for record, event, event_hash in validated
        ]
    )
    warehouse.execute("BEGIN")
    try:
        if warehouse.kind != "duckdb":
            delete_by_values(
                warehouse, "RULE_EVIDENCE_EVENTS", "event_id", frame["event_id"]
            )
        warehouse.write_pandas(frame, "RULE_EVIDENCE_EVENTS")
        warehouse.execute("COMMIT")
    except Exception:
        warehouse.execute("ROLLBACK")
        raise
    return RuleEvidenceLoadResult(
        events=len(frame),
        hands=frame["hand_id"].nunique(),
        entities=frame[["entity_type", "entity_key"]].drop_duplicates().shape[0],
    )


def _validated_scores(
    events: Iterable[Mapping[str, Any] | RiskScoreEvent],
) -> list[RiskScoreEvent]:
    unique: dict[str, tuple[RiskScoreEvent, str]] = {}
    for value in events:
        event = value if isinstance(value, RiskScoreEvent) else RiskScoreEvent.model_validate(value)
        canonical = json.dumps(
            event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        event_id = str(event.event_id)
        previous = unique.get(event_id)
        if previous is not None and previous[1] != canonical:
            raise ValueError(f"risk score event_id collision: {event_id}")
        unique[event_id] = (event, canonical)
    return [event for event, _ in unique.values()]


def load_risk_score_rule_references(
    warehouse: Warehouse,
    events: Iterable[Mapping[str, Any] | RiskScoreEvent],
) -> RuleEvidenceReferenceLoadResult:
    """Persist the many-to-many link that provides model-run lineage."""

    scores = _validated_scores(events)
    if not scores:
        return RuleEvidenceReferenceLoadResult()
    score_event_ids = [str(event.event_id) for event in scores]
    rows = [
        {
            "tenant_id": event.tenant_id,
            "product_id": event.product_id,
            "risk_score_event_id": str(event.event_id),
            "model_run_id": event.payload.model_run_id,
            "rule_event_id": str(rule_event_id),
            "hand_id": event.payload.hand_id,
            "referenced_at": event.emitted_at,
        }
        for event in scores
        for rule_event_id in event.payload.rule_evidence_event_ids
    ]
    frame = pd.DataFrame(rows)
    warehouse.execute("BEGIN")
    try:
        if warehouse.kind != "duckdb":
            delete_by_values(
                warehouse,
                "RISK_SCORE_RULE_EVIDENCE",
                "risk_score_event_id",
                score_event_ids,
            )
        else:
            for event in scores:
                event_id = sql_string_literal(event.event_id)
                references = [
                    str(value) for value in event.payload.rule_evidence_event_ids
                ]
                predicate = (
                    f" AND rule_event_id NOT IN ({sql_string_list(references)})"
                    if references
                    else ""
                )
                warehouse.execute(
                    "DELETE FROM RISK_SCORE_RULE_EVIDENCE "
                    f"WHERE risk_score_event_id = {event_id}{predicate}"
                )
        if not frame.empty:
            warehouse.write_pandas(frame, "RISK_SCORE_RULE_EVIDENCE")
        warehouse.execute("COMMIT")
    except Exception:
        warehouse.execute("ROLLBACK")
        raise
    return RuleEvidenceReferenceLoadResult(
        scores=len(scores),
        references=len(frame),
        model_runs=len({event.payload.model_run_id for event in scores}),
    )
