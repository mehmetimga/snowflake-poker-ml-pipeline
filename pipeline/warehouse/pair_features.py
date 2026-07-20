"""Idempotent persistence for versioned pair-feature events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

from pipeline.events import PairFeatureEvent, assert_inference_safe
from pipeline.warehouse.factory import Warehouse
from pipeline.warehouse.sql import delete_by_values


@dataclass(frozen=True)
class PairFeatureIngestRecord:
    envelope: dict[str, Any]
    topic: str | None = None
    partition: int | None = None
    offset: int | None = None
    kafka_timestamp_ms: int | None = None


@dataclass(frozen=True)
class PairFeatureLoadResult:
    events: int = 0
    hands: int = 0
    pairs: int = 0

    def __add__(self, other: "PairFeatureLoadResult") -> "PairFeatureLoadResult":
        return PairFeatureLoadResult(
            events=self.events + other.events,
            hands=self.hands + other.hands,
            pairs=self.pairs + other.pairs,
        )


def _validated(
    records: Iterable[PairFeatureIngestRecord],
) -> list[tuple[PairFeatureIngestRecord, PairFeatureEvent]]:
    unique: dict[str, tuple[PairFeatureIngestRecord, PairFeatureEvent, str]] = {}
    for record in records:
        event = PairFeatureEvent.model_validate(record.envelope)
        assert_inference_safe(event.payload.model_dump(mode="json"))
        canonical = json.dumps(
            event.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        )
        event_id = str(event.event_id)
        previous = unique.get(event_id)
        if previous is not None and previous[2] != canonical:
            raise ValueError(f"event_id collision with different payload: {event_id}")
        unique[event_id] = (record, event, canonical)
    return [(record, event) for record, event, _ in unique.values()]


def load_pair_feature_events(
    warehouse: Warehouse,
    records: Iterable[PairFeatureIngestRecord],
) -> PairFeatureLoadResult:
    validated = _validated(records)
    if not validated:
        return PairFeatureLoadResult()
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
                "hand_id": event.payload.hand_id,
                "table_id": event.payload.table_id,
                "played_at": event.payload.played_at,
                "pair_key": event.payload.pair_key,
                "player_a": event.payload.player_a,
                "player_b": event.payload.player_b,
                "num_players": event.payload.num_players,
                "source_hand_event_id": str(event.payload.source_hand_event_id),
                "source_player_context_event_id_a": str(
                    event.payload.source_player_context_event_id_a
                ),
                "source_player_context_event_id_b": str(
                    event.payload.source_player_context_event_id_b
                ),
                "source_revision_a": event.payload.source_revision_a,
                "source_revision_b": event.payload.source_revision_b,
                "context_status_a": event.payload.context_status_a,
                "context_status_b": event.payload.context_status_b,
                "context_version_a": event.payload.context_version_a,
                "context_version_b": event.payload.context_version_b,
                "snapshot_revision": event.payload.snapshot_revision,
                "feature_definition_version": event.payload.feature_definition_version,
                "payload": json.dumps(
                    event.payload.model_dump(mode="json"),
                    sort_keys=True,
                    separators=(",", ":"),
                ),
                "kafka_topic": record.topic,
                "kafka_partition": record.partition,
                "kafka_offset": record.offset,
                "kafka_timestamp_ms": record.kafka_timestamp_ms,
                "ingested_at": ingested_at,
            }
            for record, event in validated
        ]
    )
    warehouse.execute("BEGIN")
    try:
        if warehouse.kind != "duckdb":
            delete_by_values(
                warehouse,
                "PAIR_FEATURE_EVENTS",
                "event_id",
                frame["event_id"].tolist(),
            )
        warehouse.write_pandas(frame, "PAIR_FEATURE_EVENTS")
        warehouse.execute("COMMIT")
    except Exception:
        warehouse.execute("ROLLBACK")
        raise
    return PairFeatureLoadResult(
        events=len(frame),
        hands=frame["hand_id"].nunique(),
        pairs=frame["pair_key"].nunique(),
    )
