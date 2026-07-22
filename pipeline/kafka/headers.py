"""Shared Kafka headers for canonical events.

Keeping these in one place lets direct publishers and CDC adapters prove that
their canonical transport contract is identical.  Source-specific CDC lineage
is added separately and is never inserted into the canonical event value.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pipeline.events import EventEnvelope, validate_event


KafkaHeaders = tuple[tuple[str, bytes], ...]


def canonical_event_headers(
    event: Mapping[str, Any] | EventEnvelope,
) -> KafkaHeaders:
    """Return the stable headers shared by every canonical publisher."""

    envelope = validate_event(event)
    return (
        ("event_id", str(envelope.event_id).encode("utf-8")),
        ("event_type", envelope.event_type.encode("utf-8")),
        ("schema_version", str(envelope.schema_version).encode("ascii")),
        ("dataset_id", envelope.dataset_id.encode("utf-8")),
        ("trace_id", str(envelope.trace_id).encode("utf-8")),
        ("occurred_at", envelope.occurred_at.isoformat().encode("utf-8")),
    )
