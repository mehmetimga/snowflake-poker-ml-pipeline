"""Reusable invariants for deterministic event streams."""

from __future__ import annotations

from collections.abc import MutableMapping
from typing import TypeVar


EventT = TypeVar("EventT")


def remember_deterministic_event(
    events: MutableMapping[str, EventT],
    event_id: object,
    event: EventT,
    *,
    source: str,
) -> bool:
    """Store one event ID, allowing exact retries but rejecting collisions.

    Returns ``True`` for the first observation and ``False`` for an exact
    at-least-once retry.
    """

    identity = str(event_id)
    previous = events.get(identity)
    if previous is None:
        events[identity] = event
        return True
    if previous != event:
        raise ValueError(f"event_id collision in {source}: {identity}")
    return False
