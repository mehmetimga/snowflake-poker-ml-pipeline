from dataclasses import dataclass

import pytest

from pipeline.events import remember_deterministic_event


@dataclass(frozen=True)
class _Event:
    payload: str


def test_deterministic_event_accepts_first_record_and_exact_retry() -> None:
    events: dict[str, _Event] = {}
    event = _Event(payload="same")

    assert remember_deterministic_event(
        events, "event-1", event, source="test-topic"
    )
    assert not remember_deterministic_event(
        events, "event-1", event, source="test-topic"
    )
    assert events == {"event-1": event}


def test_deterministic_event_rejects_same_id_with_different_payload() -> None:
    events = {"event-1": _Event(payload="first")}

    with pytest.raises(
        ValueError,
        match=r"event_id collision in test-topic: event-1",
    ):
        remember_deterministic_event(
            events,
            "event-1",
            _Event(payload="second"),
            source="test-topic",
        )
