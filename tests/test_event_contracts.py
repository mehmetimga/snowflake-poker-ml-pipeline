from __future__ import annotations

from datetime import datetime, timezone

import pytest

from pipeline.events import (
    HAND_COMPLETED,
    HandCompletedPayload,
    assert_inference_safe,
    build_event,
    event_partition_key,
    validate_event,
)
from pipeline.generator import GeneratorConfig, HandGenerator
from pipeline.generator.dataset import separate_hand_labels


def _hand_event():
    raw_hand = next(
        HandGenerator(
            GeneratorConfig(
                n_hands=1,
                n_players=12,
                n_tables=2,
                n_colluding_pairs=3,
                seed=301,
                dataset_split="train",
            )
        ).iter_hands()
    )
    safe_hand, _ = separate_hand_labels(raw_hand)
    payload = HandCompletedPayload.model_validate(safe_hand)
    return build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id="contract-test-v1",
        dataset_split="train",
        occurred_at=payload.played_at,
    )


def test_hand_contract_round_trips_and_uses_table_partition_key():
    event = _hand_event()
    serialized = event.model_dump(mode="json")

    assert validate_event(serialized) == event
    assert event_partition_key(event) == event.payload["table_id"]
    assert event.occurred_at == datetime.fromisoformat(event.payload["played_at"])
    assert event.occurred_at.tzinfo == timezone.utc


def test_event_identifier_is_stable_for_replay():
    assert _hand_event().event_id == _hand_event().event_id
    assert _hand_event().trace_id == _hand_event().trace_id


def test_inference_contract_rejects_private_truth_at_any_depth():
    event = _hand_event().model_dump(mode="json")
    event["payload"]["players"][0]["is_suspicious"] = True

    with pytest.raises(ValueError, match="private label field"):
        validate_event(event)

    with pytest.raises(ValueError, match="private label field"):
        assert_inference_safe({"nested": [{"collusion_pair_id": "private"}]})
