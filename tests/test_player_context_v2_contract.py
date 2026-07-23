import copy
import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from pipeline.events import (
    PLAYER_HAND_CONTEXT_V2_TOPIC,
    PlayerHandContextEventV2,
)


EXAMPLE = Path("schemas/examples/poker.hand-player-context.v2.json")
SCHEMA = Path("schemas/events/poker.hand-player-context.v2.schema.json")


def _example() -> dict[str, object]:
    return json.loads(EXAMPLE.read_text())


def test_v2_example_matches_pydantic_and_json_schema() -> None:
    value = _example()

    event = PlayerHandContextEventV2.model_validate(value)
    Draft202012Validator(
        json.loads(SCHEMA.read_text()),
        format_checker=FormatChecker(),
    ).validate(value)

    assert event.schema_version == 2
    assert event.payload.context_resolution.source == "postgresql"
    assert PLAYER_HAND_CONTEXT_V2_TOPIC == "poker.hand-player-context.v2"


def test_v2_rejects_legacy_temporal_join_fields() -> None:
    value = _example()
    value["payload"]["join_policy_version"] = "event-time-user-context-v1"

    with pytest.raises(ValidationError):
        PlayerHandContextEventV2.model_validate(value)


def test_v2_resolution_must_match_context_snapshot() -> None:
    value = copy.deepcopy(_example())
    value["payload"]["context_resolution"]["context_version"] = 2

    with pytest.raises(ValidationError, match="context_version"):
        PlayerHandContextEventV2.model_validate(value)
