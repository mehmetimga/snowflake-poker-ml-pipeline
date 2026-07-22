"""Deterministic poison and acceptance scenarios for the local CDC boundary."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, replace
from typing import Any, Literal, Mapping, Sequence

from .postgres_simulator import SimulationHandInsert, build_simulation_insert
from .simulation_codec import encode_simulation_hand


ScenarioOutcome = Literal["canonical", "dead_letter", "filtered"]
ScenarioMutation = Literal[
    "none",
    "checksum_mismatch",
    "malformed_protobuf",
    "game_type_mismatch",
    "unknown_codec_version",
]


@dataclass(frozen=True)
class SimulationScenarioDefinition:
    name: str
    game_type: str
    mutation: ScenarioMutation
    expected_outcome: ScenarioOutcome
    expected_error_code: str | None = None


@dataclass(frozen=True)
class SimulationScenarioRecord:
    definition: SimulationScenarioDefinition
    record: SimulationHandInsert


FAULT_SCENARIOS = (
    SimulationScenarioDefinition(
        name="valid_cash",
        game_type="NLH_CASH_6MAX",
        mutation="none",
        expected_outcome="canonical",
    ),
    SimulationScenarioDefinition(
        name="filtered_play_money",
        game_type="PLAY_MONEY_NLH_6MAX",
        mutation="none",
        expected_outcome="filtered",
    ),
    SimulationScenarioDefinition(
        name="checksum_mismatch",
        game_type="NLH_CASH_6MAX",
        mutation="checksum_mismatch",
        expected_outcome="dead_letter",
        expected_error_code="checksum_mismatch",
    ),
    SimulationScenarioDefinition(
        name="malformed_protobuf",
        game_type="NLH_CASH_6MAX",
        mutation="malformed_protobuf",
        expected_outcome="dead_letter",
        expected_error_code="invalid_binary_payload",
    ),
    SimulationScenarioDefinition(
        name="game_type_mismatch",
        game_type="NLH_CASH_6MAX",
        mutation="game_type_mismatch",
        expected_outcome="dead_letter",
        expected_error_code="game_type_mismatch",
    ),
    SimulationScenarioDefinition(
        name="unknown_codec_version",
        game_type="NLH_TOURNAMENT_6MAX",
        mutation="unknown_codec_version",
        expected_outcome="dead_letter",
        expected_error_code="unknown_codec_version",
    ),
)


def build_fault_scenario_records(
    hands: Sequence[Mapping[str, Any]],
    *,
    dataset_id: str,
    tenant_id: str = "demo",
    product_id: str = "poker",
) -> tuple[SimulationScenarioRecord, ...]:
    if len(hands) != len(FAULT_SCENARIOS):
        raise ValueError(f"fault suite requires exactly {len(FAULT_SCENARIOS)} hands")
    results: list[SimulationScenarioRecord] = []
    for hand, definition in zip(hands, FAULT_SCENARIOS, strict=True):
        record = build_simulation_insert(
            hand,
            game_type=definition.game_type,
            dataset_id=dataset_id,
            tenant_id=tenant_id,
            product_id=product_id,
            simulation_scenario=definition.name,
        )
        if definition.mutation == "checksum_mismatch":
            record = replace(record, payload_sha256="0" * 64)
        elif definition.mutation == "malformed_protobuf":
            payload = b"\x80"
            record = replace(
                record,
                payload=payload,
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            )
        elif definition.mutation == "game_type_mismatch":
            payload = encode_simulation_hand(
                record.canonical_payload,
                game_type="NLH_TOURNAMENT_6MAX",
            )
            record = replace(
                record,
                payload=payload,
                payload_sha256=hashlib.sha256(payload).hexdigest(),
            )
        elif definition.mutation == "unknown_codec_version":
            record = replace(record, codec_version="poker-hand-protobuf-v999")
        results.append(SimulationScenarioRecord(definition=definition, record=record))
    return tuple(results)
