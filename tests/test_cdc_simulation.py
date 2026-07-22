from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.cdc import CdcAdapterConfig, CdcRecordRejected, adapt_debezium_hand_change
from pipeline.cdc.postgres_simulator import (
    PostgresSimulationSink,
    build_simulation_insert,
)
from pipeline.cdc.simulation_codec import (
    SIMULATION_PROTOBUF_CODEC_VERSION,
    SimulationProtobufV1Decoder,
    encode_simulation_hand,
    public_hand_payload,
)
from pipeline.cdc.simulation_scenarios import (
    FAULT_SCENARIOS,
    build_fault_scenario_records,
)
from pipeline.events import HandCompletedPayload
from pipeline.generator import GeneratorConfig, HandGenerator


ROOT = Path(__file__).resolve().parents[1]
PROTOBUF_FIXTURE = ROOT / "schemas/examples/poker-hand-protobuf-v1.base64"
DEBEZIUM_FIXTURE = ROOT / "schemas/examples/debezium.hand-completed-outbox.v1.json"
SQL = ROOT / "infra/simulation/postgres/init/001_cdc_simulation.sql"
SCENARIO_MIGRATION = ROOT / "infra/simulation/postgres/init/002_simulation_scenario.sql"
CONNECTOR = ROOT / "infra/simulation/debezium/poker-hand-outbox-connector.json"


def _generated_hand() -> dict:
    return next(
        HandGenerator(
            GeneratorConfig(
                n_hands=1,
                n_players=12,
                n_tables=1,
                n_colluding_pairs=2,
                seed=90210,
                dataset_split="live",
                dataset_id="sim-cdc-v1",
            ),
            start_at=datetime(2026, 7, 22, tzinfo=timezone.utc),
        ).iter_hands()
    )


def test_simulation_protobuf_is_deterministic_and_removes_private_truth() -> None:
    hand = _generated_hand()
    assert any("is_suspicious" in player for player in hand["players"])
    canonical = public_hand_payload(hand)
    first = encode_simulation_hand(canonical, game_type="NLH_CASH_6MAX")
    second = encode_simulation_hand(canonical, game_type="NLH_CASH_6MAX")

    assert first == second
    assert b"is_suspicious" not in first
    assert b"collusion_pair_id" not in first

    row = json.loads(DEBEZIUM_FIXTURE.read_text())["after"]
    row.update(
        {
            "aggregate_id": canonical.hand_id,
            "game_type": "NLH_CASH_6MAX",
            "occurred_at": canonical.played_at.isoformat(),
            "codec_version": SIMULATION_PROTOBUF_CODEC_VERSION,
        }
    )
    decoded = SimulationProtobufV1Decoder().decode(
        first,
        row=type("Row", (), row)(),
        config=CdcAdapterConfig(dataset_id="sim-cdc-v1"),
    )
    assert decoded == canonical


def test_shared_protobuf_fixture_matches_python_encoder_and_cdc_adapter() -> None:
    hand = json.loads(
        (ROOT / "schemas/examples/cdc-canonical-hand-payload-v1.json").read_text()
    )
    hand["generator"] = "pokerkit"
    canonical = public_hand_payload(hand)
    payload = encode_simulation_hand(canonical, game_type="NLH_CASH_6MAX")
    assert base64.b64encode(payload).decode() == PROTOBUF_FIXTURE.read_text().strip()
    assert hashlib.sha256(payload).hexdigest() == (
        "bc2eef1b6c3571e178c8c50e13663a82e1687de7c40b0ddbeb54b28c3be7b7a4"
    )

    envelope = json.loads(DEBEZIUM_FIXTURE.read_text())
    row = envelope["after"]
    row["codec_version"] = SIMULATION_PROTOBUF_CODEC_VERSION
    row["payload"] = base64.b64encode(payload).decode()
    row["payload_sha256"] = hashlib.sha256(payload).hexdigest()
    adapted = adapt_debezium_hand_change(
        envelope,
        config=CdcAdapterConfig(
            dataset_id="prod-cdc-v1",
            dataset_split="live",
            expected_database="poker",
            allowed_tenants=("demo",),
            allowed_game_types=("NLH_CASH_6MAX",),
        ),
        decoders={SIMULATION_PROTOBUF_CODEC_VERSION: SimulationProtobufV1Decoder()},
    )
    assert HandCompletedPayload.model_validate(adapted.event.payload) == canonical
    assert dict(adapted.kafka_headers)["cdc_game_type"] == b"NLH_CASH_6MAX"

    envelope["after"]["game_type"] = "PLAY_MONEY_NLH_6MAX"
    with pytest.raises(CdcRecordRejected, match="game_type_not_allowed"):
        adapt_debezium_hand_change(
            envelope,
            config=CdcAdapterConfig(
                dataset_id="prod-cdc-v1",
                allowed_game_types=("NLH_CASH_6MAX",),
            ),
            decoders={SIMULATION_PROTOBUF_CODEC_VERSION: SimulationProtobufV1Decoder()},
        )


class _Transaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return None


class _Connection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self):
        return _Transaction()

    def execute(self, query: str, parameters: tuple[object, ...]):
        self.calls.append((query, parameters))
        return type("Cursor", (), {"rowcount": 1})()


def test_postgres_writer_inserts_only_source_row_and_leaves_filter_to_trigger() -> None:
    record = build_simulation_insert(
        _generated_hand(),
        game_type="PLAY_MONEY_NLH_6MAX",
        dataset_id="sim-cdc-v1",
    )
    connection = _Connection()

    assert PostgresSimulationSink(connection).insert(record) is True
    assert len(connection.calls) == 1
    query, parameters = connection.calls[0]
    assert "INSERT INTO public.hand_history" in query
    assert "hand_completed_outbox" not in query
    assert parameters[2] == "sim-cdc-v1"
    assert parameters[3] == "acceptance"
    assert parameters[7] == "PLAY_MONEY_NLH_6MAX"
    assert parameters[9] == SIMULATION_PROTOBUF_CODEC_VERSION
    assert parameters[11] == record.payload


def test_fault_scenarios_are_deterministic_and_have_expected_poison_boundaries() -> (
    None
):
    generator = HandGenerator(
        GeneratorConfig(
            n_hands=len(FAULT_SCENARIOS),
            n_players=18,
            n_tables=3,
            n_colluding_pairs=3,
            seed=8801,
            dataset_split="live",
            dataset_id="sim-cdc-fault-test",
        ),
        start_at=datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
    )
    first = build_fault_scenario_records(
        list(generator.iter_hands()), dataset_id="sim-cdc-fault-test"
    )
    generator = HandGenerator(
        GeneratorConfig(
            n_hands=len(FAULT_SCENARIOS),
            n_players=18,
            n_tables=3,
            n_colluding_pairs=3,
            seed=8801,
            dataset_split="live",
            dataset_id="sim-cdc-fault-test",
        ),
        start_at=datetime(2026, 7, 22, 13, tzinfo=timezone.utc),
    )
    second = build_fault_scenario_records(
        list(generator.iter_hands()), dataset_id="sim-cdc-fault-test"
    )

    assert first == second
    by_name = {item.definition.name: item for item in first}
    assert by_name["checksum_mismatch"].record.payload_sha256 == "0" * 64
    malformed = by_name["malformed_protobuf"].record
    assert malformed.payload == b"\x80"
    assert malformed.payload_sha256 == hashlib.sha256(b"\x80").hexdigest()
    mismatch = by_name["game_type_mismatch"].record
    with pytest.raises(CdcRecordRejected, match="game_type_mismatch"):
        SimulationProtobufV1Decoder().decode(
            mismatch.payload,
            row=type("Row", (), {"game_type": mismatch.game_type})(),
            config=CdcAdapterConfig(dataset_id="sim-cdc-v1"),
        )
    assert by_name["unknown_codec_version"].record.codec_version.endswith("v999")
    assert {
        item.definition.expected_error_code
        for item in first
        if item.definition.expected_outcome == "dead_letter"
    } == {
        "checksum_mismatch",
        "invalid_binary_payload",
        "game_type_mismatch",
        "unknown_codec_version",
    }

    for scenario in first:
        if scenario.definition.expected_outcome == "filtered":
            continue
        record = scenario.record
        envelope = json.loads(DEBEZIUM_FIXTURE.read_text())
        envelope["after"].update(
            {
                "id": str(record.outbox_id),
                "aggregate_id": record.hand_id,
                "tenant_id": record.tenant_id,
                "product_id": record.product_id,
                "game_type": record.game_type,
                "occurred_at": record.occurred_at.isoformat(),
                "emitted_at": record.emitted_at.isoformat(),
                "codec_version": record.codec_version,
                "payload_sha256": record.payload_sha256,
                "payload": base64.b64encode(record.payload).decode(),
            }
        )
        config = CdcAdapterConfig(
            dataset_id="sim-cdc-v1",
            expected_database="poker",
            allowed_game_types=("NLH_CASH_6MAX", "NLH_TOURNAMENT_6MAX"),
        )
        if scenario.definition.expected_outcome == "canonical":
            adapted = adapt_debezium_hand_change(
                envelope,
                config=config,
                decoders={
                    SIMULATION_PROTOBUF_CODEC_VERSION: SimulationProtobufV1Decoder()
                },
            )
            assert adapted.event.payload["hand_id"] == record.hand_id
            continue
        with pytest.raises(CdcRecordRejected) as rejected:
            adapt_debezium_hand_change(
                envelope,
                config=config,
                decoders={
                    SIMULATION_PROTOBUF_CODEC_VERSION: SimulationProtobufV1Decoder()
                },
            )
        assert rejected.value.code == scenario.definition.expected_error_code


def test_database_and_debezium_contract_filter_before_kafka_without_parsing() -> None:
    sql = SQL.read_text()
    connector = json.loads(CONNECTOR.read_text())["config"]

    assert "AFTER INSERT ON public.hand_history" in sql
    assert "ml_cdc_game_type_allowlist" in sql
    assert "NEW.game_type" in sql
    assert "NEW.payload" in sql
    assert "BEFORE UPDATE OR DELETE ON public.hand_completed_outbox" in sql
    assert "WITH (publish = 'insert')" in sql
    migration = SCENARIO_MIGRATION.read_text()
    assert "ADD COLUMN IF NOT EXISTS simulation_scenario" in migration
    assert "WHERE simulation_scenario IS NULL" in migration

    assert connector["table.include.list"] == "public.hand_completed_outbox"
    assert connector["publication.autocreate.mode"] == "disabled"
    assert connector["binary.handling.mode"] == "base64"
    assert connector["transforms.routeHandOutbox.replacement"] == (
        "poker.sim.cdc-hand-outbox.v1"
    )
    assert "Filter" not in json.dumps(connector)
