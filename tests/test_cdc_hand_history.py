from __future__ import annotations

import base64
import copy
import hashlib
import json
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.cdc import (
    CDC_HAND_OUTBOX_TOPIC,
    CdcAdapterConfig,
    CdcRecordRejected,
    KafkaSourcePosition,
    adapt_debezium_hand_change,
)
from pipeline.events import HAND_COMPLETED, HandCompletedPayload, build_event
from pipeline.kafka.headers import canonical_event_headers
from pipeline.config import Settings
from pipeline.kafka.world_sink import WorldWarehouseSink
from pipeline.warehouse.duckdb import DuckDBWarehouse
from pipeline.warehouse.migrate import run_migrations


CDC_FIXTURE = Path("schemas/examples/debezium.hand-completed-outbox.v1.json")
HAND_FIXTURE = Path("schemas/examples/cdc-canonical-hand-payload-v1.json")
SCHEMA = Path("schemas/cdc/debezium.hand-completed-outbox.v1.schema.json")
DLQ_SCHEMA = Path("schemas/cdc/poker.cdc-hand.dead-lettered.v1.schema.json")


def _cdc() -> dict:
    return json.loads(CDC_FIXTURE.read_text())


def _hand() -> dict:
    return json.loads(HAND_FIXTURE.read_text())


def _config(**updates) -> CdcAdapterConfig:
    values = {
        "dataset_id": "prod-cdc-v1",
        "dataset_split": "live",
        "expected_database": "poker",
        "allowed_tenants": ("demo",),
    }
    values.update(updates)
    return CdcAdapterConfig(**values)


def _direct_event():
    payload = HandCompletedPayload.model_validate(_hand())
    return build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id="prod-cdc-v1",
        dataset_split="live",
        occurred_at=datetime.fromisoformat("2026-07-21T10:00:00Z"),
        emitted_at=datetime.fromisoformat("2026-07-21T10:00:01Z"),
        tenant_id="demo",
        product_id="poker",
    )


def _replace_binary(record: dict, payload: bytes) -> None:
    record["after"]["payload"] = base64.b64encode(payload).decode("ascii")
    record["after"]["payload_sha256"] = hashlib.sha256(payload).hexdigest()


def _assert_code(code: str, record, **kwargs) -> None:
    with pytest.raises(CdcRecordRejected) as captured:
        adapt_debezium_hand_change(record, config=_config(), **kwargs)
    assert captured.value.code == code


def test_cdc_fixture_and_direct_publish_are_canonically_identical() -> None:
    adapted = adapt_debezium_hand_change(
        _cdc(),
        config=_config(),
        source_position=KafkaSourcePosition(partition=2, offset=41),
    )
    direct = _direct_event()

    assert adapted.event == direct
    assert str(adapted.event.event_id) == "f00d27af-a72b-58bd-8180-14d6e38d3040"
    assert str(adapted.event.trace_id) == "e6dae691-09f7-523b-aece-0fa0a67d3609"
    assert adapted.target_topic == "poker.hands.raw.v1"
    assert adapted.partition_key == "c2_table_01"
    assert adapted.canonical_headers == canonical_event_headers(direct)


def test_fixture_binary_is_exact_canonical_payload_and_checksum() -> None:
    row = _cdc()["after"]
    expected = json.dumps(_hand(), sort_keys=True, separators=(",", ":")).encode()
    actual = base64.b64decode(row["payload"], validate=True)

    assert actual == expected
    assert hashlib.sha256(actual).hexdigest() == row["payload_sha256"]
    assert HandCompletedPayload.model_validate_json(actual).generator == "poker-server"


def test_source_transaction_lsn_and_kafka_position_reach_headers() -> None:
    adapted = adapt_debezium_hand_change(
        _cdc(),
        config=_config(),
        source_position=KafkaSourcePosition(partition=2, offset=41),
    )
    headers = {key: value.decode() for key, value in adapted.kafka_headers}

    assert adapted.lineage.source_lsn == 270113177
    assert adapted.lineage.source_tx_id == 9001
    assert adapted.lineage.transaction_id == "9001:270113177"
    assert headers["cdc_source_lsn"] == "270113177"
    assert headers["cdc_transaction_id"] == "9001:270113177"
    assert headers["cdc_source_topic"] == CDC_HAND_OUTBOX_TOPIC
    assert headers["cdc_source_partition"] == "2"
    assert headers["cdc_source_offset"] == "41"
    assert headers["cdc_payload_sha256"] == adapted.lineage.payload_sha256


def test_replay_connect_wrapper_and_snapshot_keep_same_canonical_identity() -> None:
    live = adapt_debezium_hand_change(_cdc(), config=_config())
    wrapped = {"schema": {"type": "struct"}, "payload": _cdc()}
    wrapped_result = adapt_debezium_hand_change(wrapped, config=_config())

    snapshot = _cdc()
    snapshot["op"] = "r"
    snapshot["source"]["snapshot"] = "initial"
    snapshot["source"]["txId"] = None
    snapshot["transaction"] = None
    snapshot_result = adapt_debezium_hand_change(snapshot, config=_config())

    assert live.event == wrapped_result.event == snapshot_result.event
    assert live.canonical_headers == snapshot_result.canonical_headers
    assert snapshot_result.lineage.operation == "r"
    assert snapshot_result.lineage.snapshot == "initial"


def test_real_binary_decoder_is_a_versioned_plugin_seam() -> None:
    class BinaryV42:
        codec_version = "poker-server-binary-v42"

        def decode(self, payload, *, row, config):
            assert payload == b"\x00\x01poker-hand"
            assert row.aggregate_id == "C2-FIXTURE-H-000001"
            assert config.dataset_split == "live"
            return HandCompletedPayload.model_validate(_hand())

    record = _cdc()
    record["after"]["codec_version"] = BinaryV42.codec_version
    _replace_binary(record, b"\x00\x01poker-hand")

    adapted = adapt_debezium_hand_change(
        record,
        config=_config(),
        decoders={BinaryV42.codec_version: BinaryV42()},
    )
    assert adapted.event == _direct_event()


@pytest.mark.parametrize("operation", ["u", "d"])
def test_update_and_delete_are_rejected_for_immutable_outbox(operation: str) -> None:
    record = _cdc()
    record["op"] = operation
    if operation == "d":
        record["before"] = record["after"]
        record["after"] = None
    _assert_code("immutable_outbox_operation", record)


def test_tombstones_and_missing_transaction_lineage_are_rejected() -> None:
    _assert_code("tombstone", None)

    record = _cdc()
    record["source"]["txId"] = None
    _assert_code("missing_transaction_lineage", record)


def test_checksum_unknown_codec_and_source_drift_are_rejected() -> None:
    record = _cdc()
    record["after"]["payload_sha256"] = "0" * 64
    _assert_code("checksum_mismatch", record)

    record = _cdc()
    record["after"]["codec_version"] = "poker-server-binary-v99"
    _assert_code("unknown_codec_version", record)

    _assert_code("unknown_codec_version", _cdc(), decoders={})

    record = _cdc()
    record["source"]["table"] = "hand_history_mutable"
    _assert_code("unexpected_source_table", record)


def test_identity_split_and_private_truth_poison_records_are_rejected() -> None:
    record = _cdc()
    record["after"]["aggregate_id"] = "different-hand"
    _assert_code("aggregate_identity_mismatch", record)

    with pytest.raises(CdcRecordRejected) as captured:
        adapt_debezium_hand_change(_cdc(), config=_config(dataset_split="test"))
    assert captured.value.code == "dataset_split_mismatch"

    record = _cdc()
    payload = copy.deepcopy(_hand())
    payload["players"][0]["is_suspicious"] = True
    _replace_binary(
        record,
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode(),
    )
    _assert_code("invalid_canonical_payload", record)


def test_static_contract_describes_create_snapshot_and_strict_outbox_row() -> None:
    schema = json.loads(SCHEMA.read_text())
    dlq_schema = json.loads(DLQ_SCHEMA.read_text())
    row = schema["$defs"]["outboxRow"]

    assert schema["properties"]["op"]["enum"] == ["c", "r"]
    assert row["additionalProperties"] is False
    assert row["properties"]["event_type"]["const"] == HAND_COMPLETED
    assert row["properties"]["payload"]["contentEncoding"] == "base64"
    assert "payload_sha256" in row["required"]
    dlq_payload = dlq_schema["properties"]["payload"]
    assert dlq_schema["properties"]["event_type"]["const"] == (
        "poker.cdc-hand.dead-lettered"
    )
    assert dlq_payload["additionalProperties"] is False
    assert "source_value_sha256" in dlq_payload["required"]
    assert "raw_value" not in json.dumps(dlq_payload)


def test_canonical_warehouse_audit_persists_cdc_headers(tmp_path: Path) -> None:
    adapted = adapt_debezium_hand_change(
        _cdc(),
        config=_config(),
        source_position=KafkaSourcePosition(partition=2, offset=41),
    )
    message = SimpleNamespace(
        value=adapted.event.model_dump(mode="json"),
        key=adapted.partition_key,
        topic=adapted.target_topic,
        partition=1,
        offset=12,
        timestamp=1784628001100,
        headers=adapted.kafka_headers,
    )

    class Consumer:
        commits = 0

        def __iter__(self):
            return iter([message])

        def commit(self):
            self.commits += 1

        def close(self):
            return None

    warehouse = DuckDBWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="duckdb",
            DUCKDB_PATH=tmp_path / "cdc-audit.duckdb",
        )
    )
    run_migrations(warehouse)
    consumer = Consumer()
    sink = WorldWarehouseSink(warehouse=warehouse, consumer=consumer)

    result = sink.run()
    stored = warehouse.fetch_df(
        "SELECT source_lineage FROM RAW_EVENT_ENVELOPES"
    ).iloc[0]["source_lineage"]
    lineage = json.loads(stored)

    assert result.events == 1
    assert consumer.commits == 1
    assert lineage["cdc_source_lsn"] == "270113177"
    assert lineage["cdc_source_tx_id"] == "9001"
    assert lineage["cdc_source_topic"] == CDC_HAND_OUTBOX_TOPIC
    assert lineage["cdc_source_partition"] == "2"
    assert lineage["cdc_source_offset"] == "41"
    warehouse.close()
