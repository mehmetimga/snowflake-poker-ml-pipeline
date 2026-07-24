from __future__ import annotations

import importlib.util
from pathlib import Path
from typing import Any

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "streaming"
    / "snowflake-event-writer"
    / "server.py"
)
SPEC = importlib.util.spec_from_file_location("snowflake_event_writer", MODULE_PATH)
assert SPEC and SPEC.loader
writer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(writer)


def valid_request(*, mode: str = "event") -> dict[str, Any]:
    event = {
        "event_id": "00000000-0000-5000-8000-000000000041",
        "event_type": "poker.risk-score.computed",
        "schema_version": 1,
        "tenant_id": "demo",
        "product_id": "poker",
        "dataset_id": "acceptance-d7",
        "dataset_split": "live",
        "occurred_at": "2026-07-23T10:00:00Z",
        "emitted_at": "2026-07-23T10:00:01Z",
        "trace_id": "00000000-0000-5000-8000-000000000099",
        "payload": {
            "score_id": "score-1",
            "hand_id": "hand-1",
            "table_id": "table-1",
        },
    }
    request: dict[str, Any] = {
        "mode": mode,
        "kind": "risk_score",
        **{name: value for name, value in event.items() if name != "payload"},
        "event_sha256": "a" * 64,
        "event": event,
        "service_build_version": "sink-test-build",
        "kafka": {
            "topic": "poker.synthetic.risk-scores.v1",
            "partition": 2,
            "offset": 41,
            "timestamp_ms": 1_774_519_201_000,
            "key_sha256": "b" * 64,
            "value_sha256": "a" * 64,
        },
    }
    return request


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection
        self.row: tuple[Any, ...] | None = None

    def execute(
        self, sql: str, parameters: tuple[Any, ...] | None = None
    ) -> None:
        normalized = " ".join(sql.split())
        self.connection.statements.append((normalized, parameters))
        if normalized == "SELECT 1":
            self.row = (1,)
        elif normalized.startswith("SELECT event_sha256"):
            self.row = (
                None
                if self.connection.existing_hash is None
                else (self.connection.existing_hash,)
            )
        elif (
            self.connection.fail_typed
            and "INSERT INTO POKER_ML_DEMO.SPCS.POKER_RISK_SCORE_EVENTS" in normalized
        ):
            raise RuntimeError("simulated typed write failure")

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(
        self, *, existing_hash: str | None = None, fail_typed: bool = False
    ) -> None:
        self.existing_hash = existing_hash
        self.fail_typed = fail_typed
        self.statements: list[tuple[str, tuple[Any, ...] | None]] = []
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True


class FakeConnector:
    def __init__(
        self, *, existing_hash: str | None = None, fail_typed: bool = False
    ) -> None:
        self.existing_hash = existing_hash
        self.fail_typed = fail_typed
        self.parameters: dict[str, Any] | None = None
        self.connections: list[FakeConnection] = []

    def connect(self, **parameters: Any) -> FakeConnection:
        self.parameters = parameters
        connection = FakeConnection(
            existing_hash=self.existing_hash, fail_typed=self.fail_typed
        )
        self.connections.append(connection)
        return connection


@pytest.fixture
def spcs_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> Path:
    token_path = tmp_path / "token"
    token_path.write_text("short-lived-service-token")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "CLBSDFJ-BQ59861")
    monkeypatch.setenv("SNOWFLAKE_HOST", "internal.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_OAUTH_TOKEN_PATH", str(token_path))
    return token_path


def test_event_store_uses_spcs_token_and_atomic_event_tables(
    spcs_environment: Path,
) -> None:
    connector = FakeConnector()
    store = writer.EventStore(connector)

    assert store.persist(valid_request()) == "inserted"

    assert connector.parameters is not None
    assert connector.parameters["authenticator"] == "oauth"
    assert connector.parameters["token"] == "short-lived-service-token"
    assert "user" not in connector.parameters
    statements = [sql for sql, _ in connector.connections[0].statements]
    assert statements.count("BEGIN") == 1
    assert statements.count("COMMIT") == 1
    assert any("INSERT INTO POKER_ML_DEMO.SPCS.POKER_EVENT_ENVELOPES" in sql for sql in statements)
    assert any("INSERT INTO POKER_ML_DEMO.SPCS.POKER_RISK_SCORE_EVENTS" in sql for sql in statements)


def test_duplicate_hash_is_success_without_second_insert(
    spcs_environment: Path,
) -> None:
    connector = FakeConnector(existing_hash="a" * 64)
    store = writer.EventStore(connector)

    assert store.persist(valid_request()) == "duplicate"
    statements = [sql for sql, _ in connector.connections[0].statements]
    assert "BEGIN" not in statements
    assert not any(sql.startswith("INSERT") for sql in statements)


def test_identity_collision_is_rejected_without_write(
    spcs_environment: Path,
) -> None:
    connector = FakeConnector(existing_hash="c" * 64)
    store = writer.EventStore(connector)

    with pytest.raises(writer.ImmutableEventCollision):
        store.persist(valid_request())
    statements = [sql for sql, _ in connector.connections[0].statements]
    assert "BEGIN" not in statements


def test_partial_event_write_rolls_back_and_never_commits(
    spcs_environment: Path,
) -> None:
    connector = FakeConnector(fail_typed=True)
    store = writer.EventStore(connector)

    with pytest.raises(RuntimeError, match="simulated typed write failure"):
        store.persist(valid_request())
    assert len(connector.connections) == 2
    for connection in connector.connections:
        statements = [sql for sql, _ in connection.statements]
        assert "BEGIN" in statements
        assert "ROLLBACK" in statements
        assert "COMMIT" not in statements


def test_dead_letter_contract_excludes_raw_event() -> None:
    request = valid_request()
    dead_letter = {
        "mode": "dead_letter",
        "kind": "dead_letter",
        "event_id": "0123456789abcdef0123456789abcdef",
        "event_sha256": request["event_sha256"],
        "error_code": "invalid_json_or_envelope",
        "service_build_version": request["service_build_version"],
        "kafka": request["kafka"],
    }
    assert writer.validate_request(dead_letter)["mode"] == "dead_letter"
    dead_letter["event"] = {"password": "must-not-be-stored"}
    with pytest.raises(ValueError, match="cannot contain a raw event"):
        writer.validate_request(dead_letter)


def test_writer_rejects_metadata_confusion_before_snowflake() -> None:
    request = valid_request()
    request["event"]["dataset_id"] = "other-dataset"
    with pytest.raises(ValueError, match="does not match"):
        writer.validate_request(request)


def test_event_hash_can_differ_from_raw_kafka_serialization_hash() -> None:
    request = valid_request()
    request["event_sha256"] = "c" * 64

    assert writer.validate_request(request)["event_sha256"] == "c" * 64


def test_dead_letter_hash_must_match_raw_kafka_value() -> None:
    request = valid_request()
    dead_letter = {
        "mode": "dead_letter",
        "kind": "dead_letter",
        "event_id": "0123456789abcdef0123456789abcdef",
        "event_sha256": "c" * 64,
        "error_code": "invalid_json_or_envelope",
        "service_build_version": request["service_build_version"],
        "kafka": request["kafka"],
    }

    with pytest.raises(ValueError, match="hashes must match"):
        writer.validate_request(dead_letter)
