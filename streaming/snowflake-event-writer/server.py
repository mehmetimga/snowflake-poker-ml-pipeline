#!/usr/bin/env python3
"""Private SPCS sidecar for idempotent Kafka-event persistence in Snowflake."""

from __future__ import annotations

from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import re
import threading
from typing import Any


LOGGER = logging.getLogger("snowflake-event-writer")
DEFAULT_TOKEN_PATH = Path("/snowflake/session/token")
MAX_REQUEST_BYTES = 2 * 1024 * 1024
HASH = re.compile(r"^[0-9a-f]{64}$")
SAFE_VALUE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/|-]{0,255}$")
ERROR_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$]*$")

EVENT_TABLES = {
    "hand": "POKER_HAND_EVENTS",
    "player_context": "POKER_PLAYER_CONTEXT_EVENTS",
    "pair_feature": "POKER_PAIR_FEATURE_EVENTS_V2",
    "risk_score": "POKER_RISK_SCORE_EVENTS",
    "rule_evidence": "POKER_RULE_EVIDENCE_EVENTS_V2",
    "review_decision": "POKER_REVIEW_DECISION_EVENTS",
    "risk_alert": "POKER_RISK_ALERT_EVENTS",
}


class ImmutableEventCollision(RuntimeError):
    """An event identity was reused with different immutable content."""


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required in SPCS")
    return value


def _identifier_environment(name: str, default: str) -> str:
    value = os.environ.get(name, default).strip().upper()
    if not IDENTIFIER.fullmatch(value):
        raise RuntimeError(f"{name} must be an unquoted Snowflake identifier")
    return value


def _database() -> str:
    return _identifier_environment("SNOWFLAKE_DATABASE", "POKER_ML_DEMO")


def _schema() -> str:
    return _identifier_environment("SNOWFLAKE_SCHEMA", "SPCS")


def _table(name: str) -> str:
    return f"{_database()}.{_schema()}.{name}"


def _connection_parameters() -> dict[str, Any]:
    token_path = Path(
        os.environ.get("SNOWFLAKE_OAUTH_TOKEN_PATH", str(DEFAULT_TOKEN_PATH))
    )
    token = token_path.read_text(encoding="utf-8").strip()
    if not token:
        raise RuntimeError("SPCS service token file is empty")
    return {
        "host": _required_environment("SNOWFLAKE_HOST"),
        "account": _required_environment("SNOWFLAKE_ACCOUNT"),
        "token": token,
        "authenticator": "oauth",
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "DEMO_WH"),
        "database": _database(),
        "schema": _schema(),
        "login_timeout": int(
            os.environ.get("EVENT_WRITER_CONNECT_TIMEOUT_SECONDS", "15")
        ),
        "network_timeout": int(
            os.environ.get("EVENT_WRITER_QUERY_TIMEOUT_SECONDS", "30")
        ),
    }


def _require_string(
    payload: dict[str, Any], name: str, *, maximum: int = 256
) -> str:
    value = payload.get(name)
    if (
        not isinstance(value, str)
        or not value.strip()
        or len(value) > maximum
    ):
        raise ValueError(
            f"{name} must be a non-empty string of at most {maximum} characters"
        )
    return value


def _require_safe_string(payload: dict[str, Any], name: str) -> str:
    value = _require_string(payload, name)
    if not SAFE_VALUE.fullmatch(value):
        raise ValueError(f"{name} contains unsupported characters")
    return value


def _require_hash(payload: dict[str, Any], name: str) -> str:
    value = _require_string(payload, name, maximum=64)
    if not HASH.fullmatch(value):
        raise ValueError(f"{name} must be a lowercase SHA-256 digest")
    return value


def _require_non_negative_int(payload: dict[str, Any], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _require_timestamp(payload: dict[str, Any], name: str) -> str:
    value = _require_string(payload, name, maximum=64)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{name} must be an RFC 3339 timestamp") from error
    return value


def _validate_kafka(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("kafka must be an object")
    expected = {
        "topic",
        "partition",
        "offset",
        "timestamp_ms",
        "key_sha256",
        "value_sha256",
    }
    if set(payload) != expected:
        raise ValueError("kafka fields do not match the persistence contract")
    topic = _require_string(payload, "topic")
    if not topic.startswith("poker.synthetic."):
        raise ValueError("only poker.synthetic.* topics are accepted")
    _require_non_negative_int(payload, "partition")
    _require_non_negative_int(payload, "offset")
    _require_non_negative_int(payload, "timestamp_ms")
    _require_hash(payload, "key_sha256")
    _require_hash(payload, "value_sha256")
    return payload


def validate_request(payload: Any) -> dict[str, Any]:
    """Validate the trusted Go-to-sidecar protocol before opening Snowflake."""
    if not isinstance(payload, dict):
        raise ValueError("request must be a JSON object")
    allowed = {
        "mode",
        "kind",
        "event_id",
        "event_type",
        "schema_version",
        "tenant_id",
        "product_id",
        "dataset_id",
        "dataset_split",
        "occurred_at",
        "emitted_at",
        "trace_id",
        "event_sha256",
        "event",
        "error_code",
        "service_build_version",
        "kafka",
    }
    if not set(payload).issubset(allowed):
        raise ValueError("request contains unknown fields")
    mode = _require_string(payload, "mode", maximum=16)
    if mode not in {"event", "dead_letter"}:
        raise ValueError("unsupported persistence mode")
    kind = _require_string(payload, "kind", maximum=32)
    event_id = _require_string(payload, "event_id", maximum=64)
    event_hash = _require_hash(payload, "event_sha256")
    build_version = _require_safe_string(payload, "service_build_version")
    kafka = _validate_kafka(payload.get("kafka"))

    normalized = dict(payload)
    normalized["event_id"] = event_id
    normalized["event_sha256"] = event_hash
    normalized["service_build_version"] = build_version
    normalized["kafka"] = kafka
    if mode == "dead_letter":
        if kind != "dead_letter" or "event" in payload:
            raise ValueError("dead-letter requests cannot contain a raw event")
        if event_hash != kafka["value_sha256"]:
            raise ValueError("dead-letter and Kafka value hashes must match")
        code = _require_string(payload, "error_code", maximum=64)
        if not ERROR_CODE.fullmatch(code):
            raise ValueError("invalid dead-letter error code")
        return normalized

    if kind not in EVENT_TABLES:
        raise ValueError("unsupported event kind")
    event_type = _require_safe_string(payload, "event_type")
    schema_version = payload.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version < 1
    ):
        raise ValueError("schema_version must be a positive integer")
    for name in (
        "tenant_id",
        "product_id",
        "dataset_id",
        "dataset_split",
        "trace_id",
    ):
        _require_safe_string(payload, name)
    _require_timestamp(payload, "occurred_at")
    _require_timestamp(payload, "emitted_at")
    event = payload.get("event")
    if not isinstance(event, dict) or not isinstance(event.get("payload"), dict):
        raise ValueError("event and event.payload must be objects")
    mirrored = {
        "event_id": event_id,
        "event_type": event_type,
        "schema_version": schema_version,
        "tenant_id": payload["tenant_id"],
        "product_id": payload["product_id"],
        "dataset_id": payload["dataset_id"],
        "dataset_split": payload["dataset_split"],
        "occurred_at": payload["occurred_at"],
        "emitted_at": payload["emitted_at"],
        "trace_id": payload["trace_id"],
    }
    if any(event.get(name) != value for name, value in mirrored.items()):
        raise ValueError("event envelope does not match persistence metadata")
    return normalized


def _entity_values(kind: str, event: dict[str, Any]) -> tuple[Any, ...]:
    payload = event["payload"]
    hand_id = payload.get("hand_id")
    table_id = payload.get("table_id")
    revision = 1
    if kind == "hand":
        entity_key = hand_id
    elif kind == "player_context":
        player = payload.get("player")
        entity_key = player.get("player_id") if isinstance(player, dict) else None
        revision = payload.get("revision", 1)
    elif kind == "pair_feature":
        entity_key = payload.get("pair_key")
        revision = payload.get("snapshot_revision", 1)
    elif kind == "risk_score":
        entity_key = payload.get("score_id")
    elif kind == "rule_evidence":
        entity_key = payload.get("rule_event_id")
        revision = payload.get("observation_revision", 1)
    elif kind == "review_decision":
        entity_key = payload.get("decision_id")
    elif kind == "risk_alert":
        entity_key = payload.get("alert_id")
    else:
        raise ValueError("unsupported event kind")
    if entity_key is None:
        entity_key = event["event_id"]
    if isinstance(revision, bool) or not isinstance(revision, int) or revision < 1:
        revision = 1
    return hand_id, table_id, str(entity_key), revision


class EventStore:
    """One bounded Snowflake session shared by the local sidecar threads."""

    def __init__(self, connector: Any | None = None) -> None:
        if connector is None:
            import snowflake.connector

            connector = snowflake.connector
        self._connector = connector
        self._connection: Any | None = None
        self._lock = threading.Lock()

    def connect(self) -> None:
        with self._lock:
            self._connect_locked()
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()

    def persist(self, raw_payload: Any) -> str:
        payload = validate_request(raw_payload)
        with self._lock:
            for attempt in range(2):
                try:
                    if self._connection is None:
                        self._connect_locked()
                    return self._persist_locked(payload)
                except ImmutableEventCollision:
                    raise
                except Exception:
                    self._close_locked()
                    if attempt:
                        raise
            raise AssertionError("unreachable")

    def close(self) -> None:
        with self._lock:
            self._close_locked()

    def _connect_locked(self) -> None:
        if self._connection is None:
            self._connection = self._connector.connect(**_connection_parameters())

    def _persist_locked(self, payload: dict[str, Any]) -> str:
        if payload["mode"] == "dead_letter":
            table = _table("POKER_SINK_DEAD_LETTERS")
            existing = self._existing_hash(table, "DEAD_LETTER_ID", payload["event_id"])
            if existing is not None:
                if existing == payload["event_sha256"]:
                    return "duplicate"
                raise ImmutableEventCollision(payload["event_id"])
            self._transaction(
                [
                    (
                        f"""
                        INSERT INTO {table} (
                          dead_letter_id, source_topic, source_partition,
                          source_offset, source_timestamp_ms, key_sha256,
                          event_sha256, error_code, sink_build_version
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            payload["event_id"],
                            payload["kafka"]["topic"],
                            payload["kafka"]["partition"],
                            payload["kafka"]["offset"],
                            payload["kafka"]["timestamp_ms"],
                            payload["kafka"]["key_sha256"],
                            payload["event_sha256"],
                            payload["error_code"],
                            payload["service_build_version"],
                        ),
                    )
                ]
            )
            return "inserted"

        envelope_table = _table("POKER_EVENT_ENVELOPES")
        existing = self._existing_hash(
            envelope_table, "EVENT_ID", payload["event_id"]
        )
        if existing is not None:
            if existing == payload["event_sha256"]:
                return "duplicate"
            raise ImmutableEventCollision(payload["event_id"])

        event = payload["event"]
        hand_id, table_id, entity_key, revision = _entity_values(
            payload["kind"], event
        )
        event_json = json.dumps(event, separators=(",", ":"), sort_keys=True)
        event_payload_json = json.dumps(
            event["payload"], separators=(",", ":"), sort_keys=True
        )
        common = (
            payload["event_id"],
            payload["event_type"],
            payload["schema_version"],
            payload["kind"],
            payload["tenant_id"],
            payload["product_id"],
            payload["dataset_id"],
            payload["dataset_split"],
            payload["occurred_at"],
            payload["emitted_at"],
            payload["trace_id"],
            payload["event_sha256"],
            event_json,
            payload["kafka"]["topic"],
            payload["kafka"]["partition"],
            payload["kafka"]["offset"],
            payload["kafka"]["timestamp_ms"],
            payload["kafka"]["key_sha256"],
            payload["service_build_version"],
        )
        typed = (
            payload["event_id"],
            payload["tenant_id"],
            payload["product_id"],
            payload["dataset_id"],
            payload["dataset_split"],
            hand_id,
            table_id,
            entity_key,
            revision,
            payload["occurred_at"],
            payload["emitted_at"],
            payload["trace_id"],
            event_payload_json,
            payload["event_sha256"],
        )
        self._transaction(
            [
                (
                    f"""
                    INSERT INTO {envelope_table} (
                      event_id, event_type, schema_version, event_kind,
                      tenant_id, product_id, dataset_id, dataset_split,
                      occurred_at, emitted_at, trace_id, event_sha256,
                      event_json, source_topic, source_partition, source_offset,
                      source_timestamp_ms, key_sha256, sink_build_version
                    ) SELECT
                      %s, %s, %s, %s, %s, %s, %s, %s,
                      TO_TIMESTAMP_TZ(%s), TO_TIMESTAMP_TZ(%s), %s, %s,
                      PARSE_JSON(%s), %s, %s, %s, %s, %s, %s
                    """,
                    common,
                ),
                (
                    f"""
                    INSERT INTO {_table(EVENT_TABLES[payload["kind"]])} (
                      event_id, tenant_id, product_id, dataset_id, dataset_split,
                      hand_id, table_id, entity_key, revision, occurred_at,
                      emitted_at, trace_id, payload, event_sha256
                    ) SELECT
                      %s, %s, %s, %s, %s, %s, %s, %s, %s,
                      TO_TIMESTAMP_TZ(%s), TO_TIMESTAMP_TZ(%s), %s,
                      PARSE_JSON(%s), %s
                    """,
                    typed,
                ),
            ]
        )
        return "inserted"

    def _existing_hash(
        self, table: str, identity_column: str, identity: str
    ) -> str | None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(
                f"""
                SELECT event_sha256
                FROM {table}
                WHERE {identity_column} = %s
                LIMIT 1
                """,
                (identity,),
            )
            row = cursor.fetchone()
            return None if row is None else str(row[0])
        finally:
            cursor.close()

    def _transaction(
        self, statements: list[tuple[str, tuple[Any, ...]]]
    ) -> None:
        cursor = self._connection.cursor()
        try:
            cursor.execute("BEGIN")
            for sql, parameters in statements:
                cursor.execute(sql, parameters)
            cursor.execute("COMMIT")
        except Exception:
            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            cursor.close()

    def _close_locked(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


class EventWriterHandler(BaseHTTPRequestHandler):
    store: EventStore

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self._send(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        self._send(HTTPStatus.OK, {"status": "ready"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/events/persist":
            self._send(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > MAX_REQUEST_BYTES:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length))
            status = self.store.persist(payload)
        except (ValueError, json.JSONDecodeError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid-request"})
            return
        except ImmutableEventCollision:
            LOGGER.error("Immutable event identity collision")
            self._send(HTTPStatus.CONFLICT, {"error": "immutable-event-collision"})
            return
        except Exception:
            # Driver messages can contain connection metadata. Keep the
            # operational log intentionally categorical.
            LOGGER.error("Snowflake event persistence failed")
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "write-unavailable"})
            return
        response_status = (
            HTTPStatus.CREATED if status == "inserted" else HTTPStatus.OK
        )
        self._send(response_status, {"status": status})

    def log_message(self, format: str, *args: Any) -> None:
        LOGGER.info("%s - %s", self.client_address[0], format % args)

    def _send(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    store = EventStore()
    store.connect()
    EventWriterHandler.store = store
    server = ThreadingHTTPServer(
        (
            os.environ.get("EVENT_WRITER_BIND_HOST", "0.0.0.0"),
            int(os.environ.get("EVENT_WRITER_PORT", "8091")),
        ),
        EventWriterHandler,
    )
    try:
        LOGGER.info("Snowflake event writer ready")
        server.serve_forever()
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
