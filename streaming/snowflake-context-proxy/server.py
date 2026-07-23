#!/usr/bin/env python3
"""Private SPCS sidecar for point-in-time Snowflake user-context lookups."""

from __future__ import annotations

from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import logging
import os
from pathlib import Path
import re
import threading
from typing import Any


LOGGER = logging.getLogger("snowflake-context-proxy")
DEFAULT_TOKEN_PATH = Path("/snowflake/session/token")
DEFAULT_TABLE = "POKER_ML_DEMO.SPCS.POKER_USER_CONTEXT_HISTORY"
IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$]*(?:\.[A-Z][A-Z0-9_$]*){2}$")
MAX_REQUEST_BYTES = 8_192


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is required in SPCS")
    return value


def _table_name() -> str:
    value = os.environ.get("USER_CONTEXT_SNOWFLAKE_TABLE", DEFAULT_TABLE).upper()
    if not IDENTIFIER.fullmatch(value):
        raise RuntimeError(
            "USER_CONTEXT_SNOWFLAKE_TABLE must be a fully qualified "
            "unquoted Snowflake identifier"
        )
    return value


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
        "database": os.environ.get("SNOWFLAKE_DATABASE", "POKER_ML_DEMO"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "SPCS"),
        "login_timeout": int(os.environ.get("CONTEXT_PROXY_CONNECT_TIMEOUT_SECONDS", "15")),
        "network_timeout": int(os.environ.get("CONTEXT_PROXY_QUERY_TIMEOUT_SECONDS", "20")),
    }


def _require_key(payload: dict[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip() or len(value) > 128:
        raise ValueError(f"{name} must be a non-empty string of at most 128 characters")
    return value


def _played_at(payload: dict[str, Any]) -> datetime:
    value = payload.get("played_at_ms")
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("played_at_ms must be a non-negative integer")
    return datetime.fromtimestamp(value / 1_000, tz=timezone.utc)


def _json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return value


class ContextStore:
    """One bounded Snowflake session shared by the local sidecar threads."""

    def __init__(self, connector: Any | None = None) -> None:
        if connector is None:
            import snowflake.connector

            connector = snowflake.connector
        self._connector = connector
        self._connection: Any | None = None
        self._lock = threading.Lock()
        self._query = f"""
            SELECT tenant_id, product_id, user_id, context_version,
                   effective_at, account_created_at,
                   country_bucket, timezone, acquisition_channel, kyc_level,
                   account_status, bankroll_bucket, preferred_stake_bucket,
                   skill_rating, device_id, network_cluster_id
            FROM {_table_name()}
            WHERE tenant_id = %s AND product_id = %s AND user_id = %s
              AND effective_at <= %s
            ORDER BY effective_at DESC, context_version DESC
            LIMIT 1
        """

    def connect(self) -> None:
        with self._lock:
            self._connect_locked()
            cursor = self._connection.cursor()
            try:
                cursor.execute("SELECT 1")
                cursor.fetchone()
            finally:
                cursor.close()

    def lookup(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        parameters = (
            _require_key(payload, "tenant_id"),
            _require_key(payload, "product_id"),
            _require_key(payload, "user_id"),
            _played_at(payload),
        )
        with self._lock:
            for attempt in range(2):
                try:
                    if self._connection is None:
                        self._connect_locked()
                    return self._execute_locked(parameters)
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

    def _execute_locked(self, parameters: tuple[Any, ...]) -> dict[str, Any] | None:
        cursor = self._connection.cursor()
        try:
            cursor.execute(self._query, parameters)
            row = cursor.fetchone()
            if row is None:
                return None
            columns = [str(item[0]).lower() for item in cursor.description]
            return {
                name: _json_value(value)
                for name, value in zip(columns, row, strict=True)
            }
        finally:
            cursor.close()

    def _close_locked(self) -> None:
        connection, self._connection = self._connection, None
        if connection is not None:
            try:
                connection.close()
            except Exception:
                pass


class ContextHandler(BaseHTTPRequestHandler):
    store: ContextStore

    def do_GET(self) -> None:  # noqa: N802
        if self.path != "/healthz":
            self._send(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        self._send(HTTPStatus.OK, {"status": "ready"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path != "/v1/user-context/lookup":
            self._send(HTTPStatus.NOT_FOUND, {"error": "not-found"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length < 1 or length > MAX_REQUEST_BYTES:
                raise ValueError("invalid request size")
            payload = json.loads(self.rfile.read(length))
            if not isinstance(payload, dict):
                raise ValueError("request must be a JSON object")
            result = self.store.lookup(payload)
        except (ValueError, json.JSONDecodeError):
            self._send(HTTPStatus.BAD_REQUEST, {"error": "invalid-request"})
            return
        except Exception:
            # Driver messages can contain connection metadata. Keep the
            # operational log intentionally categorical.
            LOGGER.error("Snowflake context lookup failed")
            self._send(HTTPStatus.SERVICE_UNAVAILABLE, {"error": "lookup-unavailable"})
            return
        if result is None:
            self._send(HTTPStatus.NOT_FOUND, {"error": "context-not-found"})
            return
        self._send(HTTPStatus.OK, result)

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
    store = ContextStore()
    store.connect()
    ContextHandler.store = store
    server = ThreadingHTTPServer(
        ("0.0.0.0", int(os.environ.get("CONTEXT_PROXY_PORT", "8090"))),
        ContextHandler,
    )
    try:
        LOGGER.info("Snowflake context proxy ready")
        server.serve_forever()
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
