from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
from pathlib import Path
from typing import Any

import pytest


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "streaming"
    / "snowflake-context-proxy"
    / "server.py"
)
SPEC = importlib.util.spec_from_file_location("snowflake_context_proxy", MODULE_PATH)
assert SPEC and SPEC.loader
proxy = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(proxy)


class FakeCursor:
    description = [
        (name,)
        for name in (
            "TENANT_ID",
            "PRODUCT_ID",
            "USER_ID",
            "CONTEXT_VERSION",
            "EFFECTIVE_AT",
            "ACCOUNT_CREATED_AT",
            "COUNTRY_BUCKET",
            "TIMEZONE",
            "ACQUISITION_CHANNEL",
            "KYC_LEVEL",
            "ACCOUNT_STATUS",
            "BANKROLL_BUCKET",
            "PREFERRED_STAKE_BUCKET",
            "SKILL_RATING",
            "DEVICE_ID",
            "NETWORK_CLUSTER_ID",
        )
    ]

    def __init__(self) -> None:
        self.row: tuple[Any, ...] | None = None

    def execute(self, sql: str, parameters: tuple[Any, ...] | None = None) -> None:
        if sql == "SELECT 1":
            self.row = (1,)
            return
        assert "WHERE tenant_id = %s AND product_id = %s AND user_id = %s" in sql
        assert parameters is not None
        assert parameters[:3] == ("demo", "poker", "A")
        assert parameters[3] == datetime(
            2026, 7, 23, 12, 0, tzinfo=timezone.utc
        )
        self.row = (
            "demo",
            "poker",
            "A",
            2,
            datetime(2026, 7, 23, 10, 0, tzinfo=timezone.utc),
            datetime(2025, 7, 23, 10, 0, tzinfo=timezone.utc),
            "TR",
            "Europe/Istanbul",
            "organic",
            "full",
            "active",
            "medium",
            "low",
            0.72,
            "device-a",
            "network-a",
        )

    def fetchone(self) -> tuple[Any, ...] | None:
        return self.row

    def close(self) -> None:
        return None


class FakeConnection:
    def cursor(self) -> FakeCursor:
        return FakeCursor()

    def close(self) -> None:
        return None


class FakeConnector:
    def __init__(self) -> None:
        self.parameters: dict[str, Any] | None = None

    def connect(self, **parameters: Any) -> FakeConnection:
        self.parameters = parameters
        return FakeConnection()


def test_context_store_uses_spcs_token_and_parameterized_temporal_lookup(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    token_path = tmp_path / "token"
    token_path.write_text("short-lived-service-token")
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "CLBSDFJ-BQ59861")
    monkeypatch.setenv("SNOWFLAKE_HOST", "internal.snowflakecomputing.com")
    monkeypatch.setenv("SNOWFLAKE_OAUTH_TOKEN_PATH", str(token_path))
    connector = FakeConnector()
    store = proxy.ContextStore(connector)

    result = store.lookup(
        {
            "tenant_id": "demo",
            "product_id": "poker",
            "user_id": "A",
            "played_at_ms": int(
                datetime(
                    2026, 7, 23, 12, 0, tzinfo=timezone.utc
                ).timestamp()
                * 1_000
            ),
        }
    )

    assert connector.parameters is not None
    assert connector.parameters["authenticator"] == "oauth"
    assert connector.parameters["token"] == "short-lived-service-token"
    assert "user" not in connector.parameters
    assert result is not None
    assert result["context_version"] == 2
    assert result["effective_at"] == "2026-07-23T10:00:00Z"


def test_context_store_rejects_invalid_keys_before_query(
    monkeypatch: pytest.MonkeyPatch
) -> None:
    store = proxy.ContextStore(FakeConnector())
    with pytest.raises(ValueError, match="tenant_id"):
        store.lookup(
            {
                "tenant_id": "",
                "product_id": "poker",
                "user_id": "A",
                "played_at_ms": 1,
            }
        )
