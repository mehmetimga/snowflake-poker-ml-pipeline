from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipeline.config import Settings
from pipeline.warehouse.snowflake import SnowflakeWarehouse


class _Connection:
    def close(self) -> None:
        pass


class _Cursor:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def execute(self, sql, params=None):
        return self

    def fetch_pandas_all(self):
        return self.df.copy()

    def close(self):
        pass


class _MetadataCursor(_Cursor):
    description = [("NAME",), ("STATUS",)]

    def fetch_pandas_all(self):
        from snowflake.connector.errors import NotSupportedError

        raise NotSupportedError

    def fetchall(self):
        return [("POKER_ADMIN", "RUNNING")]


class _FetchConnection(_Connection):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def cursor(self):
        return _Cursor(self.df)


class _MetadataConnection(_Connection):
    def cursor(self):
        return _MetadataCursor(pd.DataFrame())


def test_spcs_oauth_token_is_used_without_local_credentials(monkeypatch, tmp_path: Path):
    token_path = tmp_path / "token"
    token_path.write_text("service-oauth-token\n")
    captured = {}

    def connect(**kwargs):
        captured.update(kwargs)
        return _Connection()

    monkeypatch.setattr("snowflake.connector.connect", connect)
    settings = Settings(
        _env_file=None,
        WAREHOUSE_BACKEND="snowflake",
        SNOWFLAKE_ACCOUNT="account-locator",
        SNOWFLAKE_HOST="internal.snowflakecomputing.com",
        SNOWFLAKE_USER="local-user",
        SNOWFLAKE_PASSWORD="local-password",
        SNOWFLAKE_ROLE="SYSADMIN",
        SNOWFLAKE_OAUTH_TOKEN_PATH=token_path,
    )

    SnowflakeWarehouse(settings)

    assert captured["authenticator"] == "oauth"
    assert captured["token"] == "service-oauth-token"
    assert captured["host"] == "internal.snowflakecomputing.com"
    assert "user" not in captured
    assert "password" not in captured
    assert "role" not in captured


def test_write_pandas_normalizes_columns_to_snowflake_identifiers(monkeypatch):
    settings = Settings(
        _env_file=None,
        WAREHOUSE_BACKEND="snowflake",
        SNOWFLAKE_ACCOUNT="org-account",
        SNOWFLAKE_USER="user",
        SNOWFLAKE_PASSWORD="password",
    )
    monkeypatch.setattr("snowflake.connector.connect", lambda **_: _Connection())
    warehouse = SnowflakeWarehouse(settings)
    captured = {}

    def fake_write_pandas(conn, df, table, **kwargs):
        captured.update(conn=conn, df=df, table=table, kwargs=kwargs)

    monkeypatch.setattr("snowflake.connector.pandas_tools.write_pandas", fake_write_pandas)
    original = pd.DataFrame([{"hand_id": "H-1", "table_id": "table-1"}])

    warehouse.write_pandas(original, "raw_hands")

    assert list(captured["df"].columns) == ["HAND_ID", "TABLE_ID"]
    assert captured["table"] == "RAW_HANDS"
    assert list(original.columns) == ["hand_id", "table_id"]


def test_password_auth_can_request_cached_mfa_token(monkeypatch):
    captured = {}

    def connect(**kwargs):
        captured.update(kwargs)
        return _Connection()

    monkeypatch.setattr("snowflake.connector.connect", connect)
    settings = Settings(
        _env_file=None,
        WAREHOUSE_BACKEND="snowflake",
        SNOWFLAKE_ACCOUNT="org-account",
        SNOWFLAKE_USER="user",
        SNOWFLAKE_PASSWORD="password",
        SNOWFLAKE_AUTHENTICATOR="username_password_mfa",
        SNOWFLAKE_CLIENT_REQUEST_MFA_TOKEN=True,
    )

    SnowflakeWarehouse(settings)

    assert captured["authenticator"] == "username_password_mfa"
    assert captured["client_request_mfa_token"] is True


def test_fetch_df_normalizes_snowflake_columns_to_pipeline_names(monkeypatch):
    connection = _FetchConnection(pd.DataFrame([{"HAND_ID": "H-1", "AMOUNT": 2.5}]))
    monkeypatch.setattr("snowflake.connector.connect", lambda **_: connection)
    warehouse = SnowflakeWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="snowflake",
            SNOWFLAKE_ACCOUNT="org-account",
            SNOWFLAKE_USER="user",
            SNOWFLAKE_PASSWORD="password",
        )
    )

    result = warehouse.fetch_df("SELECT HAND_ID, AMOUNT FROM RAW_ACTIONS")

    assert list(result.columns) == ["hand_id", "amount"]


def test_fetch_df_falls_back_for_metadata_commands(monkeypatch):
    monkeypatch.setattr("snowflake.connector.connect", lambda **_: _MetadataConnection())
    warehouse = SnowflakeWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="snowflake",
            SNOWFLAKE_ACCOUNT="org-account",
            SNOWFLAKE_USER="user",
            SNOWFLAKE_PASSWORD="password",
        )
    )

    result = warehouse.fetch_df("SHOW SERVICES")

    assert result.to_dict("records") == [{"name": "POKER_ADMIN", "status": "RUNNING"}]
