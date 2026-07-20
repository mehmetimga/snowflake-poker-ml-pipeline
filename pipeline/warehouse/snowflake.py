from __future__ import annotations

from typing import Any

import pandas as pd

from pipeline.config import Settings


class SnowflakeWarehouse:
    kind = "snowflake"

    def __init__(self, settings: Settings) -> None:
        import snowflake.connector

        kwargs: dict[str, Any] = {
            "account": settings.snowflake_account,
            "warehouse": settings.snowflake_warehouse,
            "database": settings.snowflake_database,
            "schema": settings.snowflake_schema,
        }
        token_path = settings.snowflake_oauth_token_path
        if settings.snowflake_host and token_path.is_file():
            # Snowpark Container Services injects a short-lived service OAuth
            # token and refreshes the file automatically. The service runs as
            # its owner role, so do not request the local user's configured
            # role here.
            kwargs.update(
                host=settings.snowflake_host,
                authenticator="oauth",
                token=token_path.read_text().strip(),
            )
        elif settings.snowflake_private_key_path:
            kwargs.update(user=settings.snowflake_user, role=settings.snowflake_role)
            with open(settings.snowflake_private_key_path, "rb") as f:
                kwargs["private_key"] = f.read()
        else:
            kwargs.update(
                user=settings.snowflake_user,
                password=settings.snowflake_password,
                role=settings.snowflake_role,
            )
            if settings.snowflake_authenticator:
                kwargs["authenticator"] = settings.snowflake_authenticator
            if settings.snowflake_client_request_mfa_token:
                kwargs["client_request_mfa_token"] = True
        self.conn = snowflake.connector.connect(**kwargs)
        self._settings = settings

    def execute(self, sql: str, params: tuple | None = None) -> None:
        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
        finally:
            cur.close()

    def fetch_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
        from snowflake.connector.errors import NotSupportedError

        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            try:
                df = cur.fetch_pandas_all()
            except NotSupportedError:
                # Metadata commands such as SHOW don't expose an Arrow result
                # set, but they are still useful through the warehouse API.
                columns = [item[0] for item in cur.description]
                df = pd.DataFrame(cur.fetchall(), columns=columns)
            # Keep the warehouse interface backend-neutral. DuckDB returns the
            # lowercase names used throughout the pipeline, while Snowflake
            # returns unquoted identifiers in uppercase.
            df.columns = [str(column).lower() for column in df.columns]
            return df
        finally:
            cur.close()

    def write_pandas(self, df: pd.DataFrame, table: str, mode: str = "append") -> None:
        if df.empty:
            return
        from snowflake.connector.pandas_tools import write_pandas

        if mode == "replace":
            self.execute(f"DELETE FROM {table}")
        upload = df.reset_index(drop=True).copy()
        # The migrations create unquoted Snowflake identifiers, which are
        # stored uppercase. write_pandas quotes DataFrame column names by
        # default, so lowercase pandas names otherwise become invalid quoted
        # identifiers such as "hand_id".
        upload.columns = [str(column).upper() for column in upload.columns]
        write_pandas(
            self.conn,
            upload,
            table.upper(),
            auto_create_table=False,
            use_logical_type=True,
        )

    def close(self) -> None:
        self.conn.close()
