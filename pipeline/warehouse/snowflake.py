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
            "user": settings.snowflake_user,
            "warehouse": settings.snowflake_warehouse,
            "database": settings.snowflake_database,
            "schema": settings.snowflake_schema,
            "role": settings.snowflake_role,
        }
        if settings.snowflake_private_key_path:
            with open(settings.snowflake_private_key_path, "rb") as f:
                kwargs["private_key"] = f.read()
        else:
            kwargs["password"] = settings.snowflake_password
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
        cur = self.conn.cursor()
        try:
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            return cur.fetch_pandas_all()
        finally:
            cur.close()

    def write_pandas(self, df: pd.DataFrame, table: str, mode: str = "append") -> None:
        if df.empty:
            return
        from snowflake.connector.pandas_tools import write_pandas

        if mode == "replace":
            self.execute(f"DELETE FROM {table}")
        write_pandas(self.conn, df, table.upper(), auto_create_table=False)

    def close(self) -> None:
        self.conn.close()
