from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb
import pandas as pd

from pipeline.config import Settings


def _stringify_complex(df: pd.DataFrame) -> pd.DataFrame:
    """DuckDB tolerates JSON-as-text in JSON columns; convert lists/dicts to JSON strings."""
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            mask = out[col].apply(lambda v: isinstance(v, (list, dict)))
            if mask.any():
                out.loc[mask, col] = out.loc[mask, col].apply(json.dumps)
    return out


class DuckDBWarehouse:
    kind = "duckdb"

    def __init__(self, settings: Settings) -> None:
        self.path = Path(settings.duckdb_path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.path))
        self._s3_bucket = settings.duckdb_s3_bucket
        self._s3_prefix = (settings.duckdb_s3_prefix or "").lstrip("/")
        self._configure_s3()

    def _configure_s3(self) -> None:
        """Install + load httpfs, then resolve AWS credentials from the env or
        the instance/task role. On SageMaker / ECS the role is picked up
        automatically from instance metadata."""
        if not self._s3_bucket:
            return
        self.conn.execute("INSTALL httpfs")
        self.conn.execute("LOAD httpfs")
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if region:
            self.conn.execute(f"SET s3_region='{region}'")
        # CREATE SECRET (credential_chain) lets DuckDB use the same provider
        # chain as boto3 (env, profile, instance role). Available in DuckDB >= 0.10.
        try:
            self.conn.execute("CREATE OR REPLACE SECRET s3_creds (TYPE S3, PROVIDER CREDENTIAL_CHAIN)")
        except duckdb.Error:
            # Older DuckDB — fall back to env vars if present.
            ak = os.environ.get("AWS_ACCESS_KEY_ID")
            sk = os.environ.get("AWS_SECRET_ACCESS_KEY")
            tok = os.environ.get("AWS_SESSION_TOKEN")
            if ak and sk:
                self.conn.execute(f"SET s3_access_key_id='{ak}'")
                self.conn.execute(f"SET s3_secret_access_key='{sk}'")
                if tok:
                    self.conn.execute(f"SET s3_session_token='{tok}'")

    def _s3_table_uri(self, table: str) -> str:
        return f"s3://{self._s3_bucket}/{self._s3_prefix}{table.lower()}.parquet"

    def _hydrate_table_from_s3(self, table: str) -> None:
        """If the table is empty locally but a parquet exists in S3, load it."""
        if not self._s3_bucket:
            return
        try:
            count = self.conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except duckdb.Error:
            return  # table doesn't exist yet
        if count > 0:
            return
        uri = self._s3_table_uri(table)
        try:
            self.conn.execute(
                f"INSERT INTO {table} SELECT * FROM read_parquet('{uri}')"
            )
        except duckdb.Error:
            # No parquet at that key yet — fine.
            pass

    def _persist_table_to_s3(self, table: str) -> None:
        if not self._s3_bucket:
            return
        uri = self._s3_table_uri(table)
        self.conn.execute(
            f"COPY (SELECT * FROM {table}) TO '{uri}' (FORMAT 'parquet', OVERWRITE_OR_IGNORE TRUE)"
        )

    def execute(self, sql: str, params: tuple | None = None) -> None:
        if params:
            self.conn.execute(sql, params)
        else:
            self.conn.execute(sql)

    def fetch_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame:
        if params:
            return self.conn.execute(sql, params).fetch_df()
        return self.conn.execute(sql).fetch_df()

    def write_pandas(self, df: pd.DataFrame, table: str, mode: str = "append") -> None:
        if df.empty:
            return
        # If S3 backed, hydrate the local copy first so an "append" against an
        # ephemeral container actually appends to the stored data.
        self._hydrate_table_from_s3(table)
        df_clean = _stringify_complex(df)
        self.conn.register("_tmp_df", df_clean)
        if mode == "replace":
            self.conn.execute(f"DELETE FROM {table}")
        cols = ", ".join(df_clean.columns)
        self.conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {cols} FROM _tmp_df")
        self.conn.unregister("_tmp_df")
        self._persist_table_to_s3(table)

    def close(self) -> None:
        self.conn.close()
