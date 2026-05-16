from __future__ import annotations

import json
from pathlib import Path
from typing import Any

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
        df_clean = _stringify_complex(df)
        self.conn.register("_tmp_df", df_clean)
        if mode == "replace":
            self.conn.execute(f"DELETE FROM {table}")
        cols = ", ".join(df_clean.columns)
        # INSERT OR REPLACE makes re-runs idempotent against PK constraints.
        self.conn.execute(f"INSERT OR REPLACE INTO {table} ({cols}) SELECT {cols} FROM _tmp_df")
        self.conn.unregister("_tmp_df")

    def close(self) -> None:
        self.conn.close()
