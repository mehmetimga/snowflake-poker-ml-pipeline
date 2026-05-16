from __future__ import annotations

from typing import Protocol

import pandas as pd

from pipeline.config import get_settings


class Warehouse(Protocol):
    """Common interface implemented by both Snowflake and DuckDB adapters."""

    def execute(self, sql: str, params: tuple | None = None) -> None: ...

    def fetch_df(self, sql: str, params: tuple | None = None) -> pd.DataFrame: ...

    def write_pandas(self, df: pd.DataFrame, table: str, mode: str = "append") -> None: ...

    def close(self) -> None: ...

    @property
    def kind(self) -> str: ...


def get_warehouse() -> Warehouse:
    settings = get_settings()
    if settings.warehouse_backend == "snowflake":
        from .snowflake import SnowflakeWarehouse

        return SnowflakeWarehouse(settings)
    from .duckdb import DuckDBWarehouse

    return DuckDBWarehouse(settings)
