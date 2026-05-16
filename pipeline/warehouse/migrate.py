from __future__ import annotations

from pathlib import Path

from pipeline.warehouse.dialect import to_duckdb
from pipeline.warehouse.factory import Warehouse, get_warehouse

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent.parent / "sql" / "migrations"


def _split_statements(sql: str) -> list[str]:
    return [s.strip() for s in sql.split(";") if s.strip()]


def run_migrations(warehouse: Warehouse | None = None) -> None:
    wh = warehouse or get_warehouse()
    files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    for path in files:
        sql = path.read_text()
        if wh.kind == "duckdb":
            sql = to_duckdb(sql)
        for stmt in _split_statements(sql):
            wh.execute(stmt)
        print(f"[migrate] applied {path.name}")
