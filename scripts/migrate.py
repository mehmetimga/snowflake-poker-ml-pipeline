"""Apply SQL migrations to the configured warehouse."""

from __future__ import annotations

from pipeline.warehouse.migrate import run_migrations

if __name__ == "__main__":
    run_migrations()
    print("[migrate] done")
