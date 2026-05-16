"""Translate Snowflake DDL to DuckDB-compatible DDL.

Same .sql migration files are written once (Snowflake-flavored) and translated
on the fly when the warehouse backend is DuckDB.
"""

from __future__ import annotations

import re


_DUCKDB_SUBSTITUTIONS = [
    (re.compile(r"\bVARIANT\b", re.IGNORECASE), "JSON"),
    (re.compile(r"\bTIMESTAMP_TZ\b", re.IGNORECASE), "TIMESTAMP"),
    (re.compile(r"\bARRAY\b(?!\s*\[)", re.IGNORECASE), "JSON"),
    (re.compile(r"\s+CLUSTER\s+BY\s*\([^)]*\)", re.IGNORECASE), ""),
    (re.compile(r"\s+CHANGE_TRACKING\s*=\s*TRUE", re.IGNORECASE), ""),
]


def to_duckdb(sql: str) -> str:
    out = sql
    for pattern, repl in _DUCKDB_SUBSTITUTIONS:
        out = pattern.sub(repl, out)
    return out
