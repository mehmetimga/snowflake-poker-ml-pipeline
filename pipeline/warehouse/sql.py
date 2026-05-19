from __future__ import annotations

import re
from collections.abc import Iterable

from pipeline.warehouse.factory import Warehouse


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def unique_strings(values: Iterable[object]) -> list[str]:
    return sorted({str(value) for value in values if value is not None})


def sql_string_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def sql_string_list(values: Iterable[object]) -> str:
    items = unique_strings(values)
    if not items:
        raise ValueError("Cannot build SQL IN list from an empty sequence.")
    return ", ".join(sql_string_literal(value) for value in items)


def validate_identifier(identifier: str) -> str:
    if not _IDENTIFIER_RE.match(identifier):
        raise ValueError(f"Invalid SQL identifier: {identifier!r}")
    return identifier


def delete_by_values(
    warehouse: Warehouse,
    table: str,
    column: str,
    values: Iterable[object],
) -> int:
    items = unique_strings(values)
    if not items:
        return 0
    table_name = validate_identifier(table)
    column_name = validate_identifier(column)
    warehouse.execute(f"DELETE FROM {table_name} WHERE {column_name} IN ({sql_string_list(items)})")
    return len(items)
