from __future__ import annotations

from pathlib import Path

import pytest

from pipeline.warehouse.artifacts import upload_model_artifacts


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[str] = []
        self.closed = False

    def execute(self, sql: str) -> None:
        self.statements.append(sql)

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self._cursor = cursor

    def cursor(self) -> _Cursor:
        return self._cursor


class _Warehouse:
    def __init__(self, cursor: _Cursor) -> None:
        self.conn = _Connection(cursor)


def test_upload_model_artifacts_uses_safe_uncompressed_put(tmp_path: Path):
    (tmp_path / "b.onnx").write_bytes(b"b")
    (tmp_path / "a.json").write_text("{}")
    cursor = _Cursor()

    count = upload_model_artifacts(
        _Warehouse(cursor), tmp_path, "@POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS"
    )

    assert count == 2
    assert cursor.closed
    assert "a.json" in cursor.statements[0]
    assert "b.onnx" in cursor.statements[1]
    assert all(
        statement.endswith(
            "@POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        for statement in cursor.statements
    )


def test_upload_model_artifacts_rejects_sql_in_stage_name(tmp_path: Path):
    with pytest.raises(ValueError):
        upload_model_artifacts(_Warehouse(_Cursor()), tmp_path, "stage; DROP DATABASE demo")
