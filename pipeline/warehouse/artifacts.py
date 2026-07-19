from __future__ import annotations

import re
from pathlib import Path

from pipeline.warehouse.factory import Warehouse


_STAGE_NAME = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_$]*(?:\.[A-Za-z_][A-Za-z0-9_$]*){0,2}$"
)


def upload_model_artifacts(warehouse: Warehouse, models_dir: Path, stage: str) -> int:
    """Upload model files to an internal Snowflake stage without compression."""
    stage_name = stage.removeprefix("@").strip()
    if not _STAGE_NAME.fullmatch(stage_name):
        raise ValueError(f"Invalid Snowflake stage name: {stage!r}")
    if not models_dir.is_dir():
        raise FileNotFoundError(f"Models directory does not exist: {models_dir}")
    if not hasattr(warehouse, "conn"):
        raise TypeError("Artifact upload requires the Snowflake warehouse adapter")

    files = sorted(path for path in models_dir.iterdir() if path.is_file())
    cursor = warehouse.conn.cursor()  # type: ignore[attr-defined]
    try:
        for path in files:
            uri = path.resolve().as_uri().replace("'", "''")
            cursor.execute(
                f"PUT '{uri}' @{stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
    finally:
        cursor.close()
    return len(files)
