"""Run the existing training pipeline as a Snowflake container job."""

from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys
import tempfile


def main() -> None:
    models_dir = Path(
        os.environ.get("ML_JOB_MODELS_DIR")
        or tempfile.mkdtemp(prefix="poker-models-")
    )
    models_dir.mkdir(parents=True, exist_ok=True)
    os.environ["MODELS_DIR"] = str(models_dir)

    train_script = Path(__file__).with_name("train.py")
    subprocess.run([sys.executable, str(train_script)], check=True)

    # Import only after MODELS_DIR is set because Settings is process-cached.
    from pipeline.config import get_settings
    from pipeline.warehouse import get_warehouse
    from pipeline.warehouse.artifacts import upload_model_artifacts

    settings = get_settings()
    warehouse = get_warehouse()
    try:
        count = upload_model_artifacts(
            warehouse,
            models_dir=models_dir,
            stage=settings.snowflake_model_stage,
        )
    finally:
        warehouse.close()
    print(
        f"[snowflake-ml] uploaded {count} model artifacts "
        f"to @{settings.snowflake_model_stage}"
    )


if __name__ == "__main__":
    main()
