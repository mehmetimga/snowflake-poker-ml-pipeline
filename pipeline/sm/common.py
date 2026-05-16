"""Shared helpers for SageMaker entrypoints."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def sm_model_dir() -> Path:
    """SageMaker mounts /opt/ml/model; the contents are uploaded to the
    estimator's `output_path` as `model.tar.gz` when the job completes."""
    p = Path(os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def sm_output_dir() -> Path:
    """Auxiliary output channel — uploaded as `output.tar.gz`."""
    p = Path(os.environ.get("SM_OUTPUT_DATA_DIR", "/opt/ml/output/data"))
    p.mkdir(parents=True, exist_ok=True)
    return p


def copy_models_dir_to(sm_dir: Path) -> None:
    """Copy whatever the training function wrote to settings.models_dir into
    SageMaker's model dir so the job artifact gets captured."""
    from pipeline.config import get_settings

    src = Path(get_settings().models_dir)
    if not src.exists():
        return
    for entry in src.iterdir():
        dst = sm_dir / entry.name
        if entry.is_dir():
            shutil.copytree(entry, dst, dirs_exist_ok=True)
        else:
            shutil.copy2(entry, dst)


def configure_models_dir() -> None:
    """Force MODELS_DIR=/opt/ml/model so training writes directly to the
    SageMaker artifact location and we skip the copy step entirely."""
    os.environ["MODELS_DIR"] = str(sm_model_dir())
