"""Shared dataset-use guardrails for builders and model trainers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def load_dataset_manifest(root: Path) -> dict[str, Any]:
    path = root.resolve() / "manifest.json"
    if not path.is_file():
        raise FileNotFoundError(f"missing dataset manifest: {path}")
    return json.loads(path.read_text())


def assert_training_allowed(root: Path) -> dict[str, Any]:
    """Reject products explicitly sealed for replay/acceptance use only."""

    manifest = load_dataset_manifest(root)
    if (
        manifest.get("training_allowed") is False
        or manifest.get("product_type") == "alert_acceptance"
        or "model_training" in manifest.get("forbidden_uses", ())
    ):
        raise ValueError(
            "alert-acceptance datasets are prohibited from model training, "
            "validation, testing, calibration, and promotion"
        )
    return manifest
