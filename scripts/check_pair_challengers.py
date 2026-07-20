"""Verify Phase 9 challenger artifacts, hashes, split boundaries, and gates."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from pipeline.dl.tabular_models import MODEL_NAMES


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("models/pair-challengers-full-v2"),
    )
    args = parser.parse_args()
    root = args.model_dir.resolve()
    summary_path = root / "summary.json"
    manifest_path = root / "artifact_manifest.json"
    predictions_path = root / "predictions.parquet"
    for path in (summary_path, manifest_path, predictions_path):
        if not path.is_file():
            raise FileNotFoundError(path)
    summary = json.loads(summary_path.read_text())
    manifest = json.loads(manifest_path.read_text())
    if summary["run_id"] != manifest["run_id"]:
        raise ValueError("summary and artifact manifest run IDs disagree")
    if summary["challenge_labels_used"] is not False:
        raise ValueError("Phase 9 DGX run must not use private challenge labels")
    if summary["feature_definition_version"] != "pair-features-v1":
        raise ValueError("unexpected feature definition")
    if set(summary["models"]) != set(MODEL_NAMES):
        raise ValueError("full Phase 9 run must contain all challenger models")

    tracked = manifest["artifacts"]
    actual_files = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    if actual_files != set(tracked):
        raise ValueError("artifact manifest file set disagrees with the model directory")
    for relative, expected in tracked.items():
        if _sha256(root / relative) != expected:
            raise ValueError(f"challenger artifact hash mismatch: {relative}")

    predictions = pd.read_parquet(predictions_path)
    if set(predictions["split"].astype(str)) != {"validation", "test"}:
        raise ValueError("challenger predictions contain a non-public split")
    if set(predictions["model_name"].astype(str)) != set(MODEL_NAMES):
        raise ValueError("challenger predictions are missing a model")
    if predictions.duplicated(["model_name", "split", "event_id"]).any():
        raise ValueError("challenger predictions contain duplicate event IDs")
    for model_name in MODEL_NAMES:
        model_result = summary["models"][model_name]
        if model_result["quality_gate"]["promotion_eligible"] is not False:
            raise ValueError("private challenge evaluation is required before promotion")
        for split in ("validation", "test"):
            expected = summary["counts"][split]["rows"]
            actual = int(
                (
                    (predictions["model_name"] == model_name)
                    & (predictions["split"] == split)
                ).sum()
            )
            if actual != expected:
                raise ValueError(
                    f"{model_name}/{split} predictions={actual}; expected {expected}"
                )

    compact = {
        name: {
            "best_epoch": result["best_epoch"],
            "test_pr_auc": result["reports"]["test"]["pr_auc"],
            "test_f1": result["reports"]["test"]["f1"],
            "recall_at_alert_budget": result["reports"]["test"][
                "recall_at_alert_budget"
            ],
            "p95_ms": result["latency"]["p95_ms"],
            "promotion_candidate": result["quality_gate"][
                "promotion_candidate"
            ],
        }
        for name, result in summary["models"].items()
    }
    print(
        "[pair-challengers-check] "
        + json.dumps(
            {
                "run_id": summary["run_id"],
                "artifact_hashes": "passed",
                "split_boundaries": "passed",
                "challenge_labels_used": False,
                "models": compact,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
