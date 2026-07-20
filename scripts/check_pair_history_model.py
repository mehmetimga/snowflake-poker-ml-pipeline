"""Verify Phase 10 model hashes, evaluation splits, and promotion boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pipeline.dl.history_dataset import sha256_file
from pipeline.dl.history_train import MODEL_NAME


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-history-full-v2")
    )
    args = parser.parse_args()
    root = args.model_dir.resolve()
    summary = json.loads((root / "summary.json").read_text())
    manifest = json.loads((root / "artifact_manifest.json").read_text())
    if summary["run_id"] != manifest["run_id"] or summary["phase"] != 10:
        raise ValueError("Phase 10 artifact identity mismatch")
    for key in (
        "challenge_artifacts_read",
        "challenge_labels_used",
        "pretraining_labels_used",
    ):
        if summary[key] is not False:
            raise ValueError(f"Phase 10 safety boundary failed: {key}")
    actual_files = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    if actual_files != set(manifest["artifacts"]):
        raise ValueError("Phase 10 artifact file set mismatch")
    for relative, expected in manifest["artifacts"].items():
        if sha256_file(root / relative) != expected:
            raise ValueError(f"Phase 10 model hash mismatch: {relative}")
    predictions = pd.read_parquet(root / "predictions.parquet")
    if set(predictions["split"].astype(str)) != {"validation", "test"}:
        raise ValueError("Phase 10 predictions contain a forbidden split")
    if set(predictions["model_name"].astype(str)) != {MODEL_NAME}:
        raise ValueError("Phase 10 predictions contain an unexpected model")
    if predictions.duplicated(["split", "event_id"]).any():
        raise ValueError("Phase 10 predictions contain duplicate events")
    for split in ("validation", "test"):
        actual = int((predictions["split"] == split).sum())
        expected = int(summary["counts"][split]["rows"])
        if actual != expected:
            raise ValueError(f"{split} predictions={actual}; expected={expected}")
    if summary["model"]["quality_gate"]["promotion_eligible"] is not False:
        raise ValueError("private challenge evaluation is required before promotion")
    result = summary["model"]
    print(
        "[pair-history-check] "
        + json.dumps(
            {
                "run_id": summary["run_id"],
                "artifact_hashes": "passed",
                "public_splits_only": "passed",
                "pretraining_labels_used": False,
                "test_pr_auc": result["reports"]["test"]["pr_auc"],
                "test_f1": result["reports"]["test"]["f1"],
                "recall_at_alert_budget": result["reports"]["test"][
                    "recall_at_alert_budget"
                ],
                "promotion_candidate": result["quality_gate"]["promotion_candidate"],
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
