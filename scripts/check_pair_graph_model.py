"""Verify Phase 11 graph artifacts and multi-benchmark promotion gates."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pipeline.dl.graph_dataset import GRAPH_BENCHMARKS, sha256_file
from pipeline.dl.graph_train import MODEL_NAME


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-graph-full-v2")
    )
    args = parser.parse_args()
    root = args.model_dir.resolve()
    summary = json.loads((root / "summary.json").read_text())
    manifest = json.loads((root / "artifact_manifest.json").read_text())
    if summary["phase"] != 11 or summary["run_id"] != manifest["run_id"]:
        raise ValueError("Phase 11 artifact identity mismatch")
    if summary["challenge_artifacts_read"] is not False or summary[
        "challenge_labels_used"
    ] is not False:
        raise ValueError("Phase 11 challenge boundary failed")
    if summary["raw_id_embedding_count"] != 0:
        raise ValueError("Phase 11 model uses raw-ID embeddings")
    if set(summary["benchmarks"]) != set(GRAPH_BENCHMARKS):
        raise ValueError("Phase 11 full run is missing a benchmark")
    actual_files = {
        str(path.relative_to(root))
        for path in root.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    if actual_files != set(manifest["artifacts"]):
        raise ValueError("Phase 11 artifact file set mismatch")
    for relative, expected in manifest["artifacts"].items():
        if sha256_file(root / relative) != expected:
            raise ValueError(f"Phase 11 artifact hash mismatch: {relative}")
    predictions = pd.read_parquet(root / "predictions.parquet")
    if set(predictions["benchmark"].astype(str)) != set(GRAPH_BENCHMARKS):
        raise ValueError("Phase 11 predictions are missing a benchmark")
    if set(predictions["split"].astype(str)) != {"validation", "test"}:
        raise ValueError("Phase 11 predictions contain a forbidden split")
    if set(predictions["model_name"].astype(str)) != {MODEL_NAME}:
        raise ValueError("Phase 11 predictions contain an unexpected model")
    if predictions.duplicated(["benchmark", "split", "event_id"]).any():
        raise ValueError("Phase 11 predictions contain duplicate events")
    compact = {}
    for benchmark, result in summary["benchmarks"].items():
        if result["raw_id_embedding_count"] != 0:
            raise ValueError("benchmark model contains raw-ID embeddings")
        if result["quality_gate"]["promotion_eligible"] is not False:
            raise ValueError("private challenge evaluation is required before promotion")
        for split in ("validation", "test"):
            expected = int(result["counts"][split]["rows"])
            actual = int(
                (
                    (predictions["benchmark"] == benchmark)
                    & (predictions["split"] == split)
                ).sum()
            )
            if actual != expected:
                raise ValueError(
                    f"{benchmark}/{split} predictions={actual}; expected={expected}"
                )
        compact[benchmark] = {
            "test_pr_auc": result["reports"]["test"]["pr_auc"],
            "test_f1": result["reports"]["test"]["f1"],
            "recall_at_alert_budget": result["reports"]["test"][
                "recall_at_alert_budget"
            ],
            "promotion_candidate": result["quality_gate"]["promotion_candidate"],
        }
    print(
        "[pair-graph-check] "
        + json.dumps(
            {
                "run_id": summary["run_id"],
                "artifact_hashes": "passed",
                "public_splits_only": "passed",
                "raw_id_embedding_count": 0,
                "stable_incremental_lift": summary["stable_incremental_lift"],
                "benchmarks": compact,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
