"""Verify champion stability evidence against frozen public artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.stability import validate_stability_report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("models/registry/stability_report.json"),
    )
    args = parser.parse_args()
    result = validate_stability_report(args.dataset, args.model_dir, args.report)
    interval = result["pr_auc_interval"]
    print(
        f"[model-stability-check] model={result['model_name']}:{result['run_id']} "
        f"split={result['split']} hands={result['hands']} rows={result['rows']} "
        f"pr_auc={result['pr_auc']:.6f} "
        f"ci=[{interval['lower']:.6f},{interval['upper']:.6f}] "
        f"bootstrap={result['bootstrap_samples']} hashes=passed "
        "recompute=passed challenge_read=false"
    )


if __name__ == "__main__":
    main()
