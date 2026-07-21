"""Validate multi-seed robustness evidence and its leakage boundary."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.seed_stability import validate_seed_stability_report


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
        default=Path("models/registry/validation_seed_stability.json"),
    )
    parser.add_argument(
        "--recompute",
        action="store_true",
        help="Retrain every seed and require byte-equivalent numeric evidence.",
    )
    args = parser.parse_args()
    result = validate_seed_stability_report(
        args.dataset,
        args.model_dir,
        args.report,
        recompute=args.recompute,
    )
    print(
        f"[validation-seed-stability-check] model={result['model_name']}:"
        f"{result['run_id']} seeds={result['seeds']} "
        f"validation_pr_auc=[{result['minimum_pr_auc']:.6f},"
        f"{result['maximum_pr_auc']:.6f}] "
        f"relative_spread={result['relative_pr_auc_spread']:.6f} "
        f"status={result['status']} integrity=passed hashes=passed "
        f"recompute={'passed' if result['recomputed'] else 'not_requested'} "
        "test_read=false challenge_read=false"
    )


if __name__ == "__main__":
    main()
