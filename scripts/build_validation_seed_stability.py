"""Build validation-only multi-seed robustness evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.seed_stability import (
    SeedStabilityConfig,
    build_seed_stability_report,
    parse_seeds,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/registry/validation_seed_stability.json"),
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship"),
        default="cold_start",
    )
    parser.add_argument("--seeds", default="11,23,42,67,101")
    parser.add_argument("--maximum-relative-pr-auc-spread", type=float, default=0.25)
    parser.add_argument(
        "--minimum-pr-auc-prevalence-multiple", type=float, default=2.0
    )
    args = parser.parse_args()
    report = build_seed_stability_report(
        args.dataset,
        args.model_dir,
        args.output,
        config=SeedStabilityConfig(
            benchmark=args.benchmark,
            seeds=parse_seeds(args.seeds),
            maximum_relative_pr_auc_spread=args.maximum_relative_pr_auc_spread,
            minimum_pr_auc_prevalence_multiple=(
                args.minimum_pr_auc_prevalence_multiple
            ),
        ),
    )
    summary = report["metric_summaries"]["pr_auc"]
    print(
        f"[validation-seed-stability] model={report['champion']['model_name']}:"
        f"{report['champion']['run_id']} seeds={len(report['seed_results'])} "
        f"validation_pr_auc=[{summary['minimum']:.6f},{summary['maximum']:.6f}] "
        f"relative_spread={report['robustness']['validation_pr_auc_relative_spread']:.6f} "
        f"status={report['robustness']['status']} "
        f"test_read=false challenge_read=false output={args.output}"
    )


if __name__ == "__main__":
    main()
