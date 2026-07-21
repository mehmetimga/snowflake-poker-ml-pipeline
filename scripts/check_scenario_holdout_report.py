"""Validate scenario holdout artifacts, lineage, and leakage controls."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.scenario_holdout import validate_scenario_holdout_report


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--source-world", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=Path("models/registry/scenario_holdout_report.json"),
    )
    parser.add_argument(
        "--lineage",
        type=Path,
        default=Path("models/registry/generator_scenario_lineage.parquet"),
    )
    parser.add_argument("--recompute", action="store_true")
    args = parser.parse_args()
    result = validate_scenario_holdout_report(
        args.dataset,
        args.model_dir,
        args.source_world,
        args.report,
        args.lineage,
        recompute=args.recompute,
    )
    print(
        f"[scenario-holdout-check] model={result['model_name']}:"
        f"{result['run_id']} families={result['families']} "
        f"holdout_pr_auc=[{result['minimum_pr_auc']:.6f},"
        f"{result['maximum_pr_auc']:.6f}] integrity=passed lineage=passed "
        f"recompute={'passed' if result['recomputed'] else 'not_requested'} "
        "challenge_read=false leakage=passed"
    )


if __name__ == "__main__":
    main()
