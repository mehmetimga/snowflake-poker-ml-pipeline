"""Build generator-seed and leave-one-scenario-family-out evidence."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.scenario_holdout import (
    ScenarioHoldoutConfig,
    build_scenario_holdout_report,
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
        "--source-world", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/registry/scenario_holdout_report.json"),
    )
    parser.add_argument(
        "--lineage",
        type=Path,
        default=Path("models/registry/generator_scenario_lineage.parquet"),
    )
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--bootstrap-samples", type=int, default=300)
    parser.add_argument("--bootstrap-seed", type=int, default=7300)
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--minimum-scenario-positives", type=int, default=10)
    args = parser.parse_args()
    report = build_scenario_holdout_report(
        args.dataset,
        args.model_dir,
        args.source_world,
        args.output,
        args.lineage,
        config=ScenarioHoldoutConfig(
            random_seed=args.random_seed,
            bootstrap_samples=args.bootstrap_samples,
            bootstrap_seed=args.bootstrap_seed,
            confidence_level=args.confidence_level,
            minimum_scenario_positives=args.minimum_scenario_positives,
        ),
    )
    summary = report["summary"]
    print(
        f"[scenario-holdout] model={report['champion']['model_name']}:"
        f"{report['champion']['run_id']} families={summary['families_evaluated']} "
        f"holdout_pr_auc=[{summary['minimum_scenario_holdout_pr_auc']:.6f},"
        f"{summary['maximum_scenario_holdout_pr_auc']:.6f}] "
        f"generator_seeds={len(report['generator_seed_holdouts'])} "
        f"challenge_read=false production_changed=false output={args.output}"
    )


if __name__ == "__main__":
    main()
