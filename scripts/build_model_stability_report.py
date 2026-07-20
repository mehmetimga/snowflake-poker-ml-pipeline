"""Build the public-test, hand-grouped champion stability report."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.stability import StabilityConfig, build_stability_report


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
        default=Path("models/registry/stability_report.json"),
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship"),
        default="cold_start",
    )
    parser.add_argument(
        "--split", choices=("validation", "test"), default="test"
    )
    parser.add_argument("--bootstrap-samples", type=int, default=1000)
    parser.add_argument("--confidence-level", type=float, default=0.95)
    parser.add_argument("--random-seed", type=int, default=42)
    args = parser.parse_args()
    report = build_stability_report(
        args.dataset,
        args.model_dir,
        args.output,
        config=StabilityConfig(
            benchmark=args.benchmark,
            split=args.split,
            bootstrap_samples=args.bootstrap_samples,
            confidence_level=args.confidence_level,
            random_seed=args.random_seed,
        ),
    )
    interval = report["bootstrap"]["metrics"]["pr_auc"]
    print(
        f"[model-stability] model={report['model']['model_name']} "
        f"run={report['model']['run_id']} split={args.split} "
        f"hands={report['counts']['hands']} rows={report['counts']['rows']} "
        f"pr_auc={report['point_metrics']['pr_auc']:.6f} "
        f"ci=[{interval['lower']:.6f},{interval['upper']:.6f}] "
        f"bootstrap={args.bootstrap_samples} sampling_unit=hand_id "
        f"output={args.output}"
    )


if __name__ == "__main__":
    main()
