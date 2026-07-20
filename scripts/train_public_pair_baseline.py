"""Train a CatBoost comparison without opening the private challenge."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.public_pair_baseline import (
    PublicPairBaselineConfig,
    train_public_pair_baseline,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("models/pair-catboost-new-relationship-v2"),
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship"),
        default="new_relationship",
    )
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--early-stopping-rounds", type=int, default=80)
    parser.add_argument("--positive-class-weight", type=float, default=100.0)
    parser.add_argument("--max-alert-rate", type=float, default=0.02)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    metrics = train_public_pair_baseline(
        PublicPairBaselineConfig(
            dataset_dir=args.dataset,
            output_dir=args.output_dir,
            benchmark=args.benchmark,
            iterations=args.iterations,
            depth=args.depth,
            learning_rate=args.learning_rate,
            early_stopping_rounds=args.early_stopping_rounds,
            positive_class_weight=args.positive_class_weight,
            max_alert_rate=args.max_alert_rate,
            random_seed=args.seed,
            overwrite=args.overwrite,
        )
    )
    report = metrics["reports"]["catboost"]["test"]
    print(
        f"[public-pair-baseline] run={metrics['run_id']} "
        f"benchmark={metrics['benchmark']} pr_auc={report['pr_auc']:.6f} "
        f"f1={report['f1']:.6f} challenge_read=false"
    )


if __name__ == "__main__":
    main()
