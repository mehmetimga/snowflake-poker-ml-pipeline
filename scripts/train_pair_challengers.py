"""Train Phase 9 neural tabular challengers on frozen pair datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.pair_challengers import PairChallengerConfig, train_pair_challengers
from pipeline.dl.tabular_models import MODEL_NAMES


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--baseline-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("models/pair-challengers-full-v2")
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship"),
        default="cold_start",
    )
    parser.add_argument(
        "--models",
        default=",".join(MODEL_NAMES),
        help=f"comma-separated subset of {','.join(MODEL_NAMES)}",
    )
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--patience", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--positive-class-weight", type=float, default=100.0)
    parser.add_argument("--max-alert-rate", type=float, default=0.02)
    parser.add_argument("--minimum-relative-pr-gain", type=float, default=0.02)
    parser.add_argument("--bootstrap-samples", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    models = tuple(value.strip() for value in args.models.split(",") if value.strip())

    summary = train_pair_challengers(
        PairChallengerConfig(
            dataset_dir=args.dataset,
            baseline_dir=args.baseline_dir,
            output_dir=args.output_dir,
            benchmark=args.benchmark,
            models=models,
            epochs=args.epochs,
            batch_size=args.batch_size,
            patience=args.patience,
            learning_rate=args.learning_rate,
            weight_decay=args.weight_decay,
            positive_class_weight=args.positive_class_weight,
            max_alert_rate=args.max_alert_rate,
            minimum_relative_pr_gain=args.minimum_relative_pr_gain,
            bootstrap_samples=args.bootstrap_samples,
            random_seed=args.seed,
            num_workers=args.num_workers,
            device_name=args.device,
            overwrite=args.overwrite,
        )
    )
    print(
        f"[pair-challengers] run={summary['run_id']} "
        f"device={summary['device']} benchmark={summary['benchmark']}"
    )
    for name, result in summary["models"].items():
        report = result["reports"]["test"]
        gate = result["quality_gate"]
        print(
            f"[pair-challengers] model={name} pr_auc={report['pr_auc']:.6f} "
            f"f1={report['f1']:.6f} "
            f"recall_at_budget={report['recall_at_alert_budget']:.6f} "
            f"promotion_candidate={gate['promotion_candidate']}"
        )


if __name__ == "__main__":
    main()
