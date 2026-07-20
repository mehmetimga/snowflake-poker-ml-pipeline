"""Train, calibrate, evaluate, and export the Phase 8 pair CatBoost model."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.pair_train import PairTrainingConfig, train_pair_catboost


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-v1")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("models/pair-catboost-v1")
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship"),
        default="cold_start",
    )
    parser.add_argument("--iterations", type=int, default=500)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--early-stopping-rounds", type=int, default=80)
    parser.add_argument("--positive-class-weight", type=float, default=100.0)
    parser.add_argument("--max-alert-rate", type=float, default=0.02)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    metrics = train_pair_catboost(
        PairTrainingConfig(
            dataset_dir=args.dataset,
            output_dir=args.output_dir,
            benchmark=args.benchmark,
            iterations=args.iterations,
            depth=args.depth,
            learning_rate=args.learning_rate,
            early_stopping_rounds=args.early_stopping_rounds,
            positive_class_weight=args.positive_class_weight,
            max_alert_rate=args.max_alert_rate,
            random_seed=args.random_seed,
            overwrite=args.overwrite,
        )
    )
    gate = metrics["quality_gate"]
    challenge = metrics["reports"]["catboost"]["challenge"]
    print(
        f"[pair-catboost] run={metrics['run_id']} benchmark={metrics['benchmark']} "
        f"challenge_pr_auc={challenge['pr_auc']} challenge_f1={challenge['f1']:.4f} "
        f"onnx_p95_ms={metrics['onnx_latency']['p95_ms']:.3f} "
        f"promotion_eligible={gate['promotion_eligible']}"
    )
    if gate["reasons"]:
        print(f"[pair-catboost] quality-gate: {'; '.join(gate['reasons'])}")
    print(f"[pair-catboost] artifacts={args.output_dir}")


if __name__ == "__main__":
    main()
