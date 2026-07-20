"""Train and evaluate the leakage-safe Phase 12 OOF stacker."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.ensemble import EnsembleTrainingConfig, train_oof_ensemble


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/datasets/pair-full-v2"))
    parser.add_argument("--champion-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    parser.add_argument("--output-dir", type=Path, default=Path("models/pair-ensemble-full-v2"))
    parser.add_argument("--folds", type=int, default=5)
    parser.add_argument("--catboost-iterations", type=int)
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--learning-rate", type=float, default=0.03)
    parser.add_argument("--positive-class-weight", type=float, default=100.0)
    parser.add_argument("--max-alert-rate", type=float, default=0.02)
    parser.add_argument("--minimum-relative-pr-gain", type=float, default=0.02)
    parser.add_argument("--bootstrap-samples", type=int, default=500)
    parser.add_argument("--random-seed", type=int, default=42)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    metrics = train_oof_ensemble(
        EnsembleTrainingConfig(
            dataset_dir=args.dataset,
            champion_dir=args.champion_dir,
            output_dir=args.output_dir,
            folds=args.folds,
            catboost_iterations=args.catboost_iterations,
            depth=args.depth,
            learning_rate=args.learning_rate,
            positive_class_weight=args.positive_class_weight,
            max_alert_rate=args.max_alert_rate,
            minimum_relative_pr_gain=args.minimum_relative_pr_gain,
            bootstrap_samples=args.bootstrap_samples,
            random_seed=args.random_seed,
            overwrite=args.overwrite,
        )
    )
    gate = metrics["quality_gate"]
    print(
        f"[pair-ensemble] run={metrics['run_id']} "
        f"test_pr_auc={metrics['reports']['test']['pr_auc']:.6f} "
        f"test_f1={metrics['reports']['test']['f1']:.6f} "
        f"promotion_candidate={gate['promotion_candidate']}"
    )
    if gate["reasons"]:
        print(f"[pair-ensemble] quality-gate: {'; '.join(gate['reasons'])}")
    print(f"[pair-ensemble] artifacts={args.output_dir}")


if __name__ == "__main__":
    main()
