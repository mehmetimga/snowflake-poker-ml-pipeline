"""Train the Phase 10 pretrained multi-hand pair-risk model."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.history_train import PairHistoryConfig, train_pair_history_model


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--history-dataset",
        type=Path,
        default=Path("data/datasets/pair-sequences-full-v2"),
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--baseline-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("models/pair-history-full-v2")
    )
    parser.add_argument("--pretrain-epochs", type=int, default=5)
    parser.add_argument("--epochs", type=int, default=15)
    parser.add_argument("--pretrain-batch-size", type=int, default=512)
    parser.add_argument("--batch-size", type=int, default=1024)
    parser.add_argument("--patience", type=int, default=4)
    parser.add_argument("--learning-rate", type=float, default=1e-3)
    parser.add_argument("--pretrain-learning-rate", type=float, default=1e-3)
    parser.add_argument("--weight-decay", type=float, default=1e-4)
    parser.add_argument("--positive-class-weight", type=float, default=100.0)
    parser.add_argument("--max-alert-rate", type=float, default=0.02)
    parser.add_argument("--minimum-relative-pr-gain", type=float, default=0.02)
    parser.add_argument("--bootstrap-samples", type=int, default=200)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--num-workers", type=int, default=4)
    parser.add_argument("--encoder-width", type=int, default=32)
    parser.add_argument("--encoder-heads", type=int, default=4)
    parser.add_argument("--encoder-layers", type=int, default=2)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    summary = train_pair_history_model(
        PairHistoryConfig(
            history_dataset_dir=args.history_dataset,
            pair_dataset_dir=args.pair_dataset,
            baseline_dir=args.baseline_dir,
            output_dir=args.output_dir,
            pretrain_epochs=args.pretrain_epochs,
            epochs=args.epochs,
            pretrain_batch_size=args.pretrain_batch_size,
            batch_size=args.batch_size,
            patience=args.patience,
            learning_rate=args.learning_rate,
            pretrain_learning_rate=args.pretrain_learning_rate,
            weight_decay=args.weight_decay,
            positive_class_weight=args.positive_class_weight,
            max_alert_rate=args.max_alert_rate,
            minimum_relative_pr_gain=args.minimum_relative_pr_gain,
            bootstrap_samples=args.bootstrap_samples,
            random_seed=args.seed,
            num_workers=args.num_workers,
            encoder_width=args.encoder_width,
            encoder_heads=args.encoder_heads,
            encoder_layers=args.encoder_layers,
            device_name=args.device,
            overwrite=args.overwrite,
        )
    )
    report = summary["model"]["reports"]["test"]
    print(
        f"[pair-history] run={summary['run_id']} device={summary['device']} "
        f"pr_auc={report['pr_auc']:.6f} f1={report['f1']:.6f}"
    )


if __name__ == "__main__":
    main()
