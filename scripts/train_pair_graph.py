"""Train the Phase 11 inductive graph models."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.graph_dataset import GRAPH_BENCHMARKS
from pipeline.dl.graph_train import GraphTrainingConfig, train_graph_models


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--graph-dataset", type=Path, default=Path("data/datasets/pair-graph-full-v2")
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--cold-start-baseline", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--new-relationship-baseline",
        type=Path,
        default=Path("models/pair-catboost-new-relationship-v2"),
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("models/pair-graph-full-v2")
    )
    parser.add_argument("--benchmarks", default=",".join(GRAPH_BENCHMARKS))
    parser.add_argument("--epochs", type=int, default=15)
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
    parser.add_argument("--graph-width", type=int, default=64)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    benchmarks = tuple(
        value.strip() for value in args.benchmarks.split(",") if value.strip()
    )
    summary = train_graph_models(
        GraphTrainingConfig(
            graph_dataset_dir=args.graph_dataset,
            pair_dataset_dir=args.pair_dataset,
            cold_start_baseline_dir=args.cold_start_baseline,
            new_relationship_baseline_dir=args.new_relationship_baseline,
            output_dir=args.output_dir,
            benchmarks=benchmarks,
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
            graph_width=args.graph_width,
            device_name=args.device,
            overwrite=args.overwrite,
        )
    )
    print(
        f"[pair-graph] run={summary['run_id']} device={summary['device']} "
        f"stable_incremental_lift={summary['stable_incremental_lift']}"
    )
    for benchmark, result in summary["benchmarks"].items():
        report = result["reports"]["test"]
        print(
            f"[pair-graph] benchmark={benchmark} pr_auc={report['pr_auc']:.6f} "
            f"f1={report['f1']:.6f} "
            f"promotion_candidate={result['quality_gate']['promotion_candidate']}"
        )


if __name__ == "__main__":
    main()
