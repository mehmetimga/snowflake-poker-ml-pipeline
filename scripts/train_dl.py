"""Train leakage-safe sequence models from a portable dataset bundle."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.dataset import load_sequence_partitions
from pipeline.dl.train import train_sequence_models_from_partitions


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/dgx-v1/dl_sequences.npz"),
    )
    parser.add_argument("--output-dir", type=Path, default=Path("models/dgx"))
    parser.add_argument("--epochs", type=int, default=20)
    parser.add_argument("--batch-size", type=int, default=512)
    parser.add_argument("--patience", type=int, default=4)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--device", choices=("auto", "cpu", "cuda"), default="auto")
    args = parser.parse_args()

    partitions = load_sequence_partitions(args.dataset)
    train_sequence_models_from_partitions(
        partitions,
        output_dir=args.output_dir,
        epochs=args.epochs,
        batch_size=args.batch_size,
        patience=args.patience,
        random_seed=args.seed,
        device_name=args.device,
    )


if __name__ == "__main__":
    main()
