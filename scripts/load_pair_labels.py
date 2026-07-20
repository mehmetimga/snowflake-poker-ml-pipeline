"""Load restricted pair-label Parquet sidecars into the warehouse."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from pipeline.warehouse import get_warehouse
from pipeline.warehouse.migrate import run_migrations
from pipeline.warehouse.pair_labels import load_pair_labels


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/pair-v1"),
    )
    parser.add_argument(
        "--splits",
        nargs="+",
        default=["train", "validation", "test"],
    )
    parser.add_argument("--include-challenge-private", action="store_true")
    parser.add_argument("--migrate", action="store_true")
    args = parser.parse_args()

    paths = [
        args.dataset
        / "benchmarks"
        / "cold_start"
        / split
        / ("private_labels" if split == "challenge" else "labels")
        / "pair_labels.parquet"
        for split in args.splits
    ]
    if args.include_challenge_private and "challenge" not in args.splits:
        paths.append(
            args.dataset
            / "benchmarks"
            / "cold_start"
            / "challenge"
            / "private_labels"
            / "pair_labels.parquet"
        )
    missing = [path for path in paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"missing pair-label sidecars: {missing}")

    rows = []
    for path in paths:
        frame = pd.read_parquet(path)
        rows.extend(frame.to_dict(orient="records"))
    warehouse = get_warehouse()
    try:
        if args.migrate:
            run_migrations(warehouse)
        loaded = load_pair_labels(warehouse, rows)
    finally:
        warehouse.close()
    print(f"[pair-labels] loaded={loaded} files={len(paths)}")


if __name__ == "__main__":
    main()
