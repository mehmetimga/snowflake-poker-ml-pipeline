"""Build the Phase 10 strictly point-in-time multi-hand dataset."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.history_dataset import HistoryDatasetConfig, build_history_dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-dir", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/datasets/pair-sequences-full-v2"),
    )
    parser.add_argument("--max-history", type=int, default=16)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    manifest = build_history_dataset(
        HistoryDatasetConfig(
            source_dir=args.source_dir,
            pair_dataset_dir=args.pair_dataset,
            output_dir=args.output_dir,
            max_history=args.max_history,
            overwrite=args.overwrite,
        )
    )
    print(
        f"[pair-history-dataset] dataset={manifest['dataset_id']} "
        f"max_history={manifest['max_history']} challenge_read=false"
    )


if __name__ == "__main__":
    main()
