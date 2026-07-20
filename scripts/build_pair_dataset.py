"""Build frozen pair benchmarks and portable DGX Parquet files."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.pair_dataset import PairDatasetBuildConfig, build_pair_datasets


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/datasets/context-v1"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/datasets/pair-v1"),
    )
    parser.add_argument("--temporal-source-split", default="train")
    parser.add_argument("--new-relationship-source-split", default="train")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    manifest = build_pair_datasets(
        PairDatasetBuildConfig(
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            temporal_source_split=args.temporal_source_split,
            new_relationship_source_split=args.new_relationship_source_split,
            overwrite=args.overwrite,
        )
    )
    summary = {
        benchmark: {
            split: values["feature_rows"]
            for split, values in details["splits"].items()
        }
        for benchmark, details in manifest["benchmarks"].items()
    }
    print(
        f"[pair-dataset] wrote {args.output_dir / 'manifest.json'} "
        f"benchmarks={summary}"
    )


if __name__ == "__main__":
    main()
