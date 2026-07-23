"""Build label-free D5 benchmark assignments from a multi-table world."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.generator import (
    MultiTableBenchmarkConfig,
    build_multitable_benchmarks,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/datasets/multitable-cold-v1"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/datasets/multitable-benchmarks-v1"),
    )
    parser.add_argument("--temporal-source-split", default="train")
    parser.add_argument("--new-relationship-source-split", default="train")
    args = parser.parse_args()

    manifest = build_multitable_benchmarks(
        MultiTableBenchmarkConfig(
            source_dir=args.source_dir,
            output_dir=args.output_dir,
            temporal_source_split=args.temporal_source_split,
            new_relationship_source_split=(args.new_relationship_source_split),
        )
    )
    print(
        "[multitable-benchmarks] "
        f"wrote={args.output_dir / 'manifest.json'} "
        f"benchmarks={len(manifest['benchmarks'])} "
        f"challenge_labels_read={manifest['challenge_private_labels_read']}"
    )


if __name__ == "__main__":
    main()
