"""Recompute hashes and leakage gates for D5 benchmark assignments."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.generator import verify_multitable_benchmarks


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/multitable-benchmarks-v1"),
    )
    parser.add_argument(
        "--source-dir",
        type=Path,
        default=Path("data/datasets/multitable-cold-v1"),
    )
    args = parser.parse_args()

    result = verify_multitable_benchmarks(args.dataset, args.source_dir)
    print(
        "[multitable-benchmarks-check] "
        f"status={result['status']} "
        f"artifacts={result['artifacts']} "
        f"benchmarks={result['benchmarks']} "
        f"source_hands={result['source_hands']} "
        "challenge_labels_read=false"
    )


if __name__ == "__main__":
    main()
