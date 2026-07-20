"""Export frozen Snowflake/DuckDB sequence data as a portable GPU bundle."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.dataset import build_frozen_sequence_partitions, save_sequence_partitions
from pipeline.warehouse import get_warehouse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("data/datasets/dgx-v1/dl_sequences.npz"),
    )
    parser.add_argument("--max-len", type=int, default=60)
    args = parser.parse_args()

    warehouse = get_warehouse()
    try:
        partitions = build_frozen_sequence_partitions(warehouse, max_len=args.max_len)
        manifest = save_sequence_partitions(partitions, args.output)
    finally:
        warehouse.close()

    counts = {
        name: details["rows"]
        for name, details in manifest["splits"].items()
    }
    print(
        f"[dl-export] wrote {args.output} sha256={manifest['sha256']} "
        f"splits={counts}"
    )


if __name__ == "__main__":
    main()
