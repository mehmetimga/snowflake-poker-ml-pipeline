"""Build Phase 11 prior-only heterogeneous graph snapshots."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.dl.graph_dataset import (
    GRAPH_BENCHMARKS,
    GraphDatasetConfig,
    build_graph_dataset,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--source-dir", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("data/datasets/pair-graph-full-v2")
    )
    parser.add_argument("--benchmarks", default=",".join(GRAPH_BENCHMARKS))
    parser.add_argument("--max-user-neighbors", type=int, default=8)
    parser.add_argument("--max-resource-neighbors", type=int, default=4)
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    benchmarks = tuple(
        value.strip() for value in args.benchmarks.split(",") if value.strip()
    )
    manifest = build_graph_dataset(
        GraphDatasetConfig(
            source_dir=args.source_dir,
            pair_dataset_dir=args.pair_dataset,
            output_dir=args.output_dir,
            benchmarks=benchmarks,
            max_user_neighbors=args.max_user_neighbors,
            max_resource_neighbors=args.max_resource_neighbors,
            overwrite=args.overwrite,
        )
    )
    print(
        f"[pair-graph-dataset] dataset={manifest['dataset_id']} "
        f"benchmarks={','.join(manifest['benchmarks'])} "
        "future_edges=false challenge_read=false raw_id_embeddings=0"
    )


if __name__ == "__main__":
    main()
