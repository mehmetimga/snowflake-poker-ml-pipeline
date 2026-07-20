"""Verify graph hashes, lineage, alignment, and temporal boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from pipeline.dl.graph_dataset import (
    GRAPH_SPLITS,
    PAIR_GRAPH_FEATURES,
    RESOURCE_NODE_FEATURES,
    RESOURCE_TYPES,
    ROOT_USER_FEATURES,
    USER_EDGE_FEATURES,
    event_alignment_sha256,
    load_graph_split,
    sha256_file,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-graph-full-v2")
    )
    parser.add_argument(
        "--source-dir", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    args = parser.parse_args()
    root = args.dataset.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    schema = json.loads((root / "schema.json").read_text())
    if manifest["phase"] != 11 or manifest["challenge_artifacts_read"] is not False:
        raise ValueError("invalid Phase 11 safety metadata")
    if manifest["raw_id_embedding_count"] != 0 or schema["raw_id_embedding_count"] != 0:
        raise ValueError("graph dataset permits raw-ID embeddings")
    if tuple(schema["root_user_features"]) != ROOT_USER_FEATURES:
        raise ValueError("root-user graph schema mismatch")
    if tuple(schema["user_edge_features"]) != USER_EDGE_FEATURES:
        raise ValueError("user-edge graph schema mismatch")
    if tuple(schema["resource_types"]) != RESOURCE_TYPES:
        raise ValueError("graph resource type mismatch")
    if tuple(schema["resource_node_features"]) != RESOURCE_NODE_FEATURES:
        raise ValueError("graph resource feature mismatch")
    if tuple(schema["pair_graph_features"]) != PAIR_GRAPH_FEATURES:
        raise ValueError("pair graph feature mismatch")
    if sha256_file(args.source_dir.resolve() / "manifest.json") != manifest[
        "source_world_manifest_sha256"
    ]:
        raise ValueError("graph world lineage mismatch")
    if sha256_file(args.pair_dataset.resolve() / "manifest.json") != manifest[
        "source_pair_manifest_sha256"
    ]:
        raise ValueError("graph pair lineage mismatch")
    actual_files = {
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    }
    if actual_files != {"manifest.json", *manifest["artifacts"]}:
        raise ValueError("graph dataset file set mismatch")
    for relative, expected in manifest["artifacts"].items():
        if sha256_file(root / relative) != expected:
            raise ValueError(f"graph dataset hash mismatch: {relative}")
    compact = {}
    for benchmark, benchmark_info in manifest["benchmarks"].items():
        compact[benchmark] = {}
        for split in GRAPH_SPLITS:
            arrays = load_graph_split(root / "benchmarks" / benchmark / f"{split}.npz")
            audit = benchmark_info["splits"][split]
            rows = int(audit["rows"])
            if arrays["root_features"].shape != (rows, 2, len(ROOT_USER_FEATURES)):
                raise ValueError(f"{benchmark}/{split} root graph shape mismatch")
            if arrays["resource_features"].shape[2:] != (
                len(RESOURCE_TYPES),
                schema["max_resource_neighbors"],
                len(RESOURCE_NODE_FEATURES),
            ):
                raise ValueError(f"{benchmark}/{split} resource graph shape mismatch")
            if event_alignment_sha256(arrays["event_ids"]) != audit[
                "event_alignment_sha256"
            ]:
                raise ValueError(f"{benchmark}/{split} event alignment mismatch")
            if np.any(arrays["graph_last_edge_ns"] >= arrays["example_played_ns"]):
                raise ValueError(f"{benchmark}/{split} has current/future graph edges")
            if int(arrays["labels"].sum()) != int(audit["positive_rows"]):
                raise ValueError(f"{benchmark}/{split} graph label count mismatch")
            compact[benchmark][split] = {
                "rows": rows,
                "user_edges": int(arrays["user_neighbor_masks"].sum()),
                "resource_edges": int(arrays["resource_masks"].sum()),
            }
    print(
        "[pair-graph-dataset-check] "
        + json.dumps(
            {
                "artifact_hashes": "passed",
                "source_lineage": "passed",
                "strictly_prior_edges": "passed",
                "raw_id_embedding_count": 0,
                "challenge_artifacts_read": False,
                "benchmarks": compact,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
