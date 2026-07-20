"""Verify Phase 10 sequence hashes, alignment, and timestamp boundaries."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from pipeline.dl.history_dataset import (
    HISTORY_SPLITS,
    PAIR_HISTORY_FEATURES,
    USER_HISTORY_FEATURES,
    event_alignment_sha256,
    load_history_split,
    sha256_file,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/pair-sequences-full-v2"),
    )
    parser.add_argument(
        "--source-dir", type=Path, default=Path("data/datasets/context-full-v2")
    )
    parser.add_argument(
        "--pair-dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    args = parser.parse_args()
    root = args.dataset.resolve()
    manifest_path, schema_path = root / "manifest.json", root / "schema.json"
    manifest = json.loads(manifest_path.read_text())
    schema = json.loads(schema_path.read_text())
    if manifest["phase"] != 10 or manifest["benchmark"] != "cold_start":
        raise ValueError("unexpected history dataset phase or benchmark")
    if manifest["challenge_artifacts_read"] is not False:
        raise ValueError("history dataset read challenge artifacts")
    if manifest["challenge_labels_public"] is not False:
        raise ValueError("history dataset exposes challenge labels")
    if schema["challenge_labels_public"] is not False:
        raise ValueError("history schema exposes challenge labels")
    if sha256_file(args.source_dir.resolve() / "manifest.json") != manifest[
        "source_world_manifest_sha256"
    ]:
        raise ValueError("history dataset world-manifest lineage mismatch")
    if sha256_file(args.pair_dataset.resolve() / "manifest.json") != manifest[
        "source_pair_manifest_sha256"
    ]:
        raise ValueError("history dataset pair-manifest lineage mismatch")
    if tuple(schema["user_history_features"]) != USER_HISTORY_FEATURES:
        raise ValueError("user history schema mismatch")
    if tuple(schema["pair_history_features"]) != PAIR_HISTORY_FEATURES:
        raise ValueError("pair history schema mismatch")
    actual_files = {
        str(path.relative_to(root)) for path in root.rglob("*") if path.is_file()
    }
    expected_files = {"manifest.json", *manifest["artifacts"]}
    if actual_files != expected_files:
        raise ValueError("history dataset file set disagrees with its manifest")
    for relative, expected in manifest["artifacts"].items():
        if sha256_file(root / relative) != expected:
            raise ValueError(f"history dataset hash mismatch: {relative}")
    compact = {}
    for split in HISTORY_SPLITS:
        arrays = load_history_split(root / "splits" / f"{split}.npz")
        audit = manifest["splits"][split]
        rows = int(audit["rows"])
        if len(arrays["event_ids"]) != rows or len(arrays["labels"]) != rows:
            raise ValueError(f"{split} example row count mismatch")
        if arrays["pair_sequences"].shape != (
            rows,
            manifest["max_history"],
            len(PAIR_HISTORY_FEATURES),
        ):
            raise ValueError(f"{split} pair sequence shape mismatch")
        if arrays["user_sequences"].shape[1:] != (
            manifest["max_history"],
            len(USER_HISTORY_FEATURES),
        ):
            raise ValueError(f"{split} user sequence shape mismatch")
        if event_alignment_sha256(arrays["event_ids"]) != audit[
            "event_alignment_sha256"
        ]:
            raise ValueError(f"{split} event order hash mismatch")
        played = arrays["example_played_ns"]
        if np.any(arrays["pair_last_seen_ns"] >= played):
            raise ValueError(f"{split} pair history is not strictly prior")
        if np.any(arrays["user_last_seen_ns"][arrays["user_a_indices"]] >= played):
            raise ValueError(f"{split} user A history is not strictly prior")
        if np.any(arrays["user_last_seen_ns"][arrays["user_b_indices"]] >= played):
            raise ValueError(f"{split} user B history is not strictly prior")
        if int(arrays["labels"].sum()) != int(audit["positive_rows"]):
            raise ValueError(f"{split} positive count mismatch")
        compact[split] = {
            "rows": rows,
            "user_snapshots": int(audit["user_snapshots"]),
            "user_history_steps": int(arrays["user_masks"].sum()),
            "pair_history_steps": int(arrays["pair_masks"].sum()),
        }
    print(
        "[pair-history-dataset-check] "
        + json.dumps(
            {
                "artifact_hashes": "passed",
                "source_lineage": "passed",
                "event_alignment": "passed",
                "strictly_prior_timestamps": "passed",
                "challenge_artifacts_read": False,
                "splits": compact,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
