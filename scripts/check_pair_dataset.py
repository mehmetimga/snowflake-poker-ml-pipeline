"""Audit frozen pair dataset hashes and leakage boundaries."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _features(root: Path, benchmark: str, split: str) -> pd.DataFrame:
    return pd.read_parquet(
        root / "benchmarks" / benchmark / split / "features.parquet"
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/pair-v1"),
    )
    args = parser.parse_args()
    root = args.dataset.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    schema = json.loads((root / "schema.json").read_text())
    if manifest["feature_definition_version"] != "pair-features-v1":
        raise ValueError("unexpected feature definition")
    if manifest["challenge_labels_public"] or schema["challenge_labels_public"]:
        raise ValueError("challenge labels are marked public")

    for relative, expected in manifest["artifacts"].items():
        path = root / relative
        if not path.exists():
            raise FileNotFoundError(path)
        actual = _sha256(path)
        if actual != expected:
            raise ValueError(f"artifact hash mismatch: {relative}")

    forbidden = {"is_collusive", "collusion_pair_id", "label_available_at", "target"}
    for path in root.glob("benchmarks/**/features.parquet"):
        frame = pd.read_parquet(path)
        leaked = forbidden & set(frame.columns)
        if leaked:
            raise ValueError(f"private columns in {path}: {sorted(leaked)}")
        if frame.groupby("hand_id")["benchmark_split"].nunique().max() != 1:
            raise ValueError(f"hand split across partitions in {path}")

    challenge = root / "benchmarks" / "challenge" / "challenge"
    if (challenge / "labels").exists():
        raise ValueError("challenge has a public labels directory")
    if not (challenge / "private_labels" / "pair_labels.parquet").exists():
        raise ValueError("challenge private label sidecar is missing")
    if (root / "dgx" / "challenge").exists():
        raise ValueError("challenge data must not be exported to DGX")

    populations: dict[str, set[str]] = {}
    for split in ("train", "validation", "test", "challenge"):
        frame = _features(root, "cold_start", split)
        populations[split] = set(frame["player_a"]) | set(frame["player_b"])
    names = list(populations)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            if populations[left] & populations[right]:
                raise ValueError(f"cold-start player leakage: {left}/{right}")

    temporal = {
        split: _features(root, "temporal", split)
        for split in ("train", "validation", "test")
    }
    if temporal["train"]["played_at"].max() >= temporal["validation"]["played_at"].min():
        raise ValueError("temporal train/validation order is invalid")
    if temporal["validation"]["played_at"].max() >= temporal["test"]["played_at"].min():
        raise ValueError("temporal validation/test order is invalid")

    protected = manifest["benchmarks"]["new_relationship"]["protected_positive_pairs"]
    relationship_train = _features(root, "new_relationship", "train")
    train_keys = set(relationship_train["pair_key"])
    leaked = train_keys & (set(protected["validation"]) | set(protected["test"]))
    if leaked:
        raise ValueError(f"protected relationship leakage: {sorted(leaked)}")

    dgx_rows = 0
    for path in root.glob("dgx/**/*.parquet"):
        frame = pd.read_parquet(path)
        if "target" not in frame or not set(frame["target"].unique()).issubset({0, 1}):
            raise ValueError(f"invalid DGX target: {path}")
        dgx_rows += len(frame)
    print(
        f"[pair-dataset-check] artifacts={len(manifest['artifacts'])} "
        f"benchmarks={len(manifest['benchmarks'])} dgx_rows={dgx_rows} "
        "hashes=passed leakage=passed challenge_isolation=passed"
    )


if __name__ == "__main__":
    main()
