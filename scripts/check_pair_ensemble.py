"""Validate Phase 12 ensemble hashes, folds, and portable scoring contract."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from pipeline.ml.ensemble import BASE_FEATURES, portable_logistic_predict


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--model-dir", type=Path, default=Path("models/pair-ensemble-full-v2"))
    args = parser.parse_args()
    root = args.model_dir
    manifest = json.loads((root / "artifact_manifest.json").read_text())
    for relative, expected in manifest["artifacts"].items():
        path = root / relative
        if not path.is_file() or sha256(path) != expected:
            raise RuntimeError(f"artifact verification failed: {relative}")
    folds = json.loads((root / "fold_manifest.json").read_text())
    if not folds["rows_assigned_once"] or folds["private_challenge_loaded"]:
        raise RuntimeError("invalid OOF leakage contract")
    if any(item["hand_overlap"] for item in folds["fold_manifest"]):
        raise RuntimeError("OOF fold has hand overlap")
    predictions = pd.read_parquet(root / "predictions.parquet")
    stacker = json.loads((root / "stacker.json").read_text())
    matrix = predictions.loc[:, BASE_FEATURES].to_numpy(dtype=np.float64)
    actual = portable_logistic_predict(stacker, matrix)
    difference = float(np.max(np.abs(actual - predictions["raw_probability"].to_numpy())))
    if difference > 1e-10:
        raise RuntimeError(f"portable stacker mismatch: {difference}")
    metrics = json.loads((root / "metrics.json").read_text())
    print(
        f"[pair-ensemble-check] run={metrics['run_id']} rows={len(predictions)} "
        f"folds={folds['folds']} max_probability_difference={difference:.3e} "
        "hashes=passed leakage=passed portable_contract=passed"
    )


if __name__ == "__main__":
    main()
