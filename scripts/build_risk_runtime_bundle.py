#!/usr/bin/env python3
"""Build the minimal hash-verified model bundle used by POKER_RISK."""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any


BASE_RUNTIME_FILES = (
    "calibration.json",
    "decision_policy.json",
    "model.onnx",
    "preprocessing.json",
    "scoring_contract.json",
)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_runtime_bundle(source: Path, output: Path) -> dict[str, Any]:
    source = source.resolve()
    manifest_path = source / "artifact_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    scoring = json.loads((source / "scoring_contract.json").read_text())
    triton_model = scoring["batching"]["triton_model"]
    runtime_files = (
        *BASE_RUNTIME_FILES,
        f"triton/{triton_model}/1/model.onnx",
        f"triton/{triton_model}/config.pbtxt",
    )
    source_hashes = manifest.get("artifacts", {})
    selected: dict[str, str] = {}
    for relative in runtime_files:
        if relative not in source_hashes:
            raise ValueError(f"source manifest does not govern runtime file: {relative}")
        source_path = (source / relative).resolve()
        if source not in source_path.parents or not source_path.is_file():
            raise ValueError(f"unsafe or missing runtime artifact: {relative}")
        actual = sha256(source_path)
        if actual != source_hashes[relative]:
            raise ValueError(f"source artifact hash mismatch: {relative}")
        selected[relative] = actual

    if manifest.get("model_name") != scoring.get("model_name") or manifest.get(
        "run_id"
    ) != scoring.get("run_id"):
        raise ValueError("source manifest identity does not match scoring contract")

    output.mkdir(parents=True, exist_ok=True)
    for relative in runtime_files:
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source / relative, target)
    runtime_manifest = {
        "run_id": manifest["run_id"],
        "model_name": manifest["model_name"],
        "artifacts": selected,
        "triton_config": f"triton/{triton_model}/config.pbtxt",
        "bundle_purpose": "spcs-risk-runtime",
        "source_manifest_sha256": sha256(manifest_path),
    }
    (output / "artifact_manifest.json").write_text(
        json.dumps(runtime_manifest, indent=2, sort_keys=True) + "\n"
    )
    return runtime_manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--source", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--output", type=Path, default=Path("build/c1/risk-runtime")
    )
    args = parser.parse_args()
    manifest = build_runtime_bundle(args.source, args.output)
    print(
        "[risk-runtime-bundle] "
        f"model={manifest['model_name']} run={manifest['run_id']} "
        f"artifacts={len(manifest['artifacts'])} output={args.output}"
    )


if __name__ == "__main__":
    main()
