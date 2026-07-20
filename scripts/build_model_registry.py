"""Create the Phase 12 registry and immutable deployment snapshot."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ops.registry import build_registry


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--champion-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    parser.add_argument("--ensemble-dir", type=Path, default=Path("models/pair-ensemble-full-v2"))
    parser.add_argument("--output-dir", type=Path, default=Path("models/registry"))
    parser.add_argument("--operational-report", type=Path, default=Path("models/registry/operational_report.json"))
    args = parser.parse_args()
    registry = build_registry(
        args.champion_dir, args.ensemble_dir, args.output_dir,
        operational_report=args.operational_report,
    )
    production = next(entry for entry in registry["entries"] if entry["stage"] == "production")
    candidate = next(entry for entry in registry["entries"] if entry["model_name"] == "pair-oof-stack-v1")
    print(
        f"[model-registry] production={production['model_name']}:{production['run_id']} "
        f"ensemble_stage={candidate['stage']} entries={len(registry['entries'])}"
    )


if __name__ == "__main__":
    main()
