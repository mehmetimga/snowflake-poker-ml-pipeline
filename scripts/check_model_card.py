"""Verify model-card identities, source hashes, and Markdown parity."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.ml.model_card import validate_model_card


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-catboost-full-v2")
    )
    parser.add_argument(
        "--registry-dir", type=Path, default=Path("models/registry")
    )
    parser.add_argument(
        "--stability-report",
        type=Path,
        default=Path("models/registry/stability_report.json"),
    )
    parser.add_argument(
        "--seed-stability-report",
        type=Path,
        default=Path("models/registry/validation_seed_stability.json"),
    )
    parser.add_argument(
        "--scenario-holdout-report",
        type=Path,
        default=Path("models/registry/scenario_holdout_report.json"),
    )
    parser.add_argument(
        "--card", type=Path, default=Path("models/registry/model_card.json")
    )
    parser.add_argument(
        "--markdown", type=Path, default=Path("models/registry/model_card.md")
    )
    args = parser.parse_args()
    result = validate_model_card(
        args.dataset,
        args.model_dir,
        args.registry_dir,
        args.stability_report,
        args.seed_stability_report,
        args.scenario_holdout_report,
        args.card,
        args.markdown,
    )
    print(
        f"[model-card-check] model={result['model_name']}:{result['run_id']} "
        f"owner={result['owner']} review_date={result['review_date']} "
        f"segments={result['reliable_segments']}_reliable/"
        f"{result['suppressed_segments']}_suppressed "
        "hashes=passed identities=passed markdown=passed"
    )


if __name__ == "__main__":
    main()
