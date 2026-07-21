"""Build JSON and Markdown model cards for the active champion."""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from pipeline.ml.model_card import ModelCardGovernance, build_model_card


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
        "--output", type=Path, default=Path("models/registry/model_card.json")
    )
    parser.add_argument(
        "--markdown", type=Path, default=Path("models/registry/model_card.md")
    )
    parser.add_argument("--owner", default="poker-ml-platform")
    parser.add_argument("--review-date", default=date.today().isoformat())
    args = parser.parse_args()
    card = build_model_card(
        args.dataset,
        args.model_dir,
        args.registry_dir,
        args.stability_report,
        args.seed_stability_report,
        args.scenario_holdout_report,
        args.output,
        args.markdown,
        governance=ModelCardGovernance(args.owner, args.review_date),
    )
    segments = card["evaluation"]["segment_analysis"]["segments"]
    reliable = sum(
        item["reliability"]["status"] == "reliable" for item in segments
    )
    print(
        f"[model-card] model={card['identity']['model_name']}:"
        f"{card['identity']['run_id']} owner={card['governance']['owner']} "
        f"review_date={card['governance']['review_date']} "
        f"segments={reliable}/{len(segments)}_reliable output={args.output}"
    )


if __name__ == "__main__":
    main()
