"""Build the frozen D6 alert-acceptance pack and offline oracle."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.generator import (
    AlertAcceptanceBuildConfig,
    AlertAcceptanceProfile,
    build_alert_acceptance_pack,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("config/generator/multitable-alert-acceptance-v1.json"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/datasets/multitable-alert-acceptance-v1"),
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("models/pair-catboost-full-v2"),
    )
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=Path("data/datasets/multitable-benchmarks-v1"),
    )
    parser.add_argument(
        "--review-policy",
        type=Path,
        default=Path("schemas/policies/review-policy-v1.json"),
    )
    args = parser.parse_args()
    manifest = build_alert_acceptance_pack(
        AlertAcceptanceBuildConfig(
            output_dir=args.output_dir,
            model_dir=args.model_dir,
            benchmark_dir=args.benchmark_dir,
            profile=AlertAcceptanceProfile.from_json(args.config),
            review_policy_path=args.review_policy,
        )
    )
    counts = manifest["counts"]
    print(
        "[alert-acceptance] "
        f"hands={counts['hands']} pair_features={counts['pair_features']} "
        f"model_alerts={counts['expected_model_alerts']} "
        f"selected_demo_alerts={counts['selected_demo_alerts']} "
        "training_allowed=false java_flink=not_run go_risk=not_run"
    )


if __name__ == "__main__":
    main()
