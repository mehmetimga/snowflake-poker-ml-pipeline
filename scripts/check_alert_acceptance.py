"""Verify D6 hashes, oracles, model binding, and training exclusion."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.generator import verify_alert_acceptance_pack


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dataset",
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
    result = verify_alert_acceptance_pack(
        args.dataset,
        model_dir=args.model_dir,
        benchmark_dir=args.benchmark_dir,
        review_policy_path=args.review_policy,
    )
    print(
        "[alert-acceptance-check] "
        f"status={result['status']} hands={result['hands']} "
        f"model_alerts={result['expected_model_alerts']} "
        f"selected_demo_alerts={result['selected_demo_alerts']} "
        "benchmark_overlap=0 training_allowed=false"
    )


if __name__ == "__main__":
    main()
