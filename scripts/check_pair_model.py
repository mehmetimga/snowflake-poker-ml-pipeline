"""Verify artifact hashes and score label-free pair features through ONNX."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from pipeline.ml.pair_inference import PairOnnxScorer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--model-dir", type=Path, default=Path("models/pair-catboost-v1")
    )
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-v1")
    )
    parser.add_argument(
        "--benchmark",
        choices=("cold_start", "temporal", "new_relationship", "challenge"),
        default="cold_start",
    )
    parser.add_argument(
        "--split", choices=("train", "validation", "test", "challenge"), default="test"
    )
    parser.add_argument("--hands", type=int, default=1)
    args = parser.parse_args()
    if args.hands < 1:
        raise ValueError("--hands must be positive")

    benchmark = args.benchmark
    split = args.split
    if benchmark == "challenge":
        split = "challenge"
    feature_path = (
        args.dataset / "benchmarks" / benchmark / split / "features.parquet"
    )
    frame = pd.read_parquet(feature_path)
    hand_ids = frame["hand_id"].drop_duplicates().astype(str).head(args.hands)
    sample = frame[frame["hand_id"].astype(str).isin(set(hand_ids))].copy()

    scorer = PairOnnxScorer(args.model_dir)
    pair_scores, hand_scores = scorer.score_complete_hands(sample)
    print(
        f"[pair-model-check] model={scorer.contract['model_name']} "
        f"run={scorer.contract['run_id']} hands={len(hand_scores)} "
        f"pairs={len(pair_scores)} alerts={int(pair_scores['alert'].sum())} "
        f"max_risk={pair_scores['calibrated_probability'].max():.6f} "
        "hashes=passed contract=passed onnx=passed batching=passed"
    )


if __name__ == "__main__":
    main()
