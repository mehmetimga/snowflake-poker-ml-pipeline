"""Build deterministic train/validation/test/challenge PokerKit datasets."""

from __future__ import annotations

import argparse
from pathlib import Path

from pipeline.generator import FrozenDatasetConfig, build_frozen_dataset


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("data/datasets/cpu-v1"))
    parser.add_argument("--train-hands", type=int, default=20_000)
    parser.add_argument("--validation-hands", type=int, default=5_000)
    parser.add_argument("--test-hands", type=int, default=5_000)
    parser.add_argument("--challenge-hands", type=int, default=5_000)
    parser.add_argument("--players", type=int, default=200)
    parser.add_argument("--tables", type=int, default=20)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    manifest = build_frozen_dataset(
        args.output_dir,
        FrozenDatasetConfig(
            train_hands=args.train_hands,
            validation_hands=args.validation_hands,
            test_hands=args.test_hands,
            challenge_hands=args.challenge_hands,
            n_players=args.players,
            n_tables=args.tables,
            n_colluding_pairs=args.pairs,
            seed=args.seed,
        ),
    )
    counts = {name: split["hands"] for name, split in manifest["splits"].items()}
    print(f"[dataset] wrote {args.output_dir / 'manifest.json'} splits={counts}")


if __name__ == "__main__":
    main()
