"""Generate the deterministic multi-stream dataset used by the new pipeline."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pipeline.generator import (
    FrozenDatasetConfig,
    RealtimeWorldConfig,
    build_realtime_world_dataset,
)


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=Path("data/datasets/context-v1"))
    parser.add_argument("--dataset-id", default="context-v1")
    parser.add_argument("--train-hands", type=int, default=20_000)
    parser.add_argument("--validation-hands", type=int, default=5_000)
    parser.add_argument("--test-hands", type=int, default=5_000)
    parser.add_argument("--challenge-hands", type=int, default=5_000)
    parser.add_argument("--players", type=int, default=200)
    parser.add_argument("--tables", type=int, default=20)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--hand-start-at",
        type=_timestamp,
        default=None,
        help="UTC-aware ISO timestamp for the first hand; defaults to the frozen fixture time.",
    )
    args = parser.parse_args()

    context_start_at = (
        args.hand_start_at - timedelta(days=1)
        if args.hand_start_at is not None
        else None
    )
    manifest = build_realtime_world_dataset(
        args.output_dir,
        RealtimeWorldConfig(
            dataset_id=args.dataset_id,
            frozen=FrozenDatasetConfig(
                train_hands=args.train_hands,
                validation_hands=args.validation_hands,
                test_hands=args.test_hands,
                challenge_hands=args.challenge_hands,
                n_players=args.players,
                n_tables=args.tables,
                n_colluding_pairs=args.pairs,
                seed=args.seed,
            ),
        ),
        hand_start_at=args.hand_start_at,
        context_start_at=context_start_at,
    )
    counts = {split: values["hands"] for split, values in manifest["splits"].items()}
    print(f"[world] wrote {args.output_dir / 'manifest.json'} splits={counts}")


if __name__ == "__main__":
    main()
