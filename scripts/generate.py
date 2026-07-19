"""Generate synthetic hands and emit them to Kafka, Parquet, or both."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pipeline.generator import GeneratorConfig, HandGenerator


def _publish_kafka(hands_iter) -> int:
    from pipeline.kafka.producer import HandProducer  # lazy import — kafka only needed when --out=kafka

    producer = HandProducer()
    n = producer.publish_many(hands_iter)
    producer.close()
    return n


def _write_jsonl(hands_iter, path: Path) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("w") as f:
        for h in hands_iter:
            f.write(json.dumps(h) + "\n")
            n += 1
    return n


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--hands", type=int, default=5000)
    parser.add_argument("--players", type=int, default=200)
    parser.add_argument("--tables", type=int, default=20)
    parser.add_argument("--pairs", type=int, default=30)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--split", default="live", help="Dataset split label embedded in hand IDs")
    parser.add_argument("--out", choices=["kafka", "parquet", "both", "jsonl"], default="kafka")
    parser.add_argument("--jsonl-path", default="data/raw/hands.jsonl")
    args = parser.parse_args()

    cfg = GeneratorConfig(
        n_hands=args.hands,
        n_players=args.players,
        n_tables=args.tables,
        n_colluding_pairs=args.pairs,
        seed=args.seed,
        dataset_split=args.split,
    )
    gen = HandGenerator(cfg)

    hands = list(gen.iter_hands())
    print(f"[generate] produced {len(hands)} hands with {args.pairs} colluding pairs")

    if args.out in ("kafka", "both"):
        published = _publish_kafka(hands)
        print(f"[generate] published {published} hands to Kafka")
    if args.out in ("jsonl", "parquet", "both"):
        path = Path(args.jsonl_path)
        n = _write_jsonl(hands, path)
        print(f"[generate] wrote {n} hands to {path}")


if __name__ == "__main__":
    main()
