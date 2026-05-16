"""SageMaker Processing entrypoint for synthetic hand generation.

Writes JSONL to /opt/ml/processing/output/hands.jsonl which SageMaker uploads
to the configured S3 prefix.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from pipeline.generator import GeneratorConfig, HandGenerator


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--hands", type=int, default=5000)
    ap.add_argument("--players", type=int, default=200)
    ap.add_argument("--tables", type=int, default=20)
    ap.add_argument("--pairs", type=int, default=30)
    ap.add_argument("--seed", type=int, default=int(os.environ.get("RANDOM_SEED", "42")))
    args = ap.parse_args()

    out_dir = Path(os.environ.get("SM_OUTPUT_HANDS_DIR", "/opt/ml/processing/output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "hands.jsonl"

    cfg = GeneratorConfig(
        n_hands=args.hands,
        n_players=args.players,
        n_tables=args.tables,
        n_colluding_pairs=args.pairs,
        seed=args.seed,
    )
    gen = HandGenerator(cfg)

    n = 0
    with out_path.open("w") as f:
        for h in gen.iter_hands():
            f.write(json.dumps(h) + "\n")
            n += 1
    print(f"[generate] wrote {n} hands to {out_path}")


if __name__ == "__main__":
    main()
