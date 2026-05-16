"""Load hands from a local JSONL file into the warehouse, or compute features+rules."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.features.engineer import build_features_from_warehouse
from pipeline.rules.engine import build_rule_flags_from_warehouse
from pipeline.rules.pair import compute_pair_stats
from pipeline.warehouse import get_warehouse
from pipeline.warehouse.loader import load_hands


def _stream_jsonl(path: Path):
    with path.open() as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jsonl", help="Load hands from a JSONL file instead of Kafka")
    parser.add_argument("--compute-features", action="store_true", help="Compute FEATURES + RULE_FLAGS + PAIR_STATS")
    args = parser.parse_args()

    wh = get_warehouse()

    if args.jsonl:
        hands = list(_stream_jsonl(Path(args.jsonl)))
        n = load_hands(wh, hands)
        print(f"[load] inserted {n} hands from {args.jsonl}")

    if args.compute_features:
        feats = build_features_from_warehouse(wh)
        print(f"[features] wrote {len(feats)} rows to FEATURES")
        flags = build_rule_flags_from_warehouse(wh, feats)
        print(f"[rules] wrote {len(flags)} rows to RULE_FLAGS")
        pairs = compute_pair_stats(wh)
        print(f"[pairs] wrote {len(pairs)} rows to PAIR_STATS")


if __name__ == "__main__":
    main()
