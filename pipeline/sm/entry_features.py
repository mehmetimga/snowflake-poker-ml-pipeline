"""SageMaker Processing entrypoint: compute FEATURES + RULE_FLAGS + PAIR_STATS."""

from __future__ import annotations

from pipeline.features.engineer import build_features_from_warehouse
from pipeline.rules.engine import build_rule_flags_from_warehouse
from pipeline.rules.pair import compute_pair_stats
from pipeline.warehouse import get_warehouse


def main() -> None:
    wh = get_warehouse()
    feats = build_features_from_warehouse(wh)
    print(f"[features] wrote {len(feats)} rows to FEATURES")
    flags = build_rule_flags_from_warehouse(wh, feats)
    print(f"[rules] wrote {len(flags)} rows to RULE_FLAGS")
    pairs = compute_pair_stats(wh)
    print(f"[pairs] wrote {len(pairs)} rows to PAIR_STATS")
    wh.close()


if __name__ == "__main__":
    main()
