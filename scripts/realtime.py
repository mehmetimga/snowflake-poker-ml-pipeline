"""Consume Kafka hands and score each batch immediately."""

from __future__ import annotations

import argparse

from pipeline.realtime import RealTimeProcessor
from pipeline.realtime.pattern_search import PatternSearchConfig


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-messages", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=25)
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--group-id", default="realtime-processor")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--enable-pattern-search", action="store_true")
    parser.add_argument("--pattern-candidate-rule-score", type=float, default=1.0)
    parser.add_argument("--pattern-candidate-risk-score", type=float, default=0.5)
    parser.add_argument("--pattern-candidate-pair-memory-score", type=float, default=0.65)
    parser.add_argument("--pattern-max-pairs", type=int, default=50)
    parser.add_argument("--pattern-timeout", type=float, default=1.5)
    parser.add_argument("--pair-memory-max-pairs", type=int, default=10000)
    parser.add_argument("--no-pair-memory", action="store_true")
    parser.add_argument(
        "--no-persist-history",
        action="store_true",
        help="Do not write raw/features/rules to the warehouse after realtime scoring.",
    )
    parser.add_argument(
        "--no-persist-alerts",
        action="store_true",
        help="Do not write generated alerts to the warehouse after realtime scoring.",
    )
    parser.add_argument(
        "--skip-pair-stats",
        action="store_true",
        help="Deprecated; realtime no longer refreshes PAIR_STATS on the hot path.",
    )
    args = parser.parse_args()

    processor = RealTimeProcessor(
        threshold=args.threshold,
        persist_history=not args.no_persist_history,
        persist_alerts=not args.no_persist_alerts,
        pattern_search=PatternSearchConfig(
            enabled=args.enable_pattern_search,
            candidate_rule_score=args.pattern_candidate_rule_score,
            candidate_risk_score=args.pattern_candidate_risk_score,
            candidate_pair_memory_score=args.pattern_candidate_pair_memory_score,
            max_pairs=args.pattern_max_pairs,
            timeout=args.pattern_timeout,
        ),
        enable_pair_memory=not args.no_pair_memory,
        pair_memory_max_pairs=args.pair_memory_max_pairs,
    )
    total = processor.run_kafka(
        batch_size=args.batch_size,
        max_messages=args.max_messages,
        group_id=args.group_id,
        auto_offset_reset="earliest" if args.from_beginning else "latest",
    )
    print(f"[realtime] total processed {total} hands")


if __name__ == "__main__":
    main()
