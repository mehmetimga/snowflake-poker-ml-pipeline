"""Publish a frozen context-rich world directly to canonical Kafka topics."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.kafka.event_producer import WorldEventProducer, WorldTopics
from pipeline.replay import DryRunPublisher, ReplayConfig, load_world_manifest, replay_world


def _topics(args: argparse.Namespace) -> WorldTopics:
    configured = WorldTopics.from_settings()
    return WorldTopics(
        hands=args.hands_topic or configured.hands,
        user_context=args.user_context_topic or configured.user_context,
        sessions=args.session_context_topic or configured.sessions,
        account_links=args.account_links_topic or configured.account_links,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/datasets/context-v1"))
    parser.add_argument(
        "--mode",
        choices=("replay", "accelerated", "realtime", "chaos"),
        default="replay",
    )
    parser.add_argument(
        "--split",
        action="append",
        choices=("train", "validation", "test", "challenge"),
        dest="splits",
        help="Repeat to select multiple splits; default is all four.",
    )
    parser.add_argument("--max-events", type=int, default=None)
    parser.add_argument("--rate", type=float, default=None, help="Events/second")
    parser.add_argument(
        "--speed",
        type=float,
        default=3_600.0,
        help="Realtime logical-time acceleration; 3600 means one simulated hour/second.",
    )
    parser.add_argument("--chaos-seed", type=int, default=91_001)
    parser.add_argument("--duplicate-rate", type=float, default=0.01)
    parser.add_argument("--late-rate", type=float, default=0.02)
    parser.add_argument("--reorder-window", type=int, default=25)
    parser.add_argument("--publish-batch-size", type=int, default=500)
    parser.add_argument("--ack-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--hands-topic", default=None)
    parser.add_argument("--user-context-topic", default=None)
    parser.add_argument("--session-context-topic", default=None)
    parser.add_argument("--account-links-topic", default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--report", type=Path, default=None)
    parser.add_argument("--no-report", action="store_true")
    args = parser.parse_args()

    rate = args.rate
    if rate is None:
        rate = 100.0 if args.mode == "accelerated" else 0.0
    config = ReplayConfig(
        mode=args.mode,
        splits=tuple(args.splits or ("train", "validation", "test", "challenge")),
        max_events=args.max_events,
        rate=rate,
        speed=args.speed,
        chaos_seed=args.chaos_seed,
        duplicate_rate=args.duplicate_rate,
        late_rate=args.late_rate,
        reorder_window=args.reorder_window,
        publish_batch_size=args.publish_batch_size,
        ack_timeout_seconds=args.ack_timeout_seconds,
    )
    publisher = (
        DryRunPublisher()
        if args.dry_run
        else WorldEventProducer(
            bootstrap_servers=args.bootstrap_servers,
            topics=_topics(args),
        )
    )
    report = replay_world(args.dataset, publisher, config)
    report_value = report.to_dict()

    if not args.no_report:
        if args.report is None:
            dataset_id = str(load_world_manifest(args.dataset)["dataset_id"])
            report_path = Path("data/runs") / f"{dataset_id}-{args.mode}-last.json"
        else:
            report_path = args.report
        report_path.parent.mkdir(parents=True, exist_ok=True)
        report_path.write_text(json.dumps(report_value, indent=2, sort_keys=True) + "\n")
        print(f"[world-replay] report={report_path}")

    print(
        "[world-replay] "
        f"mode={report.mode} source={report.source_events} "
        f"attempted={report.attempted} acknowledged={report.acknowledged} "
        f"duplicates={report.duplicate_attempts} elapsed={report.elapsed_seconds:.3f}s"
    )
    for topic, count in sorted(report.acknowledged_by_topic.items()):
        print(f"[world-replay] topic={topic} acknowledged={count}")


if __name__ == "__main__":
    main()
