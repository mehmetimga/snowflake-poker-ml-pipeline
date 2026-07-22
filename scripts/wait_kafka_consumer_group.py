#!/usr/bin/env python3
"""Wait until a Kafka consumer group has a stable assignment for one topic."""

from __future__ import annotations

import argparse
import json
import time


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--timeout-seconds", type=float, default=60.0)
    args = parser.parse_args()
    if args.timeout_seconds <= 0:
        parser.error("--timeout-seconds must be positive")

    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        bootstrap_servers=args.bootstrap_servers.split(","),
        client_id="poker-cdc-simulation-group-waiter",
    )
    deadline = time.monotonic() + args.timeout_seconds
    latest = None
    try:
        while time.monotonic() < deadline:
            try:
                descriptions = admin.describe_consumer_groups([args.group_id])
                latest = descriptions[0] if descriptions else None
                if latest is not None and latest.state == "Stable":
                    assignments = {
                        partition.topic
                        for member in latest.members
                        for partition in member.member_assignment.partitions()
                    }
                    if args.topic in assignments:
                        print(
                            json.dumps(
                                {
                                    "status": "ready",
                                    "group_id": args.group_id,
                                    "state": latest.state,
                                    "members": len(latest.members),
                                    "topics": sorted(assignments),
                                },
                                sort_keys=True,
                            )
                        )
                        return
            except Exception as exc:  # coordinator creation is eventually consistent
                latest = repr(exc)
            time.sleep(0.25)
    finally:
        admin.close()
    raise RuntimeError(
        f"consumer group {args.group_id!r} was not ready within "
        f"{args.timeout_seconds}s: {latest}"
    )


if __name__ == "__main__":
    main()
