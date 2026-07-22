#!/usr/bin/env python3
"""Write deterministic PokerKit hands to the PostgreSQL CDC simulation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from datetime import datetime, timezone

from pipeline.cdc.postgres_simulator import (
    DEFAULT_ALLOWED_GAME_TYPES,
    DEFAULT_SIMULATION_GAME_TYPES,
    PostgresSimulationSink,
    build_simulation_insert,
    connect_postgres,
    validate_game_types,
)
from pipeline.generator import GeneratorConfig, HandGenerator


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"


def _csv(value: str) -> tuple[str, ...]:
    return validate_game_types(
        tuple(part.strip() for part in value.split(",") if part.strip())
    )


def _start_at(value: str) -> datetime:
    if value == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--start-at must include a timezone")
    return parsed.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--hands", type=int, default=20)
    parser.add_argument("--players", type=int, default=30)
    parser.add_argument("--tables", type=int, default=3)
    parser.add_argument("--pairs", type=int, default=5)
    parser.add_argument("--seed", type=int, default=4201)
    parser.add_argument("--dataset-id", default="sim-cdc-v1")
    parser.add_argument("--split", default="live")
    parser.add_argument("--tenant-id", default="demo")
    parser.add_argument("--product-id", default="poker")
    parser.add_argument(
        "--game-types", type=_csv, default=DEFAULT_SIMULATION_GAME_TYPES
    )
    parser.add_argument(
        "--allowed-game-types", type=_csv, default=DEFAULT_ALLOWED_GAME_TYPES
    )
    parser.add_argument("--start-at", type=_start_at, default=_start_at("now"))
    parser.add_argument(
        "--rate",
        type=float,
        default=10.0,
        help="hands per second; zero disables pacing",
    )
    parser.add_argument(
        "--dsn", default=os.environ.get("CDC_SIM_POSTGRES_DSN", DEFAULT_DSN)
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.hands < 0 or args.rate < 0:
        parser.error("--hands and --rate must be non-negative")

    generator = HandGenerator(
        GeneratorConfig(
            n_hands=args.hands,
            n_players=args.players,
            n_tables=args.tables,
            n_colluding_pairs=args.pairs,
            seed=args.seed,
            dataset_split=args.split,
            dataset_id=args.dataset_id,
        ),
        start_at=args.start_at,
    )
    connection = None if args.dry_run else connect_postgres(args.dsn)
    sink = PostgresSimulationSink(connection) if connection is not None else None
    inserted = duplicates = eligible = excluded = 0
    digest = hashlib.sha256()
    started = time.monotonic()
    try:
        for index, hand in enumerate(generator.iter_hands()):
            game_type = args.game_types[index % len(args.game_types)]
            record = build_simulation_insert(
                hand,
                game_type=game_type,
                dataset_id=args.dataset_id,
                tenant_id=args.tenant_id,
                product_id=args.product_id,
            )
            digest.update(record.payload)
            if game_type in args.allowed_game_types:
                eligible += 1
            else:
                excluded += 1
            if sink is not None:
                if sink.insert(record):
                    inserted += 1
                else:
                    duplicates += 1
            if args.rate > 0:
                deadline = started + (index + 1) / args.rate
                time.sleep(max(0.0, deadline - time.monotonic()))
    finally:
        if connection is not None:
            connection.close()

    print(
        json.dumps(
            {
                "status": "dry_run" if args.dry_run else "written",
                "generated_hands": args.hands,
                "source_rows_inserted": inserted,
                "source_duplicates": duplicates,
                "expected_outbox_rows": eligible,
                "expected_filtered_rows": excluded,
                "game_types": list(args.game_types),
                "allowed_game_types": list(args.allowed_game_types),
                "dataset_id": args.dataset_id,
                "start_at": args.start_at.isoformat(),
                "payload_stream_sha256": digest.hexdigest(),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
