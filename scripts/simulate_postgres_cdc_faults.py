#!/usr/bin/env python3
"""Insert the deterministic local CDC acceptance/filter/poison suite."""

from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone

from pipeline.cdc.postgres_simulator import PostgresSimulationSink, connect_postgres
from pipeline.cdc.simulation_scenarios import (
    FAULT_SCENARIOS,
    build_fault_scenario_records,
)
from pipeline.generator import GeneratorConfig, HandGenerator


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"


def _start_at(value: str) -> datetime:
    if value == "now":
        return datetime.now(timezone.utc).replace(microsecond=0)
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("--start-at must include a timezone")
    return parsed.astimezone(timezone.utc)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", default="sim-cdc-fault-v1")
    parser.add_argument("--seed", type=int, default=8801)
    parser.add_argument("--start-at", type=_start_at, default=_start_at("now"))
    parser.add_argument("--tenant-id", default="demo")
    parser.add_argument("--product-id", default="poker")
    parser.add_argument(
        "--dsn", default=os.environ.get("CDC_SIM_POSTGRES_DSN", DEFAULT_DSN)
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    hands = list(
        HandGenerator(
            GeneratorConfig(
                n_hands=len(FAULT_SCENARIOS),
                n_players=18,
                n_tables=3,
                n_colluding_pairs=3,
                seed=args.seed,
                dataset_split="live",
                dataset_id=args.dataset_id,
            ),
            start_at=args.start_at,
        ).iter_hands()
    )
    scenarios = build_fault_scenario_records(
        hands,
        dataset_id=args.dataset_id,
        tenant_id=args.tenant_id,
        product_id=args.product_id,
    )
    connection = None if args.dry_run else connect_postgres(args.dsn)
    sink = PostgresSimulationSink(connection) if connection is not None else None
    inserted = duplicates = 0
    try:
        for scenario in scenarios:
            if sink is None:
                continue
            if sink.insert(scenario.record):
                inserted += 1
            else:
                duplicates += 1
    finally:
        if connection is not None:
            connection.close()

    outcomes = {
        outcome: sum(item.definition.expected_outcome == outcome for item in scenarios)
        for outcome in ("canonical", "dead_letter", "filtered")
    }
    print(
        json.dumps(
            {
                "status": "dry_run" if args.dry_run else "written",
                "dataset_id": args.dataset_id,
                "source_rows": len(scenarios),
                "source_rows_inserted": inserted,
                "source_duplicates": duplicates,
                "expected_outbox_rows": outcomes["canonical"] + outcomes["dead_letter"],
                "expected_canonical_records": outcomes["canonical"],
                "expected_dead_letters": outcomes["dead_letter"],
                "expected_filtered_rows": outcomes["filtered"],
                "scenarios": [
                    {
                        "name": item.definition.name,
                        "hand_id": item.record.hand_id,
                        "game_type": item.record.game_type,
                        "expected_outcome": item.definition.expected_outcome,
                        "expected_error_code": item.definition.expected_error_code,
                    }
                    for item in scenarios
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
