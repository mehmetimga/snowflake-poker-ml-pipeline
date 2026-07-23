#!/usr/bin/env python3
"""Generate deterministic context for one PokerKit hand and upsert PostgreSQL."""

from __future__ import annotations

import argparse
import json
import os

from scripts.replay_shadow_simulation import build_context_events, load_source_hand


DEFAULT_DSN = "postgresql://poker_sim:poker_sim@localhost:5433/poker_sim"


UPSERT = """
INSERT INTO public.poker_user_context (
    user_id, context_version, effective_at, account_created_at,
    country_bucket, timezone, acquisition_channel, kyc_level,
    account_status, bankroll_bucket, preferred_stake_bucket,
    skill_rating, device_id, network_cluster_id
) VALUES (
    %(user_id)s, %(context_version)s, %(effective_at)s, %(account_created_at)s,
    %(country_bucket)s, %(timezone)s, %(acquisition_channel)s, %(kyc_level)s,
    %(account_status)s, %(bankroll_bucket)s, %(preferred_stake_bucket)s,
    %(skill_rating)s, %(device_id)s, %(network_cluster_id)s
)
ON CONFLICT (user_id, context_version) DO UPDATE SET
    effective_at = EXCLUDED.effective_at,
    account_created_at = EXCLUDED.account_created_at,
    country_bucket = EXCLUDED.country_bucket,
    timezone = EXCLUDED.timezone,
    acquisition_channel = EXCLUDED.acquisition_channel,
    kyc_level = EXCLUDED.kyc_level,
    account_status = EXCLUDED.account_status,
    bankroll_bucket = EXCLUDED.bankroll_bucket,
    preferred_stake_bucket = EXCLUDED.preferred_stake_bucket,
    skill_rating = EXCLUDED.skill_rating,
    device_id = EXCLUDED.device_id,
    network_cluster_id = EXCLUDED.network_cluster_id,
    updated_at = clock_timestamp()
"""


def upsert_context_events(dsn: str, events: list[object]) -> int:
    import psycopg

    rows = [event.payload for event in events]
    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.executemany(UPSERT, rows)
        connection.commit()
    return len(rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dataset-id", required=True)
    parser.add_argument("--context-dataset-id", default="sim-active-context-v1")
    parser.add_argument(
        "--postgres-dsn",
        default=os.getenv("CDC_SIM_POSTGRES_DSN", DEFAULT_DSN),
    )
    args = parser.parse_args()

    hand = load_source_hand(args.postgres_dsn, args.source_dataset_id)
    events = build_context_events(hand, dataset_id=args.context_dataset_id)
    count = upsert_context_events(args.postgres_dsn, events)
    print(
        json.dumps(
            {
                "status": "seeded",
                "source_dataset_id": args.source_dataset_id,
                "hand_id": hand.hand_id,
                "user_context_rows": count,
                "user_ids": sorted(player.player_id for player in hand.players),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
