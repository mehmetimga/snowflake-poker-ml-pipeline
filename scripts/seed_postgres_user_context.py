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
    tenant_id, product_id, user_id, context_version,
    effective_at, account_created_at,
    country_bucket, timezone, acquisition_channel, kyc_level,
    account_status, bankroll_bucket, preferred_stake_bucket,
    skill_rating, device_id, network_cluster_id
) VALUES (
    %(tenant_id)s, %(product_id)s, %(user_id)s, %(context_version)s,
    %(effective_at)s, %(account_created_at)s,
    %(country_bucket)s, %(timezone)s, %(acquisition_channel)s, %(kyc_level)s,
    %(account_status)s, %(bankroll_bucket)s, %(preferred_stake_bucket)s,
    %(skill_rating)s, %(device_id)s, %(network_cluster_id)s
)
ON CONFLICT (tenant_id, product_id, user_id, context_version) DO UPDATE SET
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


def load_source_scope(dsn: str, source_dataset_id: str) -> tuple[str, str]:
    import psycopg

    with psycopg.connect(dsn) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT DISTINCT tenant_id, product_id
                FROM public.hand_history
                WHERE simulation_dataset_id = %s
                """,
                (source_dataset_id,),
            )
            rows = cursor.fetchall()
    if len(rows) != 1:
        raise ValueError(
            "context seed requires exactly one tenant/product source scope"
        )
    return str(rows[0][0]), str(rows[0][1])


def context_rows(events: list[object]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for event in events:
        row = dict(event.payload)
        row["tenant_id"] = event.tenant_id
        row["product_id"] = event.product_id
        rows.append(row)
    return rows


def upsert_context_events(dsn: str, events: list[object]) -> int:
    import psycopg

    rows = context_rows(events)
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
    tenant_id, product_id = load_source_scope(
        args.postgres_dsn, args.source_dataset_id
    )
    events = build_context_events(
        hand,
        dataset_id=args.context_dataset_id,
        tenant_id=tenant_id,
        product_id=product_id,
    )
    count = upsert_context_events(args.postgres_dsn, events)
    print(
        json.dumps(
            {
                "status": "seeded",
                "source_dataset_id": args.source_dataset_id,
                "tenant_id": tenant_id,
                "product_id": product_id,
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
