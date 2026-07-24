#!/usr/bin/env python3
"""Verify bounded legacy/canonical replacement coverage for R6."""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.ops.realtime_retirement import (
    LEGACY_GROUP_ID,
    LEGACY_HANDS_TOPIC,
    R6_RUN_TYPE,
    accepted_admin_ids,
    compare_legacy_inputs,
    load_r6_source,
    output_contract_comparison,
    validate_replay_manifest,
)
from pipeline.warehouse import get_warehouse


SINK_GROUP_ID = "poker-snowflake-sink-synthetic-v1"
CANONICAL_TABLES = {
    "hands": "POKER_HAND_EVENTS",
    "player_context": "POKER_PLAYER_CONTEXT_EVENTS",
    "pair_features": "POKER_PAIR_FEATURE_EVENTS_V2",
    "risk_scores": "POKER_RISK_SCORE_EVENTS",
    "rule_evidence": "POKER_RULE_EVIDENCE_EVENTS_V2",
    "review_decisions": "POKER_REVIEW_DECISION_EVENTS",
    "risk_alerts": "POKER_RISK_ALERT_EVENTS",
}


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _required_offsets(
    records: list[Mapping[str, Any]],
) -> dict[tuple[str, int], int]:
    result: dict[tuple[str, int], int] = {}
    for record in records:
        key = (LEGACY_HANDS_TOPIC, int(record["partition"]))
        result[key] = max(result.get(key, 0), int(record["offset"]) + 1)
    return result


def wait_for_commits(
    group_id: str,
    required: Mapping[tuple[str, int], int],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, int]:
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        client_id="poker-r6-parity-commit-verifier-v1",
        **client_kwargs,
    )
    deadline = time.monotonic() + timeout_seconds
    latest: dict[tuple[str, int], int] = {}
    try:
        while time.monotonic() < deadline:
            offsets = admin.list_consumer_group_offsets(group_id)
            latest = {
                (item.topic, item.partition): int(metadata.offset)
                for item, metadata in offsets.items()
                if metadata is not None
            }
            if all(latest.get(key, -1) >= value for key, value in required.items()):
                return {
                    f"{topic}[{partition}]": latest[(topic, partition)]
                    for topic, partition in sorted(required)
                }
            time.sleep(1)
    finally:
        admin.close()
    raise TimeoutError(
        f"{group_id} did not commit the R6 replay: "
        + json.dumps(
            {
                f"{topic}[{partition}]": latest.get((topic, partition), -1)
                for topic, partition in sorted(required)
            },
            sort_keys=True,
        )
    )


def group_lag(
    group_id: str,
    topics: list[str],
    *,
    client_kwargs: dict[str, Any],
) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    inspector = KafkaConsumer(
        group_id=None,
        enable_auto_commit=False,
        **client_kwargs,
    )
    group = KafkaConsumer(
        group_id=group_id,
        enable_auto_commit=False,
        **client_kwargs,
    )
    try:
        rows = []
        total = 0
        for topic in sorted(set(topics)):
            partitions = sorted(inspector.partitions_for_topic(topic) or [])
            assignments = [TopicPartition(topic, value) for value in partitions]
            ends = inspector.end_offsets(assignments)
            for item in assignments:
                committed = group.committed(item)
                effective = 0 if committed is None else int(committed)
                lag = max(0, int(ends[item]) - effective)
                total += lag
                rows.append(
                    {
                        "topic": topic,
                        "partition": item.partition,
                        "committed": committed,
                        "end": int(ends[item]),
                        "lag": lag,
                    }
                )
        return {"group_id": group_id, "total_lag": total, "partitions": rows}
    finally:
        group.close()
        inspector.close()


def _bounded_frame(
    warehouse: Any,
    table: str,
    columns: str,
    hand_ids: tuple[str, ...],
) -> pd.DataFrame:
    placeholders = ",".join(["%s"] * len(hand_ids))
    return warehouse.fetch_df(
        f"""
        SELECT {columns}
        FROM POKER_ML_DEMO.PUBLIC.{table}
        WHERE hand_id IN ({placeholders})
        """,
        tuple(hand_ids),
    )


def read_legacy_frames(
    warehouse: Any,
    hand_ids: tuple[str, ...],
) -> dict[str, pd.DataFrame]:
    return {
        "raw_hands": _bounded_frame(
            warehouse,
            "RAW_HANDS",
            (
                "hand_id, table_id, played_at, small_blind, big_blind, "
                "num_players, pot_size, board, dataset_split"
            ),
            hand_ids,
        ),
        "raw_players": _bounded_frame(
            warehouse,
            "RAW_PLAYERS",
            (
                "hand_id, player_id, name, position, stack_start, "
                "hole_cards, won_amount"
            ),
            hand_ids,
        ),
        "raw_actions": _bounded_frame(
            warehouse,
            "RAW_ACTIONS",
            (
                "hand_id, sequence_no, player_id, street, "
                "action_type, amount"
            ),
            hand_ids,
        ),
        "features": _bounded_frame(
            warehouse,
            "FEATURES",
            "hand_id, player_id",
            hand_ids,
        ),
        "rule_flags": _bounded_frame(
            warehouse,
            "RULE_FLAGS",
            "hand_id, player_id",
            hand_ids,
        ),
        "alerts": _bounded_frame(
            warehouse,
            "ALERTS",
            (
                "alert_id, hand_id, suspicious_player_id, risk_score, "
                "risk_level, model_scores, created_at"
            ),
            hand_ids,
        ),
    }


def read_canonical_counts(
    warehouse: Any,
    dataset_id: str,
) -> dict[str, int]:
    result = {}
    for name, table in CANONICAL_TABLES.items():
        frame = warehouse.fetch_df(
            f"""
            SELECT COUNT(*) AS n
            FROM POKER_ML_DEMO.SPCS.{table}
            WHERE dataset_id = %s
            """,
            (dataset_id,),
        )
        result[name] = int(frame.iloc[0]["n"])
    return result


def read_canonical_admin_ids(
    warehouse: Any,
    dataset_id: str,
) -> set[str]:
    frame = warehouse.fetch_df(
        """
        SELECT alert_id
        FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V
        WHERE dataset_id = %s
        """,
        (dataset_id,),
    )
    return set() if frame.empty else set(frame["alert_id"].astype(str))


def _canonical_transport_latency(
    warehouse: Any,
    dataset_id: str,
) -> dict[str, Any]:
    frame = warehouse.fetch_df(
        """
        SELECT source_timestamp_ms, ingested_at
        FROM POKER_ML_DEMO.SPCS.POKER_EVENT_ENVELOPES
        WHERE dataset_id = %s
        """,
        (dataset_id,),
    )
    if frame.empty:
        return {"rows": 0, "p50_ms": None, "p95_ms": None, "maximum_ms": None}
    source = pd.to_datetime(frame["source_timestamp_ms"], unit="ms", utc=True)
    ingested = pd.to_datetime(frame["ingested_at"], utc=True)
    values = (ingested - source).dt.total_seconds() * 1000.0
    return {
        "rows": len(values),
        "p50_ms": float(values.quantile(0.50)),
        "p95_ms": float(values.quantile(0.95)),
        "maximum_ms": float(values.max()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--spcs-report", type=Path, required=True)
    parser.add_argument("--sink-report", type=Path, required=True)
    parser.add_argument("--preflight-report", type=Path, required=True)
    parser.add_argument("--replay-report", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    args = parser.parse_args()
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")

    source = load_r6_source(
        args.manifest,
        args.spcs_report,
        args.sink_report,
    )
    preflight = _read_json(args.preflight_report)
    replay = validate_replay_manifest(_read_json(args.replay_report))
    if (
        preflight.get("schema_version") != 1
        or preflight.get("run_type") != R6_RUN_TYPE
        or preflight.get("phase") != "preflight"
        or preflight.get("status") != "passed"
        or replay.get("dataset_id") != source["dataset_id"]
        or replay.get("target_hand_ids") != list(source["target_hand_ids"])
        or replay.get("source_hashes") != source["source_hashes"]
        or replay.get("source_commit") != preflight.get("source_commit")
    ):
        raise SystemExit("R6 reports do not form one accepted evidence chain")

    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("R6 verification requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    legacy_commits = wait_for_commits(
        LEGACY_GROUP_ID,
        _required_offsets(replay["records"]),
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )

    warehouse = get_warehouse()
    try:
        frames = read_legacy_frames(warehouse, source["target_hand_ids"])
        legacy = compare_legacy_inputs(
            source["hand_payloads"],
            **frames,
        )
        canonical_counts = read_canonical_counts(
            warehouse, source["dataset_id"]
        )
        canonical_admin_ids = read_canonical_admin_ids(
            warehouse, source["dataset_id"]
        )
        canonical_latency = _canonical_transport_latency(
            warehouse, source["dataset_id"]
        )
    finally:
        warehouse.close()

    expected_canonical = {
        name: int(source["expected_counts"][name])
        for name in canonical_counts
    }
    expected_admin_alert_ids = accepted_admin_ids(source["spcs_report"])
    canonical_status = (
        "passed"
        if canonical_counts == expected_canonical
        and canonical_admin_ids == expected_admin_alert_ids
        else "failed"
    )
    legacy_lag = group_lag(
        LEGACY_GROUP_ID,
        [LEGACY_HANDS_TOPIC],
        client_kwargs=client_kwargs,
    )
    sink_lag = group_lag(
        SINK_GROUP_ID,
        list(source["manifest"]["topics"].values()),
        client_kwargs=client_kwargs,
    )
    output_comparison = output_contract_comparison(
        legacy_alert_rows=legacy["legacy_alerts"]["rows"],
        legacy_alert_hands=legacy["legacy_alerts"]["hands"],
        canonical_scores=canonical_counts["risk_scores"],
        canonical_decisions=canonical_counts["review_decisions"],
        canonical_alerts=canonical_counts["risk_alerts"],
    )
    status = (
        "passed"
        if legacy["status"] == "passed"
        and canonical_status == "passed"
        and legacy_lag["total_lag"] == 0
        and sink_lag["total_lag"] == 0
        else "failed"
    )
    completed_at = datetime.now(timezone.utc)
    replay_completed = pd.Timestamp(replay["completed_at"])
    if replay_completed.tzinfo is None:
        replay_completed = replay_completed.tz_localize("UTC")
    else:
        replay_completed = replay_completed.tz_convert("UTC")
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "bounded_parity",
        "status": status,
        "training_allowed": False,
        "dataset_id": source["dataset_id"],
        "source_commit": replay["source_commit"],
        "source_reports": {
            "preflight": str(args.preflight_report.resolve()),
            "legacy_replay": str(args.replay_report.resolve()),
            "d7_spcs": str(args.spcs_report.resolve()),
            "d7_sink": str(args.sink_report.resolve()),
        },
        "legacy": legacy,
        "canonical": {
            "status": canonical_status,
            "counts": canonical_counts,
            "admin_rows": len(canonical_admin_ids),
            "admin_alert_ids": sorted(canonical_admin_ids),
            "target_dead_letters": int(
                source["spcs_report"]["target_dead_letters"]
            ),
        },
        "output_contract_comparison": output_comparison,
        "consumer_commits": legacy_commits,
        "lag": {
            "legacy": legacy_lag,
            "canonical_sink": sink_lag,
        },
        "latency": {
            "canonical_kafka_to_snowflake": canonical_latency,
            "legacy_visibility_upper_bound_seconds": (
                completed_at - replay_completed.to_pydatetime()
            ).total_seconds(),
            "event_time_comparison": "not_applicable_simulated_event_clock",
        },
        "completed_at": completed_at.isoformat().replace("+00:00", "Z"),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if status != "passed":
        raise SystemExit("R6 bounded parity verification failed")


if __name__ == "__main__":
    main()
