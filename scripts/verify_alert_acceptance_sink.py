#!/usr/bin/env python3
"""Reconcile the D7 acceptance replay from Kafka through POKER_SINK and admin."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.warehouse import Warehouse, get_warehouse
from scripts.verify_alert_acceptance_spcs import load_manifest


TABLES = {
    "hands": "POKER_HAND_EVENTS",
    "player_context": "POKER_PLAYER_CONTEXT_EVENTS",
    "pair_features": "POKER_PAIR_FEATURE_EVENTS_V2",
    "risk_scores": "POKER_RISK_SCORE_EVENTS",
    "rule_evidence": "POKER_RULE_EVIDENCE_EVENTS_V2",
    "review_decisions": "POKER_REVIEW_DECISION_EVENTS",
    "risk_alerts": "POKER_RISK_ALERT_EVENTS",
}
SINK_GROUP_ID = "poker-snowflake-sink-synthetic-v1"


def expected_admin_ids(spcs_report: dict[str, Any]) -> set[str]:
    """Return alert IDs accepted at the upstream SPCS verification boundary.

    The runtime schema-v2 pipeline derives score and alert IDs from enriched
    Kafka records. Those IDs can intentionally differ from the offline oracle's
    pre-enrichment projections, so the sink must reconcile against the passed
    SPCS report rather than re-derive an older expectation.
    """

    lineage = spcs_report.get("schema_v2_lineage")
    if not isinstance(lineage, dict):
        raise ValueError("D7 SPCS report is missing schema_v2_lineage")
    return {
        str(row["risk_alert_event_id"])
        for row in lineage.values()
        if isinstance(row, dict) and row.get("risk_alert_event_id") is not None
    }


def read_sink_counts(
    warehouse: Warehouse, dataset_id: str
) -> dict[str, int]:
    result: dict[str, int] = {}
    for name, table in TABLES.items():
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


def read_admin_ids(warehouse: Warehouse, dataset_id: str) -> set[str]:
    frame = warehouse.fetch_df(
        """
        SELECT alert_id
        FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V
        WHERE dataset_id = %s
        """,
        (dataset_id,),
    )
    return set() if frame.empty else set(frame["alert_id"].astype(str))


def read_required_offsets(
    warehouse: Warehouse, dataset_id: str
) -> dict[tuple[str, int], int]:
    frame = warehouse.fetch_df(
        """
        SELECT source_topic, source_partition,
               MAX(source_offset) + 1 AS required_offset
        FROM POKER_ML_DEMO.SPCS.POKER_EVENT_ENVELOPES
        WHERE dataset_id = %s
        GROUP BY source_topic, source_partition
        """,
        (dataset_id,),
    )
    return {
        (str(row.source_topic), int(row.source_partition)): int(
            row.required_offset
        )
        for row in frame.itertuples(index=False)
    }


def wait_for_sink_rows(
    warehouse: Warehouse,
    manifest: dict[str, Any],
    *,
    expected_admin_alert_ids: set[str],
    timeout_seconds: float,
) -> dict[str, Any]:
    expected = {
        name: int(manifest["expected_counts"][name]) for name in TABLES
    }
    expected["hands"] = int(manifest["expected_counts"]["hands"])
    expected_ids = expected_admin_alert_ids
    deadline = time.monotonic() + timeout_seconds
    observed: dict[str, int] = {}
    observed_ids: set[str] = set()
    while time.monotonic() < deadline:
        observed = read_sink_counts(warehouse, manifest["dataset_id"])
        observed_ids = read_admin_ids(warehouse, manifest["dataset_id"])
        if any(observed[name] > expected[name] for name in expected):
            raise ValueError(
                f"POKER_SINK produced excess rows: expected={expected} "
                f"observed={observed}"
            )
        if observed == expected and observed_ids == expected_ids:
            return {
                "status": "passed",
                "counts": observed,
                "admin_rows": len(observed_ids),
                "admin_alert_ids": sorted(observed_ids),
            }
        time.sleep(1)
    raise TimeoutError(
        "POKER_SINK/admin reconciliation timed out: "
        f"expected_counts={expected} observed_counts={observed} "
        f"expected_admin={len(expected_ids)} observed_admin={len(observed_ids)}"
    )


def wait_for_sink_commits(
    required: dict[tuple[str, int], int],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, int]:
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        client_id="poker-sink-acceptance-verifier-v1",
        **client_kwargs,
    )
    deadline = time.monotonic() + timeout_seconds
    observed: dict[str, int] = {}
    try:
        while time.monotonic() < deadline:
            offsets = admin.list_consumer_group_offsets(SINK_GROUP_ID)
            committed = {
                (item.topic, item.partition): metadata.offset
                for item, metadata in offsets.items()
                if metadata is not None
            }
            observed = {
                f"{topic}[{partition}]": committed.get((topic, partition), -1)
                for topic, partition in sorted(required)
            }
            if all(
                committed.get(key, -1) >= expected
                for key, expected in required.items()
            ):
                return observed
            time.sleep(1)
    finally:
        admin.close()
    raise TimeoutError(
        f"POKER_SINK group did not commit persisted offsets: {observed}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--spcs-report", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    args = parser.parse_args()

    manifest = load_manifest(args.manifest)
    spcs_report = json.loads(args.spcs_report.read_text())
    if (
        spcs_report.get("status") != "passed"
        or spcs_report.get("dataset_id") != manifest["dataset_id"]
        or spcs_report.get("target_dead_letters") != 0
    ):
        raise SystemExit("a passed target-DLQ-free D7 SPCS report is required")
    expected_ids = expected_admin_ids(spcs_report)
    expected_alert_count = int(manifest["expected_counts"]["risk_alerts"])
    if len(expected_ids) != expected_alert_count:
        raise SystemExit(
            "D7 SPCS lineage alert count does not match the sealed manifest: "
            f"expected={expected_alert_count} observed={len(expected_ids)}"
        )

    warehouse = get_warehouse()
    try:
        result = wait_for_sink_rows(
            warehouse,
            manifest,
            expected_admin_alert_ids=expected_ids,
            timeout_seconds=args.timeout_seconds,
        )
        required_offsets = read_required_offsets(
            warehouse, manifest["dataset_id"]
        )
    finally:
        warehouse.close()
    if not required_offsets:
        raise RuntimeError("POKER_SINK produced no persisted Kafka offsets")

    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("sink verification requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    result["consumer_group"] = SINK_GROUP_ID
    result["consumer_commits"] = wait_for_sink_commits(
        required_offsets,
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    result["required_offsets"] = {
        f"{topic}[{partition}]": offset
        for (topic, partition), offset in sorted(required_offsets.items())
    }
    result["dataset_id"] = manifest["dataset_id"]
    result["source_spcs_report"] = str(args.spcs_report.resolve())
    result["snowflake_sinks"] = "passed"
    result["admin"] = "passed"
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**result, "report": str(args.report)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
