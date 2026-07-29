#!/usr/bin/env python3
"""Recover durable R6 suspension evidence and run its guarded rollback drill."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.ops.realtime_retirement import (
    LEGACY_HANDS_TOPIC,
    LEGACY_SERVICE,
    R6_RUN_TYPE,
    sha256_path,
)
from pipeline.warehouse import get_warehouse
from scripts.manage_r6_realtime_suspension import (
    _alter_realtime,
    _canonical_snapshot,
    _iso,
    _legacy_kafka_snapshot,
    _offset_map,
    _ready,
    _service_snapshot,
    _sink_lag,
    _snapshot,
    _wait_for_legacy_kafka,
    _wait_for_realtime,
    _write_once,
)


DATASET_ID = "multitable-alert-acceptance-v1"
CANONICAL_TOPICS = (
    "poker.synthetic.hand-player-context.v2",
    "poker.synthetic.hands.raw.v1",
    "poker.synthetic.pair-features.context-v2.v1",
    "poker.synthetic.pipeline.dead-letter.v1",
    "poker.synthetic.review-decisions.v1",
    "poker.synthetic.risk-alerts.v1",
    "poker.synthetic.risk-scores.v1",
    "poker.synthetic.rule-evidence.v1",
)
EXPECTED_SPEC_DIGEST = (
    "2941d7339e54e0105e9254c1999cdd07c6d7171dda63879b0eb7f01e2d89249d"
)
EXPECTED_IMAGE_DIGEST = (
    "sha256:79d87545891735cdc057915beaaa7a6288c83142bcf91859d29cac65f176fd3a"
)
EXPECTED_ADMIN_ROWS = 14
EXPECTED_DEAD_LETTER_ROWS = 139
EXPECTED_LEGACY_OFFSET = 99
MINIMUM_SUSPENSION_HOURS = 24.0


def _tracked_source_commit() -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=all"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    dirty = [
        line
        for line in status.splitlines()
        if not line.startswith("?? evidence/r6-realtime-retirement-")
    ]
    if dirty:
        raise RuntimeError(
            "R6 recovery requires all tracked source changes to be committed"
        )
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _timestamp(value: Any) -> datetime | None:
    if value is None or pd.isna(value):
        return None
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.to_pydatetime()


def _service_lifecycle() -> dict[str, Any]:
    warehouse = get_warehouse()
    try:
        service = warehouse.fetch_df(
            "SHOW SERVICES LIKE 'POKER_REALTIME' "
            "IN SCHEMA POKER_ML_DEMO.SPCS"
        )
        if len(service) != 1:
            raise RuntimeError("expected one POKER_REALTIME service")
        row = service.iloc[0]
        result: dict[str, Any] = {}
        for name in (
            "created_on",
            "updated_on",
            "resumed_on",
            "suspended_on",
        ):
            value = _timestamp(row.get(name))
            result[name] = None if value is None else _iso(value)
        result["status"] = str(row["status"])
        result["spec_digest"] = str(row["spec_digest"])
        return result
    finally:
        warehouse.close()


def evaluate_recovery(
    *,
    snapshot: dict[str, Any],
    lifecycle: dict[str, Any],
    observed_at: datetime,
) -> tuple[list[str], float | None]:
    blockers: list[str] = []
    realtime = snapshot["services"][LEGACY_SERVICE]
    suspended_at = _timestamp(lifecycle.get("suspended_on"))
    resumed_at = _timestamp(lifecycle.get("resumed_on"))
    updated_at = _timestamp(lifecycle.get("updated_on"))
    elapsed_hours = (
        None
        if suspended_at is None
        else (observed_at - suspended_at).total_seconds() / 3600.0
    )
    if realtime["status"] != "SUSPENDED" or lifecycle["status"] != "SUSPENDED":
        blockers.append("POKER_REALTIME is not suspended")
    if realtime["spec_digest"] != EXPECTED_SPEC_DIGEST:
        blockers.append("POKER_REALTIME spec digest changed")
    if suspended_at is None:
        blockers.append("Snowflake has no POKER_REALTIME suspension timestamp")
    elif elapsed_hours is None or elapsed_hours < MINIMUM_SUSPENSION_HOURS:
        blockers.append("POKER_REALTIME suspension is shorter than 24 hours")
    if (
        suspended_at is not None
        and resumed_at is not None
        and resumed_at >= suspended_at
    ):
        blockers.append("POKER_REALTIME was resumed after the observed suspension")
    if (
        suspended_at is not None
        and updated_at is not None
        and updated_at != suspended_at
    ):
        blockers.append(
            "POKER_REALTIME metadata changed after the suspension timestamp"
        )
    if not _ready(snapshot["services"]["POKER_SINK"], 2):
        blockers.append("POKER_SINK is not running with two ready containers")
    if not _ready(snapshot["services"]["POKER_ADMIN"], 1):
        blockers.append("POKER_ADMIN is not running with one ready container")
    if snapshot["sink_lag"]["total_lag"] != 0:
        blockers.append("canonical sink lag is not zero")
    if snapshot["canonical"]["admin_rows"] != EXPECTED_ADMIN_ROWS:
        blockers.append("canonical admin row count changed")
    if (
        snapshot["canonical"]["sink_dead_letter_rows"]
        != EXPECTED_DEAD_LETTER_ROWS
    ):
        blockers.append("sink dead-letter count changed")
    legacy = snapshot["legacy_kafka"]
    if legacy["active_dependencies"]:
        blockers.append("an active legacy Kafka dependency exists")
    legacy_group = legacy["legacy_group"]
    expected_key = (LEGACY_HANDS_TOPIC, 0)
    offsets = _offset_map(legacy_group)
    if (
        offsets.get(expected_key) != EXPECTED_LEGACY_OFFSET
        or legacy_group["total_lag"] != 0
        or any(
            row["end"] != EXPECTED_LEGACY_OFFSET
            for row in legacy_group["offsets"]
            if (row["topic"], row["partition"]) == expected_key
        )
    ):
        blockers.append("legacy Kafka offset 99/zero-lag contract changed")
    return blockers, elapsed_hours


def capture(args: argparse.Namespace) -> None:
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")
    controller_commit = _tracked_source_commit()
    observed_at = datetime.now(timezone.utc)
    lifecycle = _service_lifecycle()
    snapshot = _snapshot(DATASET_ID, list(CANONICAL_TOPICS))
    blockers, elapsed_hours = evaluate_recovery(
        snapshot=snapshot,
        lifecycle=lifecycle,
        observed_at=observed_at,
    )
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "suspension_observation_recovery",
        "status": "passed" if not blockers else "failed",
        "controller_commit": controller_commit,
        "recovery_reason": (
            "original immutable reports were purged from /private/tmp; "
            "Snowflake lifecycle metadata and current Snowflake/Kafka state "
            "are used as the authoritative recovery evidence"
        ),
        "limitations": [
            "the deleted byte-for-byte preflight/parity/start reports cannot "
            "be reconstructed",
            "this report does not claim hashes for deleted evidence",
        ],
        "observed_at": _iso(observed_at),
        "minimum_suspension_hours": MINIMUM_SUSPENSION_HOURS,
        "observed_suspension_hours": elapsed_hours,
        "service_lifecycle": lifecycle,
        "expected_contract": {
            "dataset_id": DATASET_ID,
            "spec_digest": EXPECTED_SPEC_DIGEST,
            "image_digest": EXPECTED_IMAGE_DIGEST,
            "admin_rows": EXPECTED_ADMIN_ROWS,
            "sink_dead_letter_rows": EXPECTED_DEAD_LETTER_ROWS,
            "legacy_topic": LEGACY_HANDS_TOPIC,
            "legacy_partition": 0,
            "legacy_offset": EXPECTED_LEGACY_OFFSET,
        },
        "blockers": blockers,
        "current": snapshot,
    }
    _write_once(args.report, report)
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if blockers:
        raise SystemExit("R6 suspension recovery evidence failed")


def _validate_recovery_report(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != 1
        or value.get("run_type") != R6_RUN_TYPE
        or value.get("phase") != "suspension_observation_recovery"
        or value.get("status") != "passed"
        or value.get("blockers") != []
        or value.get("expected_contract", {}).get("spec_digest")
        != EXPECTED_SPEC_DIGEST
        or value.get("expected_contract", {}).get("image_digest")
        != EXPECTED_IMAGE_DIGEST
        or float(value.get("observed_suspension_hours", 0))
        < MINIMUM_SUSPENSION_HOURS
    ):
        raise ValueError("invalid R6 suspension recovery report")
    return value


def rollback(args: argparse.Namespace) -> None:
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")
    controller_commit = _tracked_source_commit()
    recovery = _validate_recovery_report(args.recovery_report)
    if recovery["controller_commit"] != controller_commit:
        raise SystemExit("R6 recovery report is not bound to this controller commit")

    before_lifecycle = _service_lifecycle()
    before = _snapshot(DATASET_ID, list(CANONICAL_TOPICS))
    blockers, _elapsed = evaluate_recovery(
        snapshot=before,
        lifecycle=before_lifecycle,
        observed_at=datetime.now(timezone.utc),
    )
    if blockers:
        raise SystemExit(
            "R6 live recovery contract changed before rollback: "
            + "; ".join(blockers)
        )

    _alter_realtime("RESUME")
    ready = _wait_for_realtime(
        "READY",
        timeout_seconds=args.timeout_seconds,
    )
    kafka = _wait_for_legacy_kafka(
        active=True,
        timeout_seconds=args.timeout_seconds,
    )
    canonical = _canonical_snapshot(DATASET_ID)
    sink_lag = _sink_lag(list(CANONICAL_TOPICS))
    offsets = _offset_map(kafka["legacy_group"])
    passed = (
        ready["spec_digest"] == EXPECTED_SPEC_DIGEST
        and len(ready["containers"]) == 1
        and ready["containers"][0]["image_digest"] == EXPECTED_IMAGE_DIGEST
        and offsets.get((LEGACY_HANDS_TOPIC, 0)) >= EXPECTED_LEGACY_OFFSET
        and kafka["legacy_group"]["total_lag"] == 0
        and canonical["admin_rows"] == EXPECTED_ADMIN_ROWS
        and canonical["sink_dead_letter_rows"] == EXPECTED_DEAD_LETTER_ROWS
        and sink_lag["total_lag"] == 0
    )
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "recovered_rollback",
        "status": "passed" if passed else "failed",
        "controller_commit": controller_commit,
        "recovery_report": {
            "path": str(args.recovery_report.resolve()),
            "sha256": sha256_path(args.recovery_report),
        },
        "rolled_back_at": _iso(datetime.now(timezone.utc)),
        "before": before,
        "before_lifecycle": before_lifecycle,
        "after": {
            "service": ready,
            "legacy_kafka": kafka,
            "canonical": canonical,
            "sink_lag": sink_lag,
        },
        "expected_spec_digest": EXPECTED_SPEC_DIGEST,
        "expected_image_digest": EXPECTED_IMAGE_DIGEST,
    }
    _write_once(args.report, report)
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit("R6 recovered rollback verification failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    capture_parser = sub.add_parser("capture")
    capture_parser.add_argument("--report", type=Path, required=True)
    rollback_parser = sub.add_parser("rollback")
    rollback_parser.add_argument("--recovery-report", type=Path, required=True)
    rollback_parser.add_argument("--report", type=Path, required=True)
    rollback_parser.add_argument("--timeout-seconds", type=float, default=180.0)
    args = parser.parse_args()
    if args.command == "capture":
        capture(args)
    else:
        rollback(args)


if __name__ == "__main__":
    main()
