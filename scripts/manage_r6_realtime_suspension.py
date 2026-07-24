#!/usr/bin/env python3
"""Start, check, or roll back the controlled R6 POKER_REALTIME suspension."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.ops.realtime_retirement import (
    LEGACY_ALERTS_TOPIC,
    LEGACY_GROUP_ID,
    LEGACY_HANDS_TOPIC,
    LEGACY_SERVICE,
    R6_RUN_TYPE,
    sha256_path,
)
from pipeline.warehouse import get_warehouse


SINK_GROUP_ID = "poker-snowflake-sink-synthetic-v1"
SERVICE_NAMES = ("POKER_REALTIME", "POKER_SINK", "POKER_ADMIN")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _clean_source_commit() -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError("R6 suspension control requires a clean committed worktree")
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _optional_int(value: Any) -> int | None:
    if value is None or pd.isna(value):
        return None
    return int(value)


def _group_name(value: Any) -> str:
    if isinstance(value, (tuple, list)):
        return str(value[0])
    for name in ("group", "group_id"):
        candidate = getattr(value, name, None)
        if candidate is not None:
            return str(candidate)
    return str(value)


def _legacy_kafka_snapshot() -> dict[str, Any]:
    from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition

    settings = get_settings()
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    admin = KafkaAdminClient(
        client_id="poker-r6-suspension-controller-v1",
        **client_kwargs,
    )
    inspector = KafkaConsumer(
        group_id=None,
        enable_auto_commit=False,
        **client_kwargs,
    )
    try:
        group_offsets: dict[str, list[dict[str, Any]]] = {}
        for item in admin.list_consumer_groups():
            group_id = _group_name(item)
            offsets = admin.list_consumer_group_offsets(group_id)
            rows = [
                {
                    "topic": str(partition.topic),
                    "partition": int(partition.partition),
                    "committed": int(metadata.offset),
                }
                for partition, metadata in offsets.items()
                if metadata is not None
                and str(partition.topic)
                in {LEGACY_HANDS_TOPIC, LEGACY_ALERTS_TOPIC}
            ]
            if rows:
                group_offsets[group_id] = sorted(
                    rows,
                    key=lambda row: (row["topic"], row["partition"]),
                )
        descriptions = (
            {
                str(getattr(item, "group", "")): item
                for item in admin.describe_consumer_groups(
                    sorted(group_offsets)
                )
            }
            if group_offsets
            else {}
        )
        topic_ends: dict[tuple[str, int], int] = {}
        for topic in (LEGACY_HANDS_TOPIC, LEGACY_ALERTS_TOPIC):
            partitions = sorted(inspector.partitions_for_topic(topic) or [])
            assignments = [
                TopicPartition(topic, partition) for partition in partitions
            ]
            topic_ends.update(
                {
                    (item.topic, item.partition): int(offset)
                    for item, offset in inspector.end_offsets(assignments).items()
                }
            )
        groups = []
        active_states = {
            "STABLE",
            "PREPARINGREBALANCE",
            "COMPLETINGREBALANCE",
        }
        for group_id, offsets in sorted(group_offsets.items()):
            described = descriptions.get(group_id)
            state = str(getattr(described, "state", "UNKNOWN")).upper()
            members = len(getattr(described, "members", ()) or ())
            rows = []
            total_lag = 0
            for row in offsets:
                end = topic_ends.get((row["topic"], row["partition"]))
                lag = (
                    None
                    if end is None
                    else max(0, int(end) - int(row["committed"]))
                )
                if lag is not None:
                    total_lag += lag
                rows.append({**row, "end": end, "lag": lag})
            groups.append(
                {
                    "group_id": group_id,
                    "state": state,
                    "members": members,
                    "active": state in active_states and members > 0,
                    "offsets": rows,
                    "total_lag": total_lag,
                }
            )
        active = [row for row in groups if row["active"]]
        legacy = next(
            (
                row
                for row in groups
                if row["group_id"] == LEGACY_GROUP_ID
            ),
            {
                "group_id": LEGACY_GROUP_ID,
                "state": "MISSING",
                "members": 0,
                "active": False,
                "offsets": [],
                "total_lag": None,
            },
        )
        return {
            "groups_with_legacy_offsets": groups,
            "active_dependencies": active,
            "legacy_group": legacy,
        }
    finally:
        inspector.close()
        admin.close()


def _service_snapshot() -> dict[str, Any]:
    warehouse = get_warehouse()
    try:
        result: dict[str, Any] = {}
        for name in SERVICE_NAMES:
            service = warehouse.fetch_df(
                f"SHOW SERVICES LIKE '{name}' IN SCHEMA POKER_ML_DEMO.SPCS"
            )
            if len(service) != 1:
                raise RuntimeError(f"expected one service named {name}")
            row = service.iloc[0]
            containers = warehouse.fetch_df(
                "SHOW SERVICE CONTAINERS IN SERVICE "
                f"POKER_ML_DEMO.SPCS.{name}"
            )
            container_rows = []
            for raw in containers.to_dict(orient="records"):
                if raw.get("container_name") is None:
                    continue
                container_rows.append(
                    {
                        "container_name": str(raw["container_name"]),
                        "status": str(raw.get("status")),
                        "image_name": str(raw.get("image_name")),
                        "image_digest": str(raw.get("image_digest")),
                        "restart_count": _optional_int(raw.get("restart_count")),
                        "last_exit_code": _optional_int(raw.get("last_exit_code")),
                    }
                )
            result[name] = {
                "status": str(row["status"]),
                "spec_digest": str(row["spec_digest"]),
                "current_instances": int(row["current_instances"]),
                "target_instances": int(row["target_instances"]),
                "containers": container_rows,
            }
        return result
    finally:
        warehouse.close()


def _canonical_snapshot(dataset_id: str) -> dict[str, Any]:
    warehouse = get_warehouse()
    try:
        admin = warehouse.fetch_df(
            """
            SELECT COUNT(*) AS rows, MAX(ingested_at) AS latest_ingested_at
            FROM POKER_ML_DEMO.SPCS.POKER_ALERT_REVIEW_V
            WHERE dataset_id = %s
            """,
            (dataset_id,),
        ).iloc[0]
        dead_letters = warehouse.fetch_df(
            """
            SELECT COUNT(*) AS rows, MAX(ingested_at) AS latest_ingested_at
            FROM POKER_ML_DEMO.SPCS.POKER_SINK_DEAD_LETTERS
            """
        ).iloc[0]
        return {
            "dataset_id": dataset_id,
            "admin_rows": int(admin["rows"]),
            "admin_latest_ingested_at": (
                None
                if pd.isna(admin["latest_ingested_at"])
                else str(admin["latest_ingested_at"])
            ),
            "sink_dead_letter_rows": int(dead_letters["rows"]),
            "sink_dead_letter_latest_ingested_at": (
                None
                if pd.isna(dead_letters["latest_ingested_at"])
                else str(dead_letters["latest_ingested_at"])
            ),
        }
    finally:
        warehouse.close()


def _sink_lag(topics: list[str]) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    settings = get_settings()
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    inspector = KafkaConsumer(
        group_id=None,
        enable_auto_commit=False,
        **client_kwargs,
    )
    group = KafkaConsumer(
        group_id=SINK_GROUP_ID,
        enable_auto_commit=False,
        **client_kwargs,
    )
    try:
        rows = []
        total_lag = 0
        for topic in sorted(set(topics)):
            partitions = sorted(inspector.partitions_for_topic(topic) or [])
            assignments = [TopicPartition(topic, value) for value in partitions]
            endings = inspector.end_offsets(assignments)
            for item in assignments:
                committed = group.committed(item)
                effective = 0 if committed is None else int(committed)
                lag = max(0, int(endings[item]) - effective)
                total_lag += lag
                rows.append(
                    {
                        "topic": topic,
                        "partition": item.partition,
                        "committed": committed,
                        "end": int(endings[item]),
                        "lag": lag,
                    }
                )
        return {"total_lag": total_lag, "partitions": rows}
    finally:
        group.close()
        inspector.close()


def _snapshot(dataset_id: str, topics: list[str]) -> dict[str, Any]:
    return {
        "captured_at": _iso(_now()),
        "services": _service_snapshot(),
        "canonical": _canonical_snapshot(dataset_id),
        "sink_lag": _sink_lag(topics),
        "legacy_kafka": _legacy_kafka_snapshot(),
    }


def _ready(service: dict[str, Any], containers: int) -> bool:
    return (
        service["status"] == "RUNNING"
        and len(service["containers"]) == containers
        and all(row["status"] == "READY" for row in service["containers"])
    )


def _offset_map(group: dict[str, Any]) -> dict[tuple[str, int], int]:
    return {
        (str(row["topic"]), int(row["partition"])): int(row["committed"])
        for row in group.get("offsets", [])
    }


def _offsets_cover(
    current: dict[str, Any],
    expected: dict[str, Any],
) -> bool:
    actual_offsets = _offset_map(current)
    return all(
        actual_offsets.get(key, -1) >= value
        for key, value in _offset_map(expected).items()
    )


def _suspended_kafka_healthy(value: dict[str, Any]) -> bool:
    return not value["active_dependencies"]


def _running_kafka_healthy(value: dict[str, Any]) -> bool:
    active = value["active_dependencies"]
    return (
        len(active) == 1
        and active[0]["group_id"] == LEGACY_GROUP_ID
        and {
            str(row["topic"])
            for row in active[0]["offsets"]
        }
        == {LEGACY_HANDS_TOPIC}
    )


def _validate_running_baseline(
    snapshot: dict[str, Any],
    *,
    preflight: dict[str, Any],
    parity: dict[str, Any],
) -> None:
    services = snapshot["services"]
    realtime = services[LEGACY_SERVICE]
    rollback = preflight["rollback"]
    if (
        not _ready(realtime, 1)
        or realtime["spec_digest"] != rollback["spec_digest"]
        or realtime["containers"][0]["image_digest"]
        != rollback["containers"][0]["image_digest"]
        or not _ready(services["POKER_SINK"], 2)
        or not _ready(services["POKER_ADMIN"], 1)
        or snapshot["sink_lag"]["total_lag"] != 0
        or snapshot["canonical"]["admin_rows"]
        != int(parity["canonical"]["admin_rows"])
        or not _running_kafka_healthy(snapshot["legacy_kafka"])
        or not snapshot["legacy_kafka"]["legacy_group"]["active"]
        or snapshot["legacy_kafka"]["legacy_group"]["total_lag"] != 0
        or not _offsets_cover(
            snapshot["legacy_kafka"]["legacy_group"],
            {
                "offsets": [
                    {
                        "topic": name.rsplit("[", 1)[0],
                        "partition": int(name.rsplit("[", 1)[1][:-1]),
                        "committed": offset,
                    }
                    for name, offset in parity["consumer_commits"].items()
                ]
            },
        )
    ):
        raise RuntimeError("R6 live baseline changed after bounded parity acceptance")


def _alter_realtime(state: str) -> None:
    if state not in {"SUSPEND", "RESUME"}:
        raise ValueError(f"unsupported POKER_REALTIME state change: {state}")
    warehouse = get_warehouse()
    try:
        warehouse.execute(
            "ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_REALTIME "
            + state
        )
    finally:
        warehouse.close()


def _wait_for_realtime(
    expected: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = _service_snapshot()[LEGACY_SERVICE]
        if expected == "SUSPENDED" and latest["status"] == "SUSPENDED":
            return latest
        if expected == "READY" and _ready(latest, 1):
            return latest
        time.sleep(2)
    raise TimeoutError(
        f"POKER_REALTIME did not reach {expected}: {latest}"
    )


def _wait_for_legacy_kafka(
    *,
    active: bool,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, Any] = {}
    while time.monotonic() < deadline:
        latest = _legacy_kafka_snapshot()
        legacy = latest["legacy_group"]
        if active:
            if (
                legacy["active"]
                and legacy["members"] > 0
                and legacy["total_lag"] == 0
            ):
                return latest
        elif _suspended_kafka_healthy(latest):
            return latest
        time.sleep(2)
    expected = "active and caught up" if active else "inactive"
    raise TimeoutError(
        f"legacy Kafka dependencies did not become {expected}: {latest}"
    )


def _load_chain(
    preflight_path: Path,
    parity_path: Path,
    manifest_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    preflight = _read_json(preflight_path)
    parity = _read_json(parity_path)
    manifest = _read_json(manifest_path)
    if (
        preflight.get("schema_version") != 1
        or preflight.get("run_type") != R6_RUN_TYPE
        or preflight.get("phase") != "preflight"
        or preflight.get("status") != "passed"
        or parity.get("schema_version") != 1
        or parity.get("run_type") != R6_RUN_TYPE
        or parity.get("phase") != "bounded_parity"
        or parity.get("status") != "passed"
        or parity.get("dataset_id") != manifest.get("dataset_id")
        or parity.get("source_commit") != preflight.get("source_commit")
    ):
        raise ValueError("R6 suspension requires one passed preflight/parity chain")
    return preflight, parity, manifest


def _validate_start_report(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    start_report = _read_json(path)
    if (
        start_report.get("schema_version") != 1
        or start_report.get("run_type") != R6_RUN_TYPE
        or start_report.get("phase") != "suspension_start"
        or start_report.get("status") != "observation_started"
        or not start_report.get("dataset_id")
        or not start_report.get("minimum_end_at")
    ):
        raise ValueError("invalid R6 suspension start report")
    source_reports = start_report.get("source_reports", {})
    for name in ("preflight", "parity", "d7_manifest"):
        source = source_reports.get(name, {})
        source_path = Path(str(source.get("path", "")))
        if (
            not source_path.is_file()
            or source.get("sha256") != sha256_path(source_path)
        ):
            raise ValueError(f"R6 {name} evidence changed after suspension start")
    source = source_reports["d7_manifest"]
    manifest_path = Path(str(source.get("path", "")))
    manifest = _read_json(manifest_path)
    if manifest.get("dataset_id") != start_report["dataset_id"]:
        raise ValueError("R6 suspension dataset does not match D7 manifest")
    return start_report, manifest


def _validate_completed_check(
    path: Path,
    start_report_path: Path,
) -> dict[str, Any]:
    report = _read_json(path)
    expected_start_hash = sha256_path(start_report_path)
    if (
        report.get("schema_version") != 1
        or report.get("run_type") != R6_RUN_TYPE
        or report.get("phase") != "suspension_check"
        or report.get("status") != "observation_window_complete"
        or report.get("start_report", {}).get("sha256")
        != expected_start_hash
    ):
        raise ValueError(
            "R6 rollback requires a completed, hash-bound suspension check"
        )
    return report


def _write_once(path: Path, value: dict[str, Any]) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite R6 evidence: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def start(args: argparse.Namespace) -> None:
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")
    controller_commit = _clean_source_commit()
    preflight, parity, manifest = _load_chain(
        args.preflight_report,
        args.parity_report,
        args.manifest,
    )
    topics = list(manifest["topics"].values())
    before = _snapshot(str(parity["dataset_id"]), topics)
    _validate_running_baseline(before, preflight=preflight, parity=parity)
    started_at = _now()
    try:
        _alter_realtime("SUSPEND")
        _wait_for_realtime("SUSPENDED", timeout_seconds=args.timeout_seconds)
        _wait_for_legacy_kafka(
            active=False,
            timeout_seconds=args.timeout_seconds,
        )
        after = _snapshot(str(parity["dataset_id"]), topics)
        if (
            after["services"][LEGACY_SERVICE]["status"] != "SUSPENDED"
            or not _ready(after["services"]["POKER_SINK"], 2)
            or not _ready(after["services"]["POKER_ADMIN"], 1)
            or after["sink_lag"]["total_lag"] != 0
            or after["canonical"]["admin_rows"]
            != before["canonical"]["admin_rows"]
            or not _suspended_kafka_healthy(after["legacy_kafka"])
            or not _offsets_cover(
                after["legacy_kafka"]["legacy_group"],
                before["legacy_kafka"]["legacy_group"],
            )
        ):
            raise RuntimeError("canonical path failed immediately after suspension")
    except Exception:
        _alter_realtime("RESUME")
        _wait_for_realtime("READY", timeout_seconds=args.timeout_seconds)
        _wait_for_legacy_kafka(
            active=True,
            timeout_seconds=args.timeout_seconds,
        )
        raise
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "suspension_start",
        "status": "observation_started",
        "controller_commit": controller_commit,
        "dataset_id": parity["dataset_id"],
        "source_reports": {
            "preflight": {
                "path": str(args.preflight_report.resolve()),
                "sha256": sha256_path(args.preflight_report),
            },
            "parity": {
                "path": str(args.parity_report.resolve()),
                "sha256": sha256_path(args.parity_report),
            },
            "d7_manifest": {
                "path": str(args.manifest.resolve()),
                "sha256": sha256_path(args.manifest),
            },
        },
        "started_at": _iso(started_at),
        "minimum_end_at": _iso(started_at + timedelta(hours=24)),
        "rollback": preflight["rollback"],
        "before": before,
        "after": after,
    }
    _write_once(args.report, report)
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))


def check(args: argparse.Namespace) -> None:
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")
    start_report, manifest = _validate_start_report(args.start_report)
    current = _snapshot(
        str(start_report["dataset_id"]),
        list(manifest["topics"].values()),
    )
    baseline = start_report["after"]
    healthy = (
        current["services"][LEGACY_SERVICE]["status"] == "SUSPENDED"
        and _ready(current["services"]["POKER_SINK"], 2)
        and _ready(current["services"]["POKER_ADMIN"], 1)
        and current["sink_lag"]["total_lag"] == 0
        and current["canonical"]["admin_rows"]
        >= baseline["canonical"]["admin_rows"]
        and current["canonical"]["sink_dead_letter_rows"]
        == baseline["canonical"]["sink_dead_letter_rows"]
        and _suspended_kafka_healthy(current["legacy_kafka"])
        and _offsets_cover(
            current["legacy_kafka"]["legacy_group"],
            baseline["legacy_kafka"]["legacy_group"],
        )
    )
    now = _now()
    minimum_end = pd.Timestamp(start_report["minimum_end_at"]).to_pydatetime()
    phase_status = (
        "observation_window_complete"
        if now >= minimum_end
        else "observation_in_progress"
    )
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "suspension_check",
        "status": phase_status if healthy else "failed",
        "start_report": {
            "path": str(args.start_report.resolve()),
            "sha256": sha256_path(args.start_report),
        },
        "minimum_end_at": start_report["minimum_end_at"],
        "remaining_seconds": max(0.0, (minimum_end - now).total_seconds()),
        "current": current,
    }
    _write_once(args.report, report)
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if not healthy:
        raise SystemExit("R6 suspension observation is unhealthy")


def rollback(args: argparse.Namespace) -> None:
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")
    start_report, _manifest = _validate_start_report(args.start_report)
    minimum_end = pd.Timestamp(start_report["minimum_end_at"]).to_pydatetime()
    if _now() < minimum_end and not args.allow_early:
        raise SystemExit("R6 rollback drill is blocked until the 24-hour window ends")
    completed_check = None
    if not args.allow_early:
        if args.check_report is None:
            raise SystemExit(
                "R6 rollback requires --check-report after the 24-hour window"
            )
        completed_check = _validate_completed_check(
            args.check_report,
            args.start_report,
        )
    rollback_contract = start_report["rollback"]
    before = _service_snapshot()[LEGACY_SERVICE]
    _alter_realtime("RESUME")
    ready = _wait_for_realtime("READY", timeout_seconds=args.timeout_seconds)
    kafka = _wait_for_legacy_kafka(
        active=True,
        timeout_seconds=args.timeout_seconds,
    )
    expected_group = start_report["before"]["legacy_kafka"]["legacy_group"]
    passed = (
        ready["spec_digest"] == rollback_contract["spec_digest"]
        and ready["containers"][0]["image_digest"]
        == rollback_contract["containers"][0]["image_digest"]
        and _offsets_cover(kafka["legacy_group"], expected_group)
        and kafka["legacy_group"]["total_lag"] == 0
    )
    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "rollback",
        "status": "passed" if passed else "failed",
        "start_report": {
            "path": str(args.start_report.resolve()),
            "sha256": sha256_path(args.start_report),
        },
        "completed_check": (
            None
            if completed_check is None
            else {
                "path": str(args.check_report.resolve()),
                "sha256": sha256_path(args.check_report),
            }
        ),
        "rolled_back_at": _iso(_now()),
        "before": before,
        "after": ready,
        "legacy_kafka": kafka,
        "expected_spec_digest": rollback_contract["spec_digest"],
        "expected_image_digest": rollback_contract["containers"][0][
            "image_digest"
        ],
    }
    _write_once(args.report, report)
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if not passed:
        raise SystemExit("R6 rollback identity verification failed")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    start_parser = sub.add_parser("start")
    start_parser.add_argument("--preflight-report", type=Path, required=True)
    start_parser.add_argument("--parity-report", type=Path, required=True)
    start_parser.add_argument("--manifest", type=Path, required=True)
    start_parser.add_argument("--report", type=Path, required=True)
    start_parser.add_argument("--timeout-seconds", type=float, default=180.0)
    check_parser = sub.add_parser("check")
    check_parser.add_argument("--start-report", type=Path, required=True)
    check_parser.add_argument("--report", type=Path, required=True)
    rollback_parser = sub.add_parser("rollback")
    rollback_parser.add_argument("--start-report", type=Path, required=True)
    rollback_parser.add_argument("--check-report", type=Path)
    rollback_parser.add_argument("--report", type=Path, required=True)
    rollback_parser.add_argument("--timeout-seconds", type=float, default=180.0)
    rollback_parser.add_argument("--allow-early", action="store_true")
    args = parser.parse_args()
    if args.command == "start":
        start(args)
    elif args.command == "check":
        check(args)
    else:
        rollback(args)


if __name__ == "__main__":
    main()
