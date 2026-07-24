#!/usr/bin/env python3
"""Capture the read-only R6 dependency and rollback baseline."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.ops.realtime_retirement import (
    LEGACY_ALERTS_TOPIC,
    LEGACY_GROUP_ID,
    LEGACY_HANDS_TOPIC,
    LEGACY_SERVICE,
    R6_RUN_TYPE,
    build_dependency_audit,
)
from pipeline.warehouse import get_warehouse


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def clean_source_commit() -> str:
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise RuntimeError("R6 baseline requires a clean committed worktree")
    return subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _snowflake_services() -> tuple[
    dict[str, str],
    dict[str, dict[str, Any]],
    list[dict[str, Any]],
]:
    warehouse = get_warehouse()
    try:
        warehouse.execute("USE ROLE SYSADMIN")
        services = warehouse.fetch_df(
            "SHOW SERVICES IN SCHEMA POKER_ML_DEMO.SPCS"
        )
        specs: dict[str, str] = {}
        metadata: dict[str, dict[str, Any]] = {}
        for row in services.to_dict(orient="records"):
            if str(row.get("is_job", "false")).lower() == "true":
                continue
            name = str(row["name"]).upper()
            described = warehouse.fetch_df(
                f"DESCRIBE SERVICE POKER_ML_DEMO.SPCS.{name}"
            )
            if len(described) != 1:
                raise RuntimeError(f"cannot inspect service {name}")
            detail = described.iloc[0].to_dict()
            spec = detail.get("spec")
            if not isinstance(spec, str) or not spec.strip():
                raise RuntimeError(f"service spec is unavailable: {name}")
            specs[name] = spec
            metadata[name] = {
                "status": str(row.get("status")),
                "spec_digest": str(row.get("spec_digest")),
                "current_instances": int(row.get("current_instances", 0)),
                "target_instances": int(row.get("target_instances", 0)),
            }
        containers = warehouse.fetch_df(
            "SHOW SERVICE CONTAINERS IN SERVICE "
            "POKER_ML_DEMO.SPCS.POKER_REALTIME"
        )
        return specs, metadata, containers.to_dict(orient="records")
    finally:
        warehouse.close()


def _group_name(value: Any) -> str:
    if isinstance(value, (tuple, list)):
        return str(value[0])
    for name in ("group", "group_id"):
        candidate = getattr(value, name, None)
        if candidate is not None:
            return str(candidate)
    return str(value)


def _kafka_dependencies(client_kwargs: dict[str, Any]) -> list[dict[str, Any]]:
    from kafka import KafkaAdminClient

    admin = KafkaAdminClient(
        client_id="poker-r6-dependency-audit-v1",
        **client_kwargs,
    )
    try:
        candidates: list[tuple[str, list[str]]] = []
        for item in admin.list_consumer_groups():
            group_id = _group_name(item)
            offsets = admin.list_consumer_group_offsets(group_id)
            topics = sorted(
                {
                    str(partition.topic)
                    for partition in offsets
                    if str(partition.topic)
                    in {LEGACY_HANDS_TOPIC, LEGACY_ALERTS_TOPIC}
                }
            )
            if topics:
                candidates.append((group_id, topics))
        descriptions = {
            str(getattr(item, "group", "")): item
            for item in admin.describe_consumer_groups(
                [group_id for group_id, _topics in candidates]
            )
        } if candidates else {}
        result = []
        for group_id, topics in candidates:
            described = descriptions.get(group_id)
            result.append(
                {
                    "group_id": group_id,
                    "state": str(getattr(described, "state", "UNKNOWN")),
                    "members": len(getattr(described, "members", ()) or ()),
                    "topics": topics,
                }
            )
        return result
    finally:
        admin.close()


def _legacy_offsets(client_kwargs: dict[str, Any]) -> dict[str, Any]:
    from kafka import KafkaConsumer, TopicPartition

    inspector = KafkaConsumer(
        group_id=None,
        enable_auto_commit=False,
        **client_kwargs,
    )
    group = KafkaConsumer(
        group_id=LEGACY_GROUP_ID,
        enable_auto_commit=False,
        **client_kwargs,
    )
    try:
        result: dict[str, Any] = {}
        for topic in (LEGACY_HANDS_TOPIC, LEGACY_ALERTS_TOPIC):
            partitions = sorted(inspector.partitions_for_topic(topic) or [])
            assignments = [TopicPartition(topic, value) for value in partitions]
            ends = inspector.end_offsets(assignments)
            result[topic] = [
                {
                    "partition": item.partition,
                    "committed": group.committed(item),
                    "end": int(ends[item]),
                }
                for item in assignments
            ]
        return result
    finally:
        group.close()
        inspector.close()


def _local_repo_processes() -> list[str]:
    completed = subprocess.run(
        ["ps", "-axo", "pid=,command="],
        check=True,
        capture_output=True,
        text=True,
    )
    root = str(Path.cwd().resolve())
    own_pid = str(os.getpid())
    result = []
    for line in completed.stdout.splitlines():
        text = line.strip()
        pid = text.split(maxsplit=1)[0] if text else ""
        if pid == own_pid or root not in text:
            continue
        if any(
            marker in text
            for marker in (
                "scripts/realtime.py",
                "scripts/generate.py",
                "flink_realtime",
                "flink-realtime",
            )
        ):
            result.append(text)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        raise SystemExit(f"refusing to overwrite R6 evidence: {args.report}")

    source_commit = clean_source_commit()
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("R6 audit requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    specs, services, containers = _snowflake_services()
    groups = _kafka_dependencies(client_kwargs)
    processes = _local_repo_processes()
    audit = build_dependency_audit(
        service_specs=specs,
        kafka_groups=groups,
        local_processes=processes,
    )
    realtime_containers = [
        {
            "container_name": str(row.get("container_name")),
            "status": str(row.get("status")),
            "image_name": str(row.get("image_name")),
            "image_digest": str(row.get("image_digest")),
            "restart_count": int(row.get("restart_count", 0)),
        }
        for row in containers
        if row.get("container_name") is not None
    ]
    if (
        services.get(LEGACY_SERVICE, {}).get("status") != "RUNNING"
        or len(realtime_containers) != 1
        or realtime_containers[0]["status"] != "READY"
    ):
        audit["status"] = "failed"
        audit["blockers"].append(
            "POKER_REALTIME must be RUNNING/READY at baseline capture"
        )

    report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "preflight",
        "status": audit["status"],
        "captured_at": _iso_now(),
        "source_commit": source_commit,
        "dependency_audit": audit,
        "services": services,
        "kafka_groups_with_legacy_offsets": groups,
        "legacy_offsets": _legacy_offsets(client_kwargs),
        "rollback": {
            "service": "POKER_ML_DEMO.SPCS.POKER_REALTIME",
            "spec": specs.get(LEGACY_SERVICE),
            "spec_digest": services.get(LEGACY_SERVICE, {}).get("spec_digest"),
            "containers": realtime_containers,
            "consumer_group": LEGACY_GROUP_ID,
            "suspend_command": (
                "ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_REALTIME SUSPEND"
            ),
            "resume_command": (
                "ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_REALTIME RESUME"
            ),
        },
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))
    if report["status"] != "passed":
        raise SystemExit("R6 dependency preflight failed")


if __name__ == "__main__":
    main()
