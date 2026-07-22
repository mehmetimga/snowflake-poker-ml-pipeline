#!/usr/bin/env python3
"""Validate and idempotently register the local Debezium simulation connector."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / "infra/simulation/debezium/poker-hand-outbox-connector.json"


def load_connector(path: Path) -> dict:
    document = json.loads(path.read_text())
    if set(document) != {"name", "config"}:
        raise ValueError("connector document must contain only name and config")
    config = document["config"]
    required = {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.dbname": "poker_sim",
        "table.include.list": "public.hand_completed_outbox",
        "binary.handling.mode": "base64",
        "publication.autocreate.mode": "disabled",
        "snapshot.locking.mode": "shared",
        "transforms.routeHandOutbox.replacement": "poker.sim.cdc-hand-outbox.v1",
    }
    for key, expected in required.items():
        if config.get(key) != expected:
            raise ValueError(f"unsafe connector value for {key}: {config.get(key)!r}")
    if "Filter" in json.dumps(config):
        raise ValueError(
            "business filtering must stay in the PostgreSQL outbox trigger"
        )
    return document


def request_json(
    url: str,
    *,
    method: str = "GET",
    body: dict | None = None,
    timeout: float = 5.0,
) -> object:
    data = None if body is None else json.dumps(body).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"Kafka Connect HTTP {exc.code}: {detail}") from exc


def wait_for_connect(base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            request_json(f"{base_url}/connector-plugins")
            return
        except (OSError, RuntimeError) as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"Kafka Connect was not ready within {timeout}s: {last_error}")


def wait_for_connector(base_url: str, name: str, timeout: float) -> dict:
    deadline = time.monotonic() + timeout
    latest: dict = {}
    while time.monotonic() < deadline:
        try:
            status = request_json(f"{base_url}/connectors/{name}/status")
        except (OSError, RuntimeError) as exc:
            # Kafka Connect can briefly return 404 after accepting a new
            # connector while its distributed status record is created.
            latest = {"request_error": str(exc)}
            time.sleep(1)
            continue
        if isinstance(status, dict):
            latest = status
            connector_state = status.get("connector", {}).get("state")
            task_states = [task.get("state") for task in status.get("tasks", [])]
            if connector_state == "RUNNING" and task_states == ["RUNNING"]:
                return status
            if connector_state == "FAILED" or "FAILED" in task_states:
                raise RuntimeError(f"Debezium connector failed: {status}")
        time.sleep(1)
    raise RuntimeError(
        f"Debezium connector was not running within {timeout}s: {latest}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--connect-url", default="http://localhost:8083")
    parser.add_argument("--timeout", type=float, default=90.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--check-only", action="store_true")
    mode.add_argument("--status-only", action="store_true")
    args = parser.parse_args()

    document = load_connector(args.config)
    if args.check_only:
        print(
            json.dumps(
                {
                    "status": "valid",
                    "name": document["name"],
                    "source_table": document["config"]["table.include.list"],
                    "target_topic": document["config"][
                        "transforms.routeHandOutbox.replacement"
                    ],
                },
                sort_keys=True,
            )
        )
        return

    base_url = args.connect_url.rstrip("/")
    wait_for_connect(base_url, args.timeout)
    if args.status_only:
        status = wait_for_connector(base_url, document["name"], args.timeout)
        print(json.dumps(status, indent=2, sort_keys=True))
        return
    request_json(
        f"{base_url}/connectors/{document['name']}/config",
        method="PUT",
        body=document["config"],
    )
    status = wait_for_connector(base_url, document["name"], args.timeout)
    print(json.dumps(status, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
