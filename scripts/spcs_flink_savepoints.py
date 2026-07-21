#!/usr/bin/env python3
"""Take named Flink savepoints through a private SPCS job service.

The controller job uses the released Flink image and calls the private REST
endpoint from inside Snowpark Container Services. It never publishes Flink's
administrative endpoint and never cancels either streaming job.
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import sys
import uuid


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from infra.snowflake import deploy  # noqa: E402
from pipeline.warehouse import get_warehouse  # noqa: E402


DATABASE = "POKER_ML_DEMO"
SCHEMA = "SPCS"
POOL = "POKER_ML_CPU_POOL"
SERVICE = "POKER_FLINK"
CONTAINER = "savepoint-controller"
TARGET_DIRECTORY = "file:///opt/flink/state/savepoints"
JOBS = {
    "context": "poker-event-time-context-enrichment-v1",
    "pair": "poker-pair-features-v1",
}
_RESULT = re.compile(
    r"^POKER_FLINK_SAVEPOINT_RESULT\|(?P<label>context|pair)\|"
    r"(?P<job_id>[0-9a-f]{32})\|(?P<location>file:/+\S+)$",
    re.MULTILINE,
)


def _snowflake_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    normalized = str(value).strip().lower()
    if normalized in {"true", "1"}:
        return True
    if normalized in {"false", "0"}:
        return False
    raise ValueError(f"unexpected Snowflake boolean value: {value!r}")


def _controller_script(service_dns: str, timeout_seconds: int) -> str:
    jobs = " ".join(f"{label}={name}" for label, name in JOBS.items())
    return f"""set -euo pipefail
base_url=http://{service_dns}:8081
target_directory={TARGET_DIRECTORY}
deadline=$((SECONDS + {timeout_seconds}))

until curl --silent --show-error --fail "${{base_url}}/overview" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "Flink REST did not become ready before timeout" >&2
    exit 1
  fi
  sleep 2
done

find_job_id() {{
  local expected_name="$1"
  local overview compact matches count
  overview="$(curl --silent --show-error --fail "${{base_url}}/jobs/overview")"
  compact="$(tr -d '[:space:]' <<<"${{overview}}")"
  matches="$(sed 's/}},{{/}}\\n{{/g' <<<"${{compact}}" \
    | grep -F "${{expected_name}}" \
    | grep -F '\"state\":\"RUNNING\"' \
    | grep -oE '\"jid\":\"[0-9a-f]{{32}}\"' \
    | grep -oE '[0-9a-f]{{32}}' || true)"
  count="$(wc -w <<<"${{matches}}" | tr -d ' ')"
  if [[ "${{count}}" != "1" ]]; then
    echo "Expected exactly one running Flink job named ${{expected_name}}" >&2
    echo "${{overview}}" >&2
    exit 1
  fi
  printf '%s\\n' "${{matches}}"
}}

take_savepoint() {{
  local label="$1" job_id="$2" request trigger_id poll status location
  request="$(curl --silent --show-error --fail -X POST \
    -H 'Content-Type: application/json' \
    --data '{{"target-directory":"'"${{target_directory}}"'","cancel-job":false}}' \
    "${{base_url}}/jobs/${{job_id}}/savepoints")"
  trigger_id="$(grep -oE '\"request-id\":\"[^\"]+\"' <<<"${{request}}" \
    | head -n 1 | cut -d'\"' -f4)"
  if [[ -z "${{trigger_id}}" ]]; then
    echo "Flink did not return a savepoint trigger ID: ${{request}}" >&2
    exit 1
  fi

  while (( SECONDS < deadline )); do
    poll="$(curl --silent --show-error --fail \
      "${{base_url}}/jobs/${{job_id}}/savepoints/${{trigger_id}}")"
    status="$(grep -oE '\"id\":\"(IN_PROGRESS|COMPLETED)\"' <<<"${{poll}}" \
      | head -n 1 | cut -d'\"' -f4 || true)"
    if [[ "${{status}}" == "COMPLETED" ]]; then
      location="$(grep -oE '\"location\":\"file:/+[^\"]+\"' <<<"${{poll}}" \
        | head -n 1 | cut -d'\"' -f4 || true)"
      if [[ -z "${{location}}" ]]; then
        echo "Savepoint operation completed without a location: ${{poll}}" >&2
        exit 1
      fi
      echo "POKER_FLINK_SAVEPOINT_RESULT|${{label}}|${{job_id}}|${{location}}"
      return
    fi
    sleep 2
  done
  echo "Timed out waiting for savepoint of ${{job_id}}" >&2
  exit 1
}}

for item in {jobs}; do
  label="${{item%%=*}}"
  job_name="${{item#*=}}"
  job_id="$(find_job_id "${{job_name}}")"
  take_savepoint "${{label}}" "${{job_id}}"
done
"""


def _job_spec(image_path: str, service_dns: str, timeout_seconds: int) -> str:
    if not deploy._IMAGE.fullmatch(image_path):
        raise ValueError("Flink image path must be a Snowflake repository image tag")
    if not re.fullmatch(r"[a-z0-9-]+\.[a-z0-9.-]+\.svc\.spcs\.internal", service_dns):
        raise ValueError("Unexpected Snowflake service DNS name")
    if timeout_seconds < 30 or timeout_seconds > 900:
        raise ValueError("timeout must be between 30 and 900 seconds")
    return json.dumps(
        {
            "spec": {
                "containers": [
                    {
                        "name": CONTAINER,
                        "image": image_path,
                        "command": ["/bin/bash"],
                        "args": [
                            "-lc",
                            _controller_script(service_dns, timeout_seconds),
                        ],
                        "resources": {
                            "requests": {"cpu": 0.1, "memory": "128Mi"},
                            "limits": {"cpu": 0.5, "memory": "512Mi"},
                        },
                    }
                ],
                "logExporters": {"eventTableConfig": {"logLevel": "INFO"}},
            }
        },
        separators=(",", ":"),
    )


def _parse_results(logs: str) -> dict[str, dict[str, str]]:
    results: dict[str, dict[str, str]] = {}
    for match in _RESULT.finditer(logs):
        label = match.group("label")
        if label in results:
            raise ValueError(f"duplicate savepoint result for {label}")
        results[label] = {
            "job_id": match.group("job_id"),
            "location": match.group("location"),
        }
    missing = sorted(set(JOBS) - set(results))
    if missing:
        raise ValueError(f"missing savepoint result(s): {', '.join(missing)}")
    return results


def take_savepoints(image_path: str, timeout_seconds: int) -> dict[str, object]:
    warehouse = get_warehouse()
    job_name = (
        "POKER_FLINK_SAVEPOINT_"
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_")
        + uuid.uuid4().hex[:6].upper()
    )
    try:
        warehouse.execute("USE ROLE SYSADMIN")
        warehouse.execute(f"USE DATABASE {DATABASE}")
        warehouse.execute(f"USE SCHEMA {SCHEMA}")
        services = warehouse.fetch_df(
            f"SHOW SERVICES LIKE '{SERVICE}' IN SCHEMA {DATABASE}.{SCHEMA}"
        )
        if len(services) != 1:
            raise RuntimeError(f"expected one {SERVICE} service, found {len(services)}")
        service = services.iloc[0]
        if service["status"] != "RUNNING" or service["owner"] != "SYSADMIN":
            raise RuntimeError(
                f"{SERVICE} must be RUNNING and owned by SYSADMIN; "
                f"got status={service['status']} owner={service['owner']}"
            )
        endpoints = warehouse.fetch_df(
            f"SHOW ENDPOINTS IN SERVICE {DATABASE}.{SCHEMA}.{SERVICE}"
        )
        rest = endpoints[endpoints["name"] == "flink-rest"]
        if (
            len(rest) != 1
            or _snowflake_bool(rest.iloc[0]["is_public"])
            or int(rest.iloc[0]["port"]) != 8081
        ):
            raise RuntimeError("flink-rest must be one private endpoint on port 8081")

        spec = _job_spec(image_path, str(service["dns_name"]), timeout_seconds)
        execution_error: Exception | None = None
        try:
            warehouse.execute(
                f"EXECUTE JOB SERVICE IN COMPUTE POOL {POOL} NAME = {job_name} "
                "ASYNC = FALSE "
                f"FROM SPECIFICATION $${spec}$$"
            )
        except Exception as error:  # Preserve SPCS logs for failed job containers.
            execution_error = error
        containers = warehouse.fetch_df(
            f"SHOW SERVICE CONTAINERS IN SERVICE {DATABASE}.{SCHEMA}.{job_name}"
        )
        logs_frame = warehouse.fetch_df(
            "SELECT SYSTEM$GET_SERVICE_LOGS("
            f"'{DATABASE}.{SCHEMA}.{job_name}', 0, '{CONTAINER}', 1000) AS logs"
        )
        logs = str(logs_frame.iloc[0]["logs"])
        if execution_error is not None:
            raise RuntimeError(
                f"savepoint controller execution failed: {execution_error}; logs:\n{logs}"
            ) from execution_error
        if len(containers) != 1 or containers.iloc[0]["status"] != "DONE":
            status = containers.to_dict(orient="records")
            raise RuntimeError(
                f"savepoint controller did not finish successfully: {status}; "
                f"logs:\n{logs}"
            )
        return {
            "controller_job": f"{DATABASE}.{SCHEMA}.{job_name}",
            "flink_service": f"{DATABASE}.{SCHEMA}.{SERVICE}",
            "flink_dns": str(service["dns_name"]),
            "image": image_path,
            "savepoints": _parse_results(logs),
        }
    finally:
        warehouse.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--image-path",
        default=os.environ.get("SPCS_FLINK_IMAGE_PATH"),
        required=os.environ.get("SPCS_FLINK_IMAGE_PATH") is None,
        help="Released Snowflake image path used by the controller job",
    )
    parser.add_argument("--timeout-seconds", type=int, default=300)
    args = parser.parse_args()
    try:
        result = take_savepoints(args.image_path, args.timeout_seconds)
    except (RuntimeError, ValueError) as error:
        raise SystemExit(f"[flink-savepoint] failed: {error}") from error
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
