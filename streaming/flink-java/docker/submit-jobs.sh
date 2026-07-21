#!/usr/bin/env bash
set -euo pipefail

rest_url="${FLINK_REST_URL:-http://127.0.0.1:8081}"
startup_timeout_seconds="${FLINK_STARTUP_TIMEOUT_SECONDS:-180}"
poll_seconds="${FLINK_HEALTH_POLL_SECONDS:-30}"
context_jar="/opt/flink/usrlib/context-enrichment.jar"
pair_jar="/opt/flink/usrlib/pair-features.jar"
context_name="poker-event-time-context-enrichment-v1"
pair_name="poker-pair-features-v1"

deadline=$((SECONDS + startup_timeout_seconds))
until curl --silent --show-error --fail "${rest_url}/overview" >/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "Flink REST endpoint did not become ready within ${startup_timeout_seconds}s" >&2
    exit 1
  fi
  sleep 2
done

running_jobs() {
  /opt/flink/bin/flink list -r 2>/dev/null || true
}

submit_job() {
  local job_name="$1"
  local jar_path="$2"
  local savepoint_path="$3"
  local current
  current="$(running_jobs)"
  if grep -Fq "${job_name}" <<<"${current}"; then
    echo "Flink job already running: ${job_name}"
    return
  fi

  local restore_args=()
  if [[ -n "${savepoint_path}" ]]; then
    restore_args=(-s "${savepoint_path}")
  fi
  /opt/flink/bin/flink run -d -m 127.0.0.1:8081 \
    "${restore_args[@]}" "${jar_path}"
  echo "Submitted Flink job: ${job_name}"
}

submit_job "${context_name}" "${context_jar}" "${FLINK_CONTEXT_SAVEPOINT_PATH:-}"
submit_job "${pair_name}" "${pair_jar}" "${FLINK_PAIR_SAVEPOINT_PATH:-}"

while true; do
  current="$(running_jobs)"
  if ! grep -Fq "${context_name}" <<<"${current}"; then
    echo "Required Flink job is not running: ${context_name}" >&2
    exit 1
  fi
  if ! grep -Fq "${pair_name}" <<<"${current}"; then
    echo "Required Flink job is not running: ${pair_name}" >&2
    exit 1
  fi
  sleep "${poll_seconds}"
done
