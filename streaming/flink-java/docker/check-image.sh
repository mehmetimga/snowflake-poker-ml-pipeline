#!/usr/bin/env bash
set -euo pipefail

test -s /opt/flink/usrlib/context-enrichment.jar
test -s /opt/flink/usrlib/pair-features.jar
test -s /opt/flink/lib/flink-metrics-prometheus-1.19.1.jar
test -s /opt/flink/lib/flink-statebackend-rocksdb-1.19.1.jar
bash -n /opt/flink/bin/submit-poker-jobs

verify_main_class() {
  local jar_path="$1"
  local main_class="$2"
  local output
  local status
  set +e
  output="$(java -cp "/opt/flink/lib/*:${jar_path}" "${main_class}" unexpected 2>&1)"
  status=$?
  set -e
  if [[ ${status} -eq 0 ]] || [[ "${output}" != *"unexpected argument: unexpected"* ]]; then
    echo "Unable to load expected main class: ${main_class}" >&2
    echo "${output}" >&2
    exit 1
  fi
}

verify_main_class \
  /opt/flink/usrlib/context-enrichment.jar \
  com.aicampions.poker.context.app.ActiveContextEnrichmentJob
verify_main_class \
  /opt/flink/usrlib/context-enrichment.jar \
  com.aicampions.poker.context.app.LegacyKafkaTemporalContextJob
verify_main_class \
  /opt/flink/usrlib/pair-features.jar \
  com.aicampions.poker.features.PairFeaturesJob

echo "Flink image contract passed"
