#!/usr/bin/env bash
set -euo pipefail

test -s /opt/flink/usrlib/context-enrichment.jar
test -s /opt/flink/usrlib/pair-features.jar
test -s /opt/flink/lib/flink-metrics-prometheus-1.19.1.jar
test -s /opt/flink/lib/flink-statebackend-rocksdb-1.19.1.jar
test -s /opt/context-proxy/server.py
bash -n /opt/flink/bin/submit-poker-jobs
/usr/local/bin/python -m py_compile /opt/context-proxy/server.py
/usr/local/bin/python -c \
  'import snowflake.connector; assert tuple(map(int, snowflake.connector.VERSION[:2])) >= (4, 5)'

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

verify_jar_class() {
  local jar_path="$1"
  local class_path="$2"
  # The pinned Flink runtime contains a Java runtime, not the JDK `jar`
  # command. ZIP entry names are stored verbatim in the JAR central directory,
  # so fixed-string binary grep provides a dependency-free packaging check.
  if ! grep -aFq "${class_path}" "${jar_path}"; then
    echo "Missing expected JAR class: ${class_path}" >&2
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

for class_path in \
  com/aicampions/poker/context/config/ContextJobConfig.class \
  com/aicampions/poker/context/contract/JdbcEnrichedEventV2.class \
  com/aicampions/poker/context/domain/ActiveContextCacheEntry.class \
  com/aicampions/poker/context/domain/ContextKey.class \
  com/aicampions/poker/context/domain/UserContextRecord.class \
  com/aicampions/poker/context/port/UserContextRepository.class \
  com/aicampions/poker/context/adapter/jdbc/JdbcConnectionFactory.class \
  com/aicampions/poker/context/adapter/jdbc/JdbcRepositoryObserver.class \
  com/aicampions/poker/context/adapter/jdbc/JdbcRetryDelay.class \
  com/aicampions/poker/context/adapter/jdbc/JdbcUserContextRepository.class \
  com/aicampions/poker/context/adapter/snowflake/SnowflakeContextProxyRepository.class \
  com/aicampions/poker/context/flink/ActiveContextState.class \
  com/aicampions/poker/context/flink/JdbcContextEnrichmentFunction.class; do
  verify_jar_class /opt/flink/usrlib/context-enrichment.jar "${class_path}"
done

echo "Flink image contract passed"
