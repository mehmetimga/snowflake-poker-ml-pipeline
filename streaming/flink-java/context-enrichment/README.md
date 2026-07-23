# Active-player user-context enrichment

This Flink 1.19.1 / Java 17 job consumes canonical hands, expands each hand to
one record per player, and lazily looks up only active players in PostgreSQL.
The result is retained in Flink keyed state with a sliding inactivity TTL.

The previous Kafka context-stream join remains available temporarily through
the separate `LegacyKafkaTemporalContextJob` entrypoint. The canonical
`ActiveContextEnrichmentJob` requires `FLINK_CONTEXT_SOURCE=jdbc`.

For a beginner-oriented explanation of streams, keyed state, event time,
watermarks, timers, both Java jobs, and the downstream model vector, read
[How the Flink real-time feature pipeline works](../../../docs/flink-realtime-feature-pipeline.md).

## JDBC lookup policy

- No full user table or daily batch is loaded.
- The first hand for A–F produces lookups only for A–F.
- State and PostgreSQL lookups are keyed by
  `(tenant_id, product_id, player_id)`; reads and writes extend the 36-hour
  state TTL.
- Cache entries are typed Flink POJOs in
  `active-user-context-cache-v1`; state schema version 1 is validated whenever
  an entry is used.
- A separate 60-minute freshness interval forces periodic refresh for active
  players.
- The SQL lookup selects the latest version whose
  `effective_at <= played_at`.
- A late hand may resolve an older effective row for that event, but it cannot
  replace a newer row already cached for subsequent hands.
- Every cache-miss lookup validates its subtask-local connection. A closed or
  invalid connection is replaced before the prepared query executes.
- PostgreSQL connect, socket, prepared-query, and validation timeouts are
  bounded. A transient SQLSTATE receives exactly one retry with at most
  100 ms jitter by default; authentication, schema, and data errors never
  retry.
- If the retry fails, the operator throws a sanitized exception. The
  canonical job uses a failure-rate restart policy instead of advancing Kafka
  work or converting an infrastructure outage into a data-quality DLQ.
- Missing rows produce a minimized data-quality diagnostic. Transient,
  authentication, schema, and other JDBC failures fail the operator with a
  sanitized category so Flink can restart from its checkpoint rather than
  silently advancing past unscored work.
- Diagnostics retain safe lineage and a SHA-256 digest but never copy the raw
  hand, hole cards, or database error message.
- Stable operator UIDs and UUIDv5 output IDs keep downstream upserts safe.

The canonical output contract is `poker.hand-player-context.enriched` schema
v2 on `poker.hand-player-context.v2`, keyed by player ID. Its
`context_resolution` object identifies PostgreSQL point-in-time resolution,
the policy version, context record ID, context version, and effective time.
Cache hit/miss and lookup latency are runtime metrics rather than event
fields, so replaying the same source data produces the same event bytes.

## Code boundaries

The canonical path is separated by responsibility:

| Package | Responsibility |
|---|---|
| `app` | Canonical and rollback-only process entrypoints |
| `config` | Argument/environment parsing and validation |
| `domain` | Flink-independent context identity, projection, and typed cache records |
| `port` | Point-in-time context repository interface |
| `adapter.jdbc` | PostgreSQL, credentials, and sanitized failure mapping |
| `contract` | Enriched-event JSON contract |
| `flink` | Typed state declaration, active-player keyed operator, and metrics |

The root package contains shared Kafka/topology plumbing and the temporary
legacy temporal-join implementation. Domain and port code does not depend on
Flink or PostgreSQL.

## Build and test

```bash
make enrichment-topics
make flink-context-test
make flink-context-build
```

The deployable JAR is
`target/context-enrichment-0.1.0.jar`. Maven and the job must use Java 17.

## Submit

Export the Kafka settings so the Flink client can place them in both source
and sink configurations, then submit the JAR:

```bash
set -a
source .env
set +a

$FLINK_HOME/bin/flink run \
  -c com.aicampions.poker.context.app.ActiveContextEnrichmentJob \
  streaming/flink-java/context-enrichment/target/context-enrichment-0.1.0.jar \
  --context-source jdbc \
  --group-id flink-active-context-v2 \
  --parallelism 2
```

Use `--from-beginning --bounded` for a deterministic finite audit. The
canonical job snapshots the hand topic's end offsets when it starts, drains
that input, and exits.

Important options:

| Option | Default |
|---|---:|
| `--context-source` | required `jdbc` for the canonical entrypoint |
| `--context-jdbc-table` | `public.poker_user_context` |
| `--context-jdbc-query-timeout-seconds` | `1` |
| `--context-jdbc-connect-timeout-seconds` | `3` |
| `--context-jdbc-validation-timeout-seconds` | `1` |
| `--context-jdbc-retry-max-jitter-ms` | `100`, maximum `5000` |
| `--context-cache-ttl-hours` | `36` |
| `--context-refresh-minutes` | `60` |
| `--allowed-lateness-ms` | `30000` |
| `--correction-window-ms` | `300000` |
| `--idle-source-timeout-ms` | `60000` |
| `--state-ttl-hours` | `720` |
| `--context-bootstrap-wait-ms` | `30000` live, `0` bounded |
| `--checkpoint-interval-ms` | `30000` |
| `--restart-max-failures-per-interval` | `3` |
| `--restart-failure-rate-interval-ms` | `600000` |
| `--restart-delay-ms` | `10000` |
| `--parallelism` | `1` |

JDBC mode requires `USER_CONTEXT_JDBC_URL` in job configuration. The
TaskManager also requires `USER_CONTEXT_DB_USER` and
`USER_CONTEXT_DB_PASSWORD` from its runtime Secret. Username/password command
line arguments are rejected, and neither credential is stored in the
serializable job configuration or process-function fields.

## Connections and metrics

Each active-context operator subtask owns one PostgreSQL connection; the pair
feature job owns none. Therefore:

```text
steady JDBC connections =
  active-context parallelism × concurrently running context jobs
```

A reconnect closes the old resource before opening its replacement, so it
does not intentionally double the steady connection count. During a
blue/green or shadow comparison, include every concurrently running context
job in the budget. Reserve separate PostgreSQL headroom for administration,
migrations, monitoring, and brief TCP cleanup; do not size the database
exactly to the Flink formula.

For example, parallelism 4 uses four steady connections. Running old and new
context jobs together uses eight. A practical POC allocation is those eight
connections plus at least five non-Flink connections of operational
headroom. Production must use the actual database connection limit and other
application workloads.

The TaskManager exports cache hit/miss/refresh, lookup found/not-found,
retry, reconnect, final failure category, and latest lookup-latency metrics.
Flink's standard busy-time, backpressure, checkpoint, restart, and Kafka-lag
metrics complete the operational view.

The rollback-only temporal join must be submitted explicitly:

```bash
$FLINK_HOME/bin/flink run \
  -c com.aicampions.poker.context.app.LegacyKafkaTemporalContextJob \
  streaming/flink-java/context-enrichment/target/context-enrichment-0.1.0.jar \
  --context-source kafka \
  --group-id flink-legacy-kafka-context-v1
```

It retains the schema-v1 topic and has a separate consumer group, job name,
and operator UID namespace. Do not restore its savepoint into the canonical
JDBC topology.

The tenant/product migration is
`infra/simulation/postgres/init/004_scope_user_context.sql`. It is forward-only
and can be safely reapplied to the local POC database.

For production, configure durable checkpoint storage in the Flink cluster and
start from a savepoint when upgrading stateful operator code.

## State recovery and upgrades

The canonical lookup operator keeps its stable UID
`active-context-v2-jdbc-lookup`. The typed cache uses the separate state name
`active-user-context-cache-v1`, schema version
`ActiveContextCacheEntry.STATE_SCHEMA_VERSION = 1`, and Flink's POJO
serializer. The local compose profile mounts one shared
`flink_state` volume into the JobManager and TaskManager and initializes
`/opt/flink/state/checkpoints` and `/opt/flink/state/savepoints`.

For compatible typed-state evolution, take a savepoint, retain the previous
JAR, preserve operator UIDs and the POJO class name, and follow Flink's POJO
rules: fields may be added or removed, but existing field types and the keyed
`ContextKey` shape must not change. Increment the application schema version
and test restore from a retained fixture.

The F3 JSON state is a derived cache. F4 deliberately does not reuse its state
name; after restoring source/operator state, the typed cache is populated
lazily from PostgreSQL as active hands arrive. Do not use
`--allowNonRestoredState` for a normal canonical restore. A legacy Kafka-join
savepoint must fail because its UID namespace is intentionally incompatible.

See the F4 evidence and upgrade checklist in
[the active-user context refactoring plan](../../../docs/active-user-context-refactoring-plan.md).
