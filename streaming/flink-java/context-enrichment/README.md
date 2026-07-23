# Active-player user-context enrichment

This Flink 1.19.1 / Java 17 job consumes canonical hands, expands each hand to
one record per player, and lazily looks up only active players in PostgreSQL.
The result is retained in Flink keyed state with a sliding inactivity TTL.

The previous Kafka context-stream join remains available temporarily through
`FLINK_CONTEXT_SOURCE=kafka` for rollback. The canonical target is
`FLINK_CONTEXT_SOURCE=jdbc`.

For a beginner-oriented explanation of streams, keyed state, event time,
watermarks, timers, both Java jobs, and the downstream model vector, read
[How the Flink real-time feature pipeline works](../../../docs/flink-realtime-feature-pipeline.md).

## JDBC lookup policy

- No full user table or daily batch is loaded.
- The first hand for A–F produces lookups only for A–F.
- State is keyed by `player_id`; reads and writes extend its 36-hour TTL.
- A separate 60-minute freshness interval forces periodic refresh for active
  players.
- The SQL lookup selects the latest version whose
  `effective_at <= played_at`.
- Missing rows and database failures go to
  `poker.pipeline.dead-letter.v1`; incomplete context is never fabricated.
- Stable operator UIDs and UUIDv5 output IDs keep downstream upserts safe.

The output contract is `poker.hand-player-context.enriched` schema v1 on
`poker.hand-player-context.v1`, keyed by player ID.

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
  -c com.aicampions.poker.context.ContextEnrichmentJob \
  streaming/flink-java/context-enrichment/target/context-enrichment-0.1.0.jar \
  --group-id flink-context-enrichment-v1 \
  --parallelism 2
```

Use `--from-beginning --bounded` for a deterministic finite audit. Bounded
mode snapshots each topic's end offsets when the job starts, drains both
inputs, advances the final watermark, and exits.

Important options:

| Option | Default |
|---|---:|
| `--context-source` | `kafka` during migration; target `jdbc` |
| `--context-jdbc-table` | `public.poker_user_context` |
| `--context-jdbc-query-timeout-seconds` | `1` |
| `--context-cache-ttl-hours` | `36` |
| `--context-refresh-minutes` | `60` |
| `--allowed-lateness-ms` | `30000` |
| `--correction-window-ms` | `300000` |
| `--idle-source-timeout-ms` | `60000` |
| `--state-ttl-hours` | `720` |
| `--context-bootstrap-wait-ms` | `30000` live, `0` bounded |
| `--checkpoint-interval-ms` | `30000` |
| `--parallelism` | `1` |

JDBC mode also requires `USER_CONTEXT_JDBC_URL`,
`USER_CONTEXT_DB_USER`, and `USER_CONTEXT_DB_PASSWORD`. These values must come
from runtime secrets and are excluded from the safe configuration summary.

For production, configure durable checkpoint storage in the Flink cluster and
start from a savepoint when upgrading stateful operator code.
