# Event-time user-context enrichment

This Flink 1.19.1 / Java 17 job consumes canonical hand and user-context
envelopes, expands each hand to one record per player, and joins each row to
the latest context version whose `effective_at <= played_at`. It never queries
Snowflake or PostgreSQL in the hot path.

For a beginner-oriented explanation of streams, keyed state, event time,
watermarks, timers, both Java jobs, and the downstream model vector, read
[How the Flink real-time feature pipeline works](../../../docs/flink-realtime-feature-pipeline.md).

## Join policy

- Kafka streams are repartitioned by `player_id` / `user_id` into keyed state.
- A hand waits 30 seconds of event time by default.
- The initial status is `matched`, `matched_late`, or `missing`.
- A newly arriving effective context can emit `corrected` with a higher
  revision for five minutes after the initial deadline.
- Duplicate hand and context event IDs are ignored.
- A conflicting context version is sent to `poker.pipeline.dead-letter.v1`.
- Context state has a configurable processing-time safety TTL (30 days by
  default); event-time timers remove completed hand state earlier.
- On cold start/recovery, live mode holds initial hand timers for 30 seconds so
  the compacted context topic can bootstrap before an idle input is ignored.
  A processing-time fallback releases only hands seen during that bootstrap
  interval; steady-state records remain event-time driven.
- Stable operator UIDs and UUIDv5 output IDs make checkpoint recovery and
  downstream upserts safe.

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
| `--allowed-lateness-ms` | `30000` |
| `--correction-window-ms` | `300000` |
| `--idle-source-timeout-ms` | `60000` |
| `--state-ttl-hours` | `720` |
| `--context-bootstrap-wait-ms` | `30000` live, `0` bounded |
| `--checkpoint-interval-ms` | `30000` |
| `--parallelism` | `1` |

For production, configure durable checkpoint storage in the Flink cluster and
start from a savepoint when upgrading stateful operator code.
