# Pair expansion and rolling features

This Java 17 / Flink 1.19.1 job consumes the canonical
`poker.hand-player-context.v2`, computes prior-only user history, reassembles
complete hands, expands a six-player hand into 15 unordered pairs, and writes
feature-contract v1 snapshots to `poker.pair-features.context-v2.v1` keyed by
`player_a:player_b`.
It also evaluates the first checkpointed Rules v2 window and embeds any fired
`poker.rule-evidence.v1` event in `upstream_rule_evidence`. The Go scorer later
publishes that evidence with its score and alert. The job performs no
synchronous database reads.

For a beginner-oriented explanation of streams, keyed state, event time,
watermarks, prior-only features, and the downstream `[15, 58]` model tensor,
read [How the Flink real-time feature pipeline works](../../../docs/flink-realtime-feature-pipeline.md).

## Feature-contract v1 semantics

- `player_a < player_b` in lexical order and the Kafka key is
  `player_a:player_b`.
- Current-hand features include position, actions, invested amounts, outcomes,
  street participation, and fold/win interaction.
- Context crosses include skill and account-age differences plus country,
  timezone, acquisition, device, network, bankroll, and stake relationships.
- User and pair rolling features are snapshots strictly before the current
  hand. The current hand updates state only after its snapshot is captured.
- Duplicate deliveries do not update state.
- A corrected player-context row changes only the five affected pair snapshots,
  increments `snapshot_revision`, and reuses the original prior-history state.
- Event IDs are UUIDv5 values derived from the source hand, both enriched source
  events, pair key, and `pair-features-v1` definition version.
- All feature floats are rounded to nine decimal places in both Java and Python
  to make cross-language parity deterministic.
- State has a configurable processing-time TTL. Stable operator UIDs preserve
  savepoint compatibility within the v1 topology.
- `pair.repeated-fold-to-partner-wins:v1` uses a scoped pair key and a rolling
  24-hour event-time window. It requires at least five hands, three fold/win
  observations in one direction, and a directional rate of at least `0.6`.
- Stateful rule evidence is transport metadata and is not added to the 58-value
  CatBoost feature vector or blended into probability.

The corresponding offline oracle is
`pipeline/features/pair_features.py`. `scripts/check_pair_features.py` validates
the Kafka contract and can compare every online payload with that oracle.

## Build and test

```bash
make enrichment-topics
make flink-pair-features-test
make flink-pair-features-build
```

The deployable JAR is `target/pair-features-0.1.0.jar`.

## Submit

```bash
set -a
source .env
set +a

$FLINK_HOME/bin/flink run \
  -c com.aicampions.poker.features.PairFeaturesJob \
  streaming/flink-java/pair-features/target/pair-features-0.1.0.jar \
  --input-schema-version 2 \
  --group-id flink-pair-features-context-v2 \
  --parallelism 2
```

Schema v2 is the default. Use `--input-schema-version 1` only for an explicit
rollback audit; v1 and v2 use separate input/output topics, consumer groups,
job names, and operator UID namespaces.

Use `--from-beginning --bounded` for a finite audit. The disorder allowance
must cover the maximum cross-partition skew of the replay. The current
20-hand Confluent audit uses `--out-of-orderness-ms 300000`; the live default is
30 seconds.

Important options:

| Option | Default |
|---|---:|
| `--input-schema-version` | `2` |
| `--out-of-orderness-ms` | `30000` |
| `--idle-source-timeout-ms` | `60000` |
| `--state-ttl-hours` | `720` |
| `--stateful-rule-window-hours` | `24` |
| `--stateful-rule-minimum-hands` | `5` |
| `--stateful-rule-minimum-directional-count` | `3` |
| `--stateful-rule-rate-threshold` | `0.6` |
| `--stateful-rule-allowed-lateness-ms` | `120000` |
| `--stateful-rule-correction-horizon-hours` | `48` |
| `--stateful-rule-state-ttl-hours` | `72` |
| `--checkpoint-interval-ms` | `30000` |
| `--parallelism` | `1` |

After a bounded run:

```bash
python scripts/check_pair_features.py \
  --input-topic poker.hand-player-context.v2 \
  --topic poker.pair-features.context-v2.v1 \
  --minimum-records 300

python scripts/ingest_pair_features.py \
  --migrate --from-beginning --max-messages 300
```

Production deployment still needs durable checkpoint storage and a savepoint
restore drill before this topology is promoted beyond the test stream.

Flink metrics added by the stateful rule include evaluation, firing,
duplicate, correction, stale, and late counters plus state-size and event-time
lag gauges. Flink's standard checkpoint duration/failure metrics remain the
source for checkpoint health.
