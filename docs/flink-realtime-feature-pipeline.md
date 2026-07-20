# How the Flink real-time feature pipeline works

This guide explains the Java/Flink part of the poker ML pipeline from first
principles. It is written for a reader who is new to Flink, but it stays tied
to the code in this repository.

The short version is:

> Flink continuously reads Kafka events, remembers the minimum history needed
> for each user and player pair, joins every hand to the context that was valid
> when the hand was played, and publishes model-ready feature records. Flink
> does **not** run the ML model. The Go risk scorer converts those records into
> tensors and performs inference.

For the exact ordered model columns, keep the
[real-time model input contract](realtime-model-input-contract.md) open beside
this guide.

## 1. Where Flink sits in the complete system

```text
Today: PokerKit generator
Future: poker server -> PostgreSQL hand history -> Debezium CDC
                              |
                              v
                    Confluent Cloud Kafka
                              |
                 +------------+-------------+
                 |                          |
                 v                          v
       poker.hands.raw.v1       poker.user-context.v1
                 |                          |
                 +------------+-------------+
                              |
                              v
              Java/Flink job 1: context enrichment
                  - validate and deduplicate
                  - expand one hand to six players
                  - event-time context join
                              |
                              v
             poker.hand-player-context.v1
                              |
                              v
              Java/Flink job 2: pair features
                  - prior user history
                  - reassemble the six-player hand
                  - expand it to 15 pairs
                  - prior pair history
                              |
                              v
                 poker.pair-features.v1
                              |
                              v
                    Go risk scorer
                  - assemble 15 pair rows
                  - make a [15, 58] tensor
                  - run CatBoost ONNX
                  - calibrate and aggregate
                              |
                 +------------+-------------+
                 v                          v
       poker.risk-scores.v1       poker.risk-alerts.v1
                 |                          |
                 +------------+-------------+
                              |
                              v
                     Snowflake tables
                  and the admin application
```

Kafka is the boundary between these components. The Flink jobs do not perform
synchronous reads from PostgreSQL or Snowflake. That keeps replay deterministic
and prevents a slow database query from stopping the real-time path.

### Current deployment versus the production target

These are two different facts and should not be confused:

- The Java/Flink jobs are implemented and can be built and tested locally.
- The current SPCS deployment still uses the original Python application
  image. Java/Flink is not yet packaged and deployed there.
- The production target is a versioned `poker-flink:<git-sha>` image running as
  the long-lived `POKER_FLINK` Snowpark Container Services service.
- The Go scorer will be a separate `poker-risk:<git-sha>` SPCS image. A small
  ONNX model should run inside Go; Triton is optional and not part of Flink.
- Local machines should generate or replay test input. Production stream
  processing belongs in SPCS, not on a developer laptop.

See the [Snowflake container deployment guide](../infra/snowflake/README.md)
for the current-versus-target deployment status.

## 2. The Flink ideas needed to understand this code

You do not need all of Flink to follow this project. Seven ideas cover most of
the implementation.

### 2.1 A stream is an unbounded sequence of events

A Kafka topic normally never “finishes.” Flink therefore processes each event
as it arrives instead of loading a whole table and running one query. Operators
form a pipeline:

```text
source -> validate -> keyBy -> stateful operation -> sink
```

A source reads Kafka. An operator transforms or joins events. A sink writes
the result to another Kafka topic.

### 2.2 `keyBy` decides which events share memory

Flink state is normally *keyed state*. After `keyBy(user_id)`, all records for
one user reach the same logical state bucket. Another user has a different
bucket. Flink can redistribute these buckets when parallelism changes.

This project uses three important keys:

| Key | Why records need to meet |
|---|---|
| `user_id` / `player_id` | Join a player's hand row with that user's context and maintain user history |
| `hand_id` | Collect all six player rows before generating pairs |
| `pair_key` | Maintain history for one canonical unordered player pair |

The pair key always uses lexical order: `smaller_player_id:larger_player_id`.
This prevents `A:B` and `B:A` from becoming different relationships.

### 2.3 State is the operator's durable memory

Ordinary local variables are lost after a crash and are not distributed
safely. Flink's `ValueState` and `MapState` are managed by the runtime and are
included in checkpoints.

The state in this pipeline answers questions such as:

- What context versions have been seen for this user?
- Which hands has this user already played?
- Have all six rows for this hand arrived?
- What were this pair's statistics *before* the present hand?
- Has this exact event or revision already been processed?

State has a configurable 30-day safety TTL. The TTL prevents inactive keys
from remaining forever, while event-time cleanup removes short-lived pending
hand state sooner.

### 2.4 Event time is when poker happened

There are two clocks to distinguish:

- **Event time** comes from `occurred_at` / `played_at`. It says when the hand
  happened in the poker system.
- **Processing time** is the wall clock on the Flink worker. It says when Flink
  happened to receive the event.

Historical replay makes the difference obvious. A hand played last week can
arrive at Flink today. User context must be selected using last week's event
time, not today's processing time.

### 2.5 A watermark is Flink's estimate of event-time progress

A watermark roughly means: “this input has probably delivered everything up
to time T.” Event-time timers can fire when the watermark passes their time.

The pair-feature job allows 30 seconds of out-of-order arrival by default. An
idle Kafka partition is marked idle after 60 seconds so it cannot hold the
whole job's watermark back forever.

The context job expects monotonically advancing timestamps in each source
partition. Its 30-second hand wait is implemented by an event-time timer at
`played_at + 30 seconds`. This is a waiting policy; it is not a promise that
arbitrarily late events will be accepted.

For a two-input operator, progress is limited by the slower active input. That
is important here because hands and contexts come from different topics.

### 2.6 Timers let an operator act when no new event arrives

A keyed process function can register a timer. This project uses timers to:

- wait briefly for context that is late relative to a hand;
- emit an explicit `missing` result instead of waiting forever;
- retain a hand during the correction window;
- process pair observations in deterministic event-time order; and
- clean up state when it is no longer useful.

### 2.7 Checkpoints recover a running computation

A checkpoint records operator state together with Kafka source positions. If a
worker fails, Flink restores the state and resumes the stream from a consistent
point.

A savepoint is a deliberately retained state snapshot used for controlled
upgrades and migrations. Stable operator UIDs in this code allow Flink to map
saved state back to the corresponding operators after a redeployment.

The Kafka sinks in both Java jobs currently use `AT_LEAST_ONCE`. After a
failure, a record can therefore be delivered more than once. Deterministic
UUIDv5 event IDs, revision numbers, downstream upserts, and idempotent consumers
are part of the correctness design; this guide does not claim end-to-end
exactly-once delivery.

## 3. Kafka contracts used by the jobs

| Topic | Producer | Consumer | Record meaning |
|---|---|---|---|
| `poker.hands.raw.v1` | PokerKit now; Debezium adapter later | Context job | One complete poker hand |
| `poker.user-context.v1` | Context producer | Context job | One effective-dated user-context version |
| `poker.hand-player-context.v1` | Context job | Pair-feature job | One player in one hand with selected context |
| `poker.pair-features.v1` | Pair-feature job | Go scorer | One player pair in one hand with structured features |
| `poker.pipeline.dead-letter.v1` | Both Flink jobs | Operations/audit consumer | Invalid or conflicting input plus error metadata |

All public inference events must remain label-free. Fields such as `target`,
`is_collusive`, and `collusion_pair_id` are private training truth and are
rejected from the real-time path.

## 4. Job 1: event-time user-context enrichment

Entry point:
[ContextEnrichmentJob.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/ContextEnrichmentJob.java)

Its output question is:

> For this player in this hand, what user context was valid at the moment the
> hand was played?

### 4.1 Step-by-step flow

1. Read hands and user-context versions from their Kafka topics.
2. Validate the event envelope and reject forbidden/private fields.
3. Assign event timestamps from `occurred_at`.
4. Expand one six-player hand into six hand-player rows.
5. Key hand-player rows by `player_id`; key contexts by `user_id`.
6. Connect the two keyed streams.
7. Store context versions and pending hand-player rows in keyed state.
8. Select the latest context whose `effective_at <= played_at`.
9. Publish one enriched row per player, or send invalid input to the DLQ.

The two keyed IDs refer to the same logical user. Once keyed, Flink guarantees
that a user's hands and context versions are processed by the same join
operator instance.

### 4.2 The temporal-selection algorithm

The selection rule is deliberately simple and deterministic:

```text
candidates = contexts where effective_at <= hand.played_at
selected   = maximum candidate by
             (effective_at, context_version, context_event_id)
```

The event ID is the final tie-breaker so replay produces the same answer even
if equally dated records arrive in a different order. A future context is never
attached to an earlier hand.

Example:

```text
context v1 effective 09:00  skill=beginner
hand H42 played     10:00
context v2 effective 11:00  skill=advanced
```

`H42` receives v1. Even if v2 reaches Kafka first during a replay, its
`effective_at` is after the hand and it is not eligible.

The pure selection and output construction live in
[TemporalJoinLogic.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/TemporalJoinLogic.java).
The state and timers live in
[ContextTemporalJoinFunction.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/ContextTemporalJoinFunction.java).

### 4.3 Join statuses and corrections

| Status | Meaning |
|---|---|
| `matched` | An eligible context was present when the first output was emitted |
| `matched_late` | An eligible context arrived after the hand in stream order but before initial emission |
| `missing` | The event-time deadline passed without an eligible context |
| `corrected` | A better eligible context arrived within the five-minute correction window |

A correction does not silently overwrite history. It publishes a new
deterministic event with a higher revision and complete source lineage. The
downstream pair job understands revisions and changes only affected pairs.

Example timeline with default settings:

```text
10:00:00 hand H42 occurs
10:00:30 initial context deadline
10:00:30 emit missing if no eligible context exists
10:03:00 late context effective at 09:55 arrives
10:03:00 emit corrected revision 2
10:05:30 correction window closes and pending hand state can be removed
```

### 4.4 State held by the context job

| State | Type | Key | Purpose |
|---|---|---|---|
| context by event ID | `MapState` | user | Effective-dated context versions and deduplication |
| player hand by event ID | `MapState` | user | Pending/emitted hand-player rows and correction metadata |
| arrival sequence | `ValueState` | user | Determine whether a selected context arrived late in stream order |

Live startup includes a short processing-time bootstrap wait so a compacted
context topic can refill state before idle input is ignored. Steady-state join
semantics remain event-time based.

## 5. Job 2: pair expansion and rolling features

Entry point:
[PairFeaturesJob.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairFeaturesJob.java)

Its output question is:

> What was known about these two players before this hand, and what happened
> between them in the current hand?

The operator chain changes keys because each stage needs a different grouping:

```text
enriched player row
  -> keyBy(player_id) -> prior user history
  -> keyBy(hand_id)   -> collect six rows and create 15 pairs
  -> keyBy(pair_key)  -> prior pair history and final feature event
```

### 5.1 Prior-only user history

[UserRollingFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/UserRollingFunction.java)
maintains running user totals. The order of operations prevents target leakage:

```text
1. snapshot the user's aggregate before the current hand
2. freeze that snapshot for this hand ID
3. update the aggregate using the current hand
4. emit the row with the frozen prior snapshot
```

Suppose a user has played ten previous hands and H11 is a large win. Features
for H11 say `hands_seen = 10`; H11's result is not allowed to influence its own
history features. H12 may see the updated total of eleven hands.

The frozen per-hand snapshot also makes correction safe. If context for H11 is
corrected later, Flink reuses H11's original prior history instead of counting
H11 twice.

### 5.2 Reassemble one hand and generate pairs

[HandPairAssemblyFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/HandPairAssemblyFunction.java)
waits until all expected player rows for a hand have arrived. It validates that
their hand metadata agrees, sorts player IDs, and generates every unordered
combination.

The number of pairs is:

```text
n choose 2 = n * (n - 1) / 2
6 choose 2 = 6 * 5 / 2 = 15
```

If one player's enriched row is corrected, that player belongs to five of the
15 pairs. Only those five pair observations receive new snapshot revisions.

### 5.3 Prior-only pair history

[PairRollingFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairRollingFunction.java)
buffers observations by hand and revision. Event-time timers order due items by
`(played_at, hand_id, revision)`, making updates deterministic when partitions
deliver records out of order.

For a new hand it follows the same anti-leakage pattern as user history:

```text
1. snapshot the pair aggregate before the current hand
2. freeze that snapshot for the hand
3. update aggregate once with the current hand
4. publish current-hand + context + user-history + pair-history groups
```

If the same Kafka record is delivered again, or a correction arrives for the
same hand, the frozen snapshot is reused and the pair aggregate is not updated
again.

### 5.4 Streaming algorithms used in Flink

These are algorithms, but they are deterministic stream-processing algorithms,
not trained ML models:

| Algorithm | Location | Purpose |
|---|---|---|
| Envelope/schema validation | Both jobs | Reject malformed, incompatible, or label-bearing events |
| Event-ID deduplication | Keyed state | Make Kafka redelivery safe |
| Effective-dated temporal join | Context job | Select context valid at hand event time |
| Deadline plus correction window | Context timers | Bound waiting while allowing controlled revision |
| Incremental aggregates | User/pair rolling functions | Update counts, sums, and rates in O(1) per observation |
| Combination generation | Hand assembly | Turn six player rows into 15 canonical pairs |
| Deterministic event-time ordering | Pair timers | Make replay stable under cross-partition disorder |
| Prior-only snapshotting | User/pair state | Prevent a hand from leaking into its own input features |
| UUIDv5 identity | Output builders | Produce stable IDs for replay and downstream upsert |

Feature formulas and nine-decimal cross-language rounding live in
[PairFeatureMath.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairFeatureMath.java).

## 6. What feature record does Flink create?

Flink publishes a *structured JSON feature event*, not a tensor. Its feature
payload has five groups:

| Group | Examples | Question answered |
|---|---|---|
| `current_hand` | positions, invested amounts, action counts, outcomes | What happened now? |
| `context` | skill gap, account age, same country/device/network, bankroll and stake distance | Who are the users and how similar is their environment? |
| `user_history_a` | hands seen, mean won, fold/raise/flop rates | What was player A's prior behavior? |
| `user_history_b` | same features for B | What was player B's prior behavior? |
| `pair_history` | hands together, outcome asymmetry, fold/win rates, shared flop/table rates, last-seen age | What was this relationship's prior behavior? |

It also carries non-feature identity and audit fields such as hand ID, pair key,
context versions, source event IDs, revision, and feature-definition version.
Those fields are essential for lineage but are not automatically model inputs.

This separation is useful: operators and analysts can inspect meaningful JSON,
while a versioned preprocessing contract controls the exact numeric order used
by a model.

## 7. How structured features become a model vector

The conversion happens in Go, not in Flink. The relevant code is
[features.go](../services/go/internal/risk/features.go).

### 7.1 Flatten names without losing their group

Go prefixes every field:

```text
current_hand.position_index_a -> current_position_index_a
context.skill_level_gap       -> context_skill_level_gap
user_history_a.hands_seen     -> user_a_hands_seen
user_history_b.hands_seen     -> user_b_hands_seen
pair_history.hands_together   -> pair_hands_together
```

`context_status_a` and `context_status_b` remain categorical fields.

### 7.2 Apply the frozen preprocessing contract

The trained artifact `models/pair-catboost-v1/preprocessing.json` defines:

- the exact column order;
- 54 numeric input columns;
- train-fitted fill values for missing numeric data;
- two categorical columns; and
- the known and unknown one-hot categories.

Each context status becomes two values: `matched` and `__UNKNOWN__`. Therefore:

```text
54 numeric values
+ 2 values for context_status_a
+ 2 values for context_status_b
= 58 float32 values for one pair
```

Statuses such as `matched_late`, `missing`, or `corrected` currently map to the
frozen `__UNKNOWN__` bucket because they were not separate training categories
in the v1 artifact. This behavior must change only through a new trained and
versioned preprocessing contract.

### 7.3 Assemble one complete hand

The Go hand assembler waits for all 15 pair identities. Pairs are sorted by
`pair_key`, each is transformed into 58 ordered `float32` values, and the
result has this shape:

```text
15 pairs x 58 features = [15, 58]
```

The model returns 15 raw pair probabilities. Go then:

1. applies Platt calibration;
2. applies the frozen decision threshold;
3. aggregates pair probabilities into player and hand risk; and
4. publishes score and alert events with model, feature, and policy versions.

Read [the exact model input contract](realtime-model-input-contract.md) for all
58 ordered columns, data types, missing-value rules, tensor names, and model
outputs.

## 8. Which ML algorithms run where?

| Component | Algorithm | Real-time? | Runs in Flink? |
|---|---|---:|---:|
| Feature pipeline | temporal join, rolling aggregates, pair combinations | Yes | Yes |
| Primary v1 model | CatBoost exported to ONNX | Yes | No; Go/ONNX or optional Triton |
| Calibration | Platt scaling | Yes | No; Go scorer |
| Decision policy | frozen probability threshold and aggregation | Yes | No; Go scorer |
| Explainability/rules | evidence codes and deterministic rules | Yes | No; scorer/sink path |
| Deep sequence models | LSTM/Transformer research challengers | Not in v1 hot path | No; offline training |
| Graph models | VGAE/HGT research challengers | Not in v1 hot path | No; DGX/offline training |

Flink should produce stable, explainable features. It should not contain model
weights or training logic. Keeping the model behind the Kafka feature contract
lets us retrain or replace CatBoost, and later replace Go with Rust, without
rewriting the stateful streaming pipeline.

## 9. Failure, replay, and correctness

### What a checkpoint protects

A checkpoint should include:

- Kafka source offsets;
- all keyed user, hand, context, and pair state;
- pending event-time timers; and
- sink progress supported by the configured delivery guarantee.

Production still needs durable checkpoint storage configured at the Flink
cluster/SPCS level and a tested savepoint restore procedure. The Java jobs
enable periodic checkpointing, but code-level enablement alone is not durable
infrastructure.

### What can happen after a restart

Because the current Kafka sinks are at-least-once, an output can be repeated.
It should have the same deterministic event ID and revision. Consumers must
deduplicate or upsert by identity. A repeated input must not increase a user's
or pair's rolling count a second time.

### Why TTL is not a business-time guarantee

State TTL is a processing-time safety limit for inactive data. Event-time
timers implement business deadlines. Increasing TTL does not increase the
allowed-lateness or correction windows.

### Partitioning limitation to keep visible

`poker.pair-features.v1` is currently keyed by `pair_key`, so the 15 pairs from
one hand can land in different Kafka partitions. Until a repartitioned topic
keyed by tenant and hand is introduced, run exactly one Go scorer replica; two
independent in-memory assemblers could otherwise each receive only part of a
hand.

## 10. How to build and verify the implementation

Run unit tests and build the shaded Java 17 JARs:

```bash
make enrichment-topics
make flink-context-test
make flink-context-build
make flink-pair-features-test
make flink-pair-features-build
```

Important tests include:

- context selection never chooses a future version;
- missing and corrected context behavior;
- rejection of private truth fields;
- six players create exactly 15 pairs;
- rolling snapshots exclude the current hand; and
- Java pair features match the Python offline definition.

For a deterministic finite audit, run both jobs with `--from-beginning
--bounded`. Bounded mode snapshots Kafka end offsets, drains that fixed input,
advances the final watermark, and exits. Then check feature contracts and load
the resulting events:

```bash
python scripts/check_pair_features.py \
  --input-topic poker.hand-player-context.v1 \
  --minimum-records 300

python scripts/ingest_pair_features.py \
  --migrate --from-beginning --max-messages 300
```

See the module runbooks for command-line options:

- [Context enrichment README](../streaming/flink-java/context-enrichment/README.md)
- [Pair-feature README](../streaming/flink-java/pair-features/README.md)

## 11. What to inspect when something looks wrong

| Symptom | First checks |
|---|---|
| Hands wait forever | Source watermarks, idle partitions, event timestamps, context bootstrap state |
| Too many `missing` contexts | Context topic key, `effective_at`, topic retention/compaction, allowed wait |
| Unexpected corrections | Context version conflict, effective date, correction-window settings |
| Fewer than 15 pairs | Missing enriched player row, mismatched `num_players`, DLQ, hand assembly state |
| Rolling counts jump after replay | Event IDs, frozen per-hand history, checkpoint restore, dedup state TTL |
| Go rejects an event | feature-definition version, lineage fields, pair order, forbidden label fields |
| Tensor has wrong width | deployed `preprocessing.json` differs from model artifact or feature contract |
| Model scores but no hand completes | pairs split across scorer replicas or an incomplete 15-pair hand |

Every production dashboard should expose Kafka lag, watermark delay, checkpoint
age/failures, DLQ rate, join-status counts, correction rate, state size, hand
assembly timeouts, and model inference latency.

## 12. Code-reading map

Read the implementation in this order:

1. [ContextEnrichmentJob.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/ContextEnrichmentJob.java) — topology and Kafka wiring.
2. [ContextTemporalJoinFunction.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/ContextTemporalJoinFunction.java) — join state and timers.
3. [TemporalJoinLogic.java](../streaming/flink-java/context-enrichment/src/main/java/com/aicampions/poker/context/TemporalJoinLogic.java) — deterministic selection and output.
4. [PairFeaturesJob.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairFeaturesJob.java) — second topology.
5. [UserRollingFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/UserRollingFunction.java) — prior user snapshot.
6. [HandPairAssemblyFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/HandPairAssemblyFunction.java) — six rows to 15 pairs.
7. [PairRollingFunction.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairRollingFunction.java) — event ordering and prior pair snapshot.
8. [PairFeatureMath.java](../streaming/flink-java/pair-features/src/main/java/com/aicampions/poker/features/PairFeatureMath.java) — exact feature formulas.
9. [Go features.go](../services/go/internal/risk/features.go) and [assembler.go](../services/go/internal/risk/assembler.go) — flatten, transform, and create `[15, 58]`.

## 13. Official reading, in a useful order

Start with the first three and return to the rest when the corresponding topic
appears in the code:

1. [Apache Flink concepts overview](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/overview/) — streams, transformations, and basic terminology.
2. [Stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/) — why state and checkpoints are central to Flink.
3. [Flink 1.19 architecture](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/concepts/flink-architecture/) — jobs, operators, parallelism, JobManager, and TaskManagers.
4. [Timely stream processing](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/concepts/time/) — event time, processing time, watermarks, and late events.
5. [Generating watermarks](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/datastream/event-time/generating_watermarks/) — timestamp assignment and idle partitions.
6. [Working with state](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/state/) — keyed state types and TTL.
7. [Process functions and timers](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/process_function/) — the low-level API used by these jobs.
8. [Checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/checkpointing/) — enabling and configuring recovery snapshots.
9. [Savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/savepoints/) — controlled upgrades of stateful applications.
10. [State backends](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/state_backends/) — where managed state is held and persisted.
11. [Flink Kafka connector](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/connectors/datastream/kafka/) — sources, sinks, offsets, and delivery guarantees.
12. [DataStream connector guarantees](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/) — what at-least-once and exactly-once mean at connector boundaries.

Use the `release-1.19` pages while this repository remains on Flink 1.19.1.
The `stable` pages are useful for general concepts, but APIs can change between
Flink releases.
