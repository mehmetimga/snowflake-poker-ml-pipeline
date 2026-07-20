# Real-Time Context and ML Implementation Plan

## Purpose

This document defines the implementation plan for evolving the poker collusion
demo into a reusable, company-wide real-time risk platform.

The detailed decisions for synthetic generation, files, PostgreSQL, CDC,
Kafka feeding, Snowflake persistence, label isolation, dataset splits, and DGX
exports are maintained in the
[data generation, storage, and pipeline plan](data-generation-and-pipeline-plan.md).

The project will:

- Generate valid poker hands with PokerKit.
- Generate deterministic synthetic user, account, device, network, and session context.
- Publish hands and context changes through Kafka in real time.
- Join events with point-in-time context in Flink.
- Build player-pair features and rolling behavioral state.
- Persist raw events, historical context, features, labels, scores, and alerts in Snowflake.
- Train classical, deep-learning, sequence, and graph models on DGX Spark.
- Preserve leakage-safe train, validation, test, and challenge datasets.
- Establish reusable event, feature, model, audit, and governance contracts.
- Use Go for operational services and realtime model scoring.
- Use Java only where required for Flink's stateful streaming data plane.
- Preserve stable contracts so measured Go bottlenecks can later move to Rust.

## Architecture decision

Use a Snowflake-hosted ML architecture with external event transport:

- Kafka carries live hand, session, account-link, and user-context changes.
- Java/Flink runs as a long-running Snowpark Container Services (SPCS) service,
  performs event-time joins, and maintains rolling user and pair state.
- The Go real-time scorer runs as a separate long-running SPCS service.
- The current CatBoost ONNX model runs directly in Go or through an optional
  CPU Triton sidecar in the same SPCS service instance. The direct ONNX path is
  preferred until a larger model justifies Triton.
- Snowflake stores immutable history, context versions, feature snapshots, labels,
  training datasets, model metadata, scores, and alerts.
- Python training, monitoring, sinks, and administration run as SPCS services or
  job services. Snowflake tables, stages, and the model registry are managed data
  services, not Docker containers.
- DGX Spark is an optional offline research accelerator only. It is not a
  dependency in the production real-time path.
- Go services own ingestion APIs, model routing, realtime scoring, and alert delivery.
- Java is restricted to Flink jobs that require native state, event-time, and recovery APIs.
- Real-time scoring never queries Snowflake tables synchronously for each hand.

The company user/account database remains the source of truth. In production,
changes should reach Kafka through change data capture (CDC) or a transactional
outbox. The external poker server should eventually expose completed hands
through a transactional outbox and Debezium, with a versioned adapter isolating
its binary hand-history format. In this project, PokerKit hands and synthetic
context publish directly to Kafka; PostgreSQL and Debezium are deferred. See the
[data plan](data-generation-and-pipeline-plan.md) for the authoritative storage
and feed sequence.

The editable visual architecture is in
[`poker-ml-end-to-end.excalidraw`](poker-ml-end-to-end.excalidraw). Every compute
box now names its deployment boundary and target Docker image. It distinguishes
client/source processes, external Confluent Cloud, SPCS container workloads,
Snowflake managed data services, and the optional external DGX research lab.

### Deployment boundaries

| Boundary | Runs here | Does not run here |
|---|---|---|
| Client/source environment | PokerKit generator today; future poker server, PostgreSQL/outbox, and Debezium CDC | Flink, scoring, training, monitoring |
| Confluent Cloud | Kafka brokers, Schema Registry, topics, ACLs | Custom application Docker images |
| Snowflake SPCS | Java/Flink, Go scorer, optional CPU Triton sidecar, Python Kafka sink, training jobs, monitoring jobs, and admin UI | Poker server or source PostgreSQL |
| Snowflake data services | Tables, stages, model registry, secrets, and governance metadata | Docker processes |
| DGX Spark, optional external lab | Offline DL/GNN experiments using frozen exports | Production streaming or real-time inference |

Target images in the Snowflake Image Repository are deliberately separate:

| Image | SPCS deployment | Purpose |
|---|---|---|
| `poker-flink:<git-sha>` | `POKER_FLINK` long-running service | Java 17/Flink enrichment, temporal joins, state, and feature computation |
| `poker-risk:<git-sha>` | `POKER_RISK` long-running service | Go Kafka consumption, hand assembly, scoring policy, and publishing |
| `tritonserver:<pinned>` | Optional second container in each `POKER_RISK` instance | CPU ONNX serving over localhost; omit when Go embeds ONNX |
| `poker-sink:<git-sha>` | `POKER_SINK` long-running service | Idempotent Kafka-to-Snowflake persistence |
| `poker-train:<git-sha>` | `POKER_TRAIN_JOB` job service | Reproducible training, calibration, evaluation, and export |
| `poker-monitor:<git-sha>` | `POKER_MONITOR_JOB` job service | Scheduled feature, score, and delayed-label monitoring |
| `poker-admin:<git-sha>` | `POKER_ADMIN` long-running service | Analyst and operational UI |

For a demo, these services can share one CPU compute pool. Production should
separate streaming, serving, batch, and administration pools so that scaling,
failure, and cost boundaries are independent. Confluent credentials are held in
a Snowflake Secret and exposed to the required services through a narrowly
scoped External Access Integration.

```text
                       +----------------------+
User/account DB --CDC->| user-context.v1     |--+
Synthetic context ---->| Kafka compacted     |  |
                       +----------------------+  |
                                                 v
PokerKit generator ---> hands.raw.v1 -------> SPCS: poker-flink image
   client/local                                 Java/Flink enrichment
                                                 |
                                                 v
                          Confluent: pair-features.v1
                                                 |
                                                 v
                                      SPCS: poker-risk image
                                      Go + embedded ONNX
                                  or CPU Triton localhost sidecar
                                                 |
                                   +-------------+-------------+
                                   v                           v
                            risk-scores.v1            risk-alerts.v1
                                   |                           |
                    +--------- SPCS: poker-sink image ----------+
                                                 |
                             point-in-time training examples
                                                 |
                                  SPCS: poker-train job
                                                 |
                                    train/evaluate/register
                                                 |
                               mount artifact in POKER_RISK

Optional only: frozen export -> external DGX research -> gated artifact registry
```

## Language strategy

The official language map is:

```text
Python = simulation, data engineering, Snowflake integration, ML, DL, GNN, and DGX
Go     = gateways, context adapters, realtime scoring, APIs, and alert delivery
Java   = native Flink stateful streaming jobs only
SQL    = Snowflake transformations and Flink relational stream processing
Rust   = future replacement for specific measured Go performance bottlenecks
```

| Component | Initial language/runtime | Long-term direction |
|---|---|---|
| PokerKit and synthetic-world generation | Python | Keep Python |
| Dataset construction and model training | Python | Keep Python |
| Snowflake transformations | SQL and Python | Keep |
| Event gateway and context adapter | Go | Keep Go unless profiling justifies Rust |
| Realtime risk scorer | Go | Primary Rust migration candidate |
| Alert delivery and risk API | Go | Keep Go |
| Stateful temporal joins and rolling state | Java/Flink or Flink SQL | Keep Java isolated here |
| GPU model serving | NVIDIA Triton | Keep; do not custom-build |
| CDC | Debezium/Kafka Connect | Do not custom-build by default |
| High-throughput feature/rule kernel | Go initially | Optional Rust replacement |

### Python boundary

Python remains the main ML and domain-development language. It owns PokerKit,
synthetic context, frozen datasets, Snowflake loading, feature research,
CatBoost, deep learning, GNN training, evaluation, calibration, ONNX export,
DGX workflows, and the admin prototype.

Python should not become the company-wide event gateway or latency-sensitive
service layer.

### Go boundary

Go is the primary application-service language:

- `event-gateway` authenticates producers, validates envelopes, applies tenant
  metadata, enforces quotas, and publishes to Kafka.
- `context-adapter` translates company user/account changes into canonical
  versioned context events when a standard CDC connector is insufficient.
- `risk-scorer` consumes all pair features for a hand, executes local ONNX or a
  batched Triton request, calibrates and aggregates scores, and publishes risk
  and alert events.
- `alert-dispatcher` handles delivery, retries, deduplication, and audit records.
- `risk-api` exposes scores, evidence, policy versions, health, and tenant-aware
  operational endpoints.

Confluent maintains a Go Kafka client based on `librdkafka`; use it for custom
Go producers and consumers. See the
[Confluent Go client documentation](https://docs.confluent.io/kafka-clients/go/current/overview.html).

### Java boundary

Java is allowed only for native Flink jobs. It owns watermarks, temporal joins,
late-data handling, checkpointed user/pair state, state TTL, recovery,
rescaling, pair expansion, and online feature computation.

Do not put model training, general APIs, Snowflake orchestration, admin
functionality, or unrelated business services in Java. Prefer Flink SQL where
it expresses the transformation clearly; use small Java functions only for
domain-specific processing or state behavior.

### Rust migration boundary

Do not introduce Rust before profiling. Design Go services around Kafka,
Protobuf/Avro, gRPC, ONNX, health, and metrics contracts so an entire service
can later be replaced without changing its neighbors.

Likely Rust candidates are the risk scorer, feature/rule evaluation kernel,
event validator, pair aggregation, and CPU inference wrapper. Prefer replacing
a complete Go service and running Go/Rust implementations in shadow mode over
introducing cross-language FFI.

Rust promotion requires a material measured improvement in CPU cost, memory,
throughput, or p99 latency while maintaining golden-output parity.

### Model-serving boundary

Small CPU models should run inside the `POKER_RISK` SPCS service. For the
current CatBoost champion, embedded ONNX in Go is the default because a remote
GPU hop adds complexity without useful acceleration. If we retain Triton's
serving contract, deploy a pinned CPU Triton container beside Go in the same
SPCS service instance and make one localhost batch request per hand, not one
request per pair.

Do not call the physical DGX from production real-time scoring. DGX remains an
optional offline lab for neural and GNN challengers. If a future model needs a
GPU in production, first query the Snowflake account for available compute-pool
instance families, then deploy Triton to an SPCS GPU pool in a supported region.
Triton provides ONNX/PyTorch/TensorRT backends, dynamic batching, HTTP/gRPC,
health endpoints, and metrics. See the
[NVIDIA Triton documentation](https://docs.nvidia.com/deeplearning/triton-inference-server/user-guide/docs/index.html).

## Kafka versus database responsibilities

This section summarizes the serving decisions. The detailed operational and
analytical storage model is defined in the
[data generation, storage, and pipeline plan](data-generation-and-pipeline-plan.md).

### Real-time scoring

Use Kafka and Flink state for context lookups. Do not synchronously read
Snowflake or the operational user database for every hand because that would
introduce:

- Network latency and inconsistent performance.
- Database availability as a scoring dependency.
- Increased Snowflake query cost.
- Inconsistent historical joins.
- Difficulty replaying an identical stream deterministically.

The user-context topic should be keyed by `user_id` and compacted so Kafka
retains the latest value for each user. Kafka's compact policy is designed to
retain the latest record for every key. See the
[Confluent topic configuration reference](https://docs.confluent.io/platform/7.7/installation/configuration/topic-configs.html).

### Production source of truth

```text
user DB transaction
    -> transactional outbox or CDC
    -> Kafka user-context topic
    -> Flink state
    -> Snowflake context history
```

For the demo:

```text
synthetic context generator
    -> Kafka user-context topic
    -> the same downstream path
```

If CDC is not initially available, use a versioned snapshot publisher. This is
a temporary bridge, not the final real-time architecture.

### Training

Training reads Snowflake rather than Kafka. Snowflake provides durable,
auditable history and reproducible point-in-time joins. Kafka remains the event
transport and replay source, not the training query engine.

## Event envelope

All topics should use a common envelope with a domain-specific payload:

```json
{
  "event_id": "uuid",
  "event_type": "poker.hand.completed",
  "schema_version": 1,
  "tenant_id": "demo",
  "product_id": "poker",
  "dataset_id": "cold-start-v1",
  "dataset_split": "train",
  "occurred_at": "2026-07-19T18:30:00Z",
  "emitted_at": "2026-07-19T18:30:01Z",
  "trace_id": "uuid",
  "payload": {}
}
```

Required semantics:

- `event_id` is globally unique and used for idempotency.
- `occurred_at` is business/event time.
- `emitted_at` is producer time.
- `dataset_id` identifies a reproducible generated world.
- `dataset_split` is assigned before generation and never inferred afterward.
- `tenant_id` and `product_id` prepare the platform for company-wide use.
- Labels never appear in hand, context, session, or account-link payloads.

Use Schema Registry with Avro, Protobuf, or JSON Schema. Enforce backward
compatibility and test schema compatibility in CI.

## Kafka topic design

| Topic | Key | Retention | Purpose |
|---|---|---|---|
| `poker.hands.raw.v1` | `table_id` | Delete | Complete PokerKit hands in table order |
| `poker.user-context.v1` | `user_id` | Compact | Versioned user-profile updates |
| `poker.session-context.v1` | `session_id` | Delete | Login, device, network, and session events |
| `poker.account-links.v1` | `user_id` | Compact/delete | User-device-network-account relationships |
| `poker.pair-features.v1` | `pair_key` | Delete | Online pair-feature snapshots |
| `poker.risk-scores.v1` | `hand_id` | Delete | Model outputs and model versions |
| `poker.risk-alerts.v1` | `hand_id` | Delete | Actionable model-risk alerts |
| `poker.labels.v1` | `example_id` | Restricted/delete | Delayed synthetic or analyst labels |
| `poker.pipeline.dead-letter.v1` | `event_id` | Delete | Invalid or incompatible events |

Hands should be partitioned by `table_id`, rather than `hand_id`, so hands at
the same table remain ordered. `hand_id` remains the deduplication identifier.

Topic configuration should be managed as code. Define partitions, replication,
retention, cleanup policy, schema subject, producer ownership, consumer
ownership, and access-control rules for every topic.

## Synthetic poker world

The generation contract, on-disk split layout, label sidecars, deterministic
identifiers, publisher modes, and acceptance tests are specified in the
[data generation, storage, and pipeline plan](data-generation-and-pipeline-plan.md).

Create a deterministic `SyntheticPokerWorld` above the existing PokerKit hand
generator. PokerKit remains responsible for legal poker state, betting order,
cards, and settlement. The world layer controls users, sessions, context,
relationships, scenarios, and event scheduling.

The synthetic world owns:

- Users and accounts.
- Devices and network clusters.
- Geographic and timezone buckets.
- Account creation dates.
- KYC and account status.
- Bankroll and preferred stakes.
- Skill and behavioral style.
- Sessions and login cadence.
- Tables and seating.
- Collusion groups and strategies.
- Context changes over time.

### User-context event

Example:

```json
{
  "user_id": "user-123",
  "context_version": 7,
  "effective_at": "2026-07-19T18:00:00Z",
  "account_created_at": "2025-11-03T00:00:00Z",
  "country_bucket": "TR",
  "timezone": "Europe/Istanbul",
  "acquisition_channel": "organic",
  "kyc_level": "verified",
  "account_status": "active",
  "bankroll_bucket": "medium",
  "preferred_stake_bucket": "1_2",
  "skill_rating": 0.63,
  "device_id": "device-456",
  "network_cluster_id": "network-18",
  "session_id": "session-789"
}
```

Use synthetic or tokenized identifiers and coarse buckets rather than raw PII.
Sensitive attributes that are not justified for risk detection should not be
generated or used by the model.

### Context scenarios

Generate:

- Account creation.
- Login and session creation.
- Device changes.
- Network-cluster changes.
- Stake-preference changes.
- Bankroll changes.
- Account suspension and reactivation.
- Late-arriving context corrections.
- User deletion/tombstone events.

Normal users must occasionally share devices or network clusters, while
colluders must not always share them. Otherwise the generated model will learn
unrealistic shortcuts.

### Collusion scenarios

Continue the existing PokerKit patterns and add:

- Soft play.
- Chip dumping.
- Raise/fold benefit.
- Squeeze coordination.
- Call-down transfer.
- Coordinated table selection.
- Coordinated session timing.
- Multi-hand signaling patterns.
- Groups larger than two for later coalition testing.

Vary collusion intensity. Include weak scenarios that can only be detected
from rolling pair history or graph relationships.

## Real-time test-data orchestrator

Add:

```text
scripts/generate_realtime_world.py
```

Supported modes:

```bash
# Wall-clock demo
python scripts/generate_realtime_world.py \
  --mode realtime --hands-per-second 2 --duration-minutes 30

# Fast integration run
python scripts/generate_realtime_world.py \
  --mode accelerated --hands 5000 --rate 100

# Deterministic replay
python scripts/generate_realtime_world.py \
  --mode replay --dataset data/datasets/context-v1

# Reliability and disorder testing
python scripts/generate_realtime_world.py \
  --mode chaos --duplicate-rate 0.01 --late-rate 0.02
```

The orchestrator should merge hand and context events into an event-time
priority queue and publish each event to its correct topic.

Every generated dataset should contain:

- Event JSONL files.
- Label sidecars.
- Context snapshots.
- Seed and configuration manifest.
- SHA-256 hashes.
- Expected topic and warehouse counts.
- Collusion scenario distribution.
- Expected positive-label counts.

The same seed and configuration must reproduce the same logical data.

## Snowflake data model

### Raw immutable tables

- `RAW_HANDS`
- `RAW_ACTIONS`
- `RAW_PLAYERS`
- `USER_CONTEXT_EVENTS`
- `USER_SESSION_EVENTS`
- `ACCOUNT_LINK_EVENTS`
- `LABEL_EVENTS`
- `RAW_RISK_SCORES`

Kafka messages remain domain events. Snowflake sinks normalize each domain
event into the appropriate raw tables. Producers should not publish
warehouse-table-shaped messages merely because the warehouse has multiple
tables.

### Historical context tables

- `USER_CONTEXT_HISTORY`
- `USER_CONTEXT_CURRENT`
- `USER_SESSION_HISTORY`
- `ACCOUNT_LINK_HISTORY`

`USER_CONTEXT_HISTORY` should use SCD Type 2 semantics:

```text
user_id
context_version
effective_from
effective_to
is_current
context fields...
```

Use Snowflake Streams and Tasks for durable SCD Type 2 processing. Snowflake
recommends Streams and Tasks when historical SCD2 state is required, while
Dynamic Tables are better suited to derived current state. See the
[Snowflake Dynamic Tables decision guide](https://docs.snowflake.com/en/user-guide/dynamic-tables/decision-guide)
and [Snowflake Streams CDC documentation](https://docs.snowflake.com/en/user-guide/streams-intro).

Do not rely on limited-retention Time Travel as the permanent feature-history
store.

### Feature and ML tables

- `HAND_PLAYER_CONTEXT_SNAPSHOT`
- `PAIR_HAND_EVENTS`
- `USER_ROLLING_FEATURES`
- `PAIR_ROLLING_FEATURES`
- `PAIR_TRAINING_EXAMPLES`
- `FEATURE_DEFINITIONS`
- `DATASET_MANIFESTS`
- `MODEL_RUNS`
- `MODEL_METRICS`
- `MODEL_ARTIFACTS`
- `ALERTS`
- `ANALYST_FEEDBACK`

Every scored pair should persist:

- Event ID and hand ID.
- Pair key.
- Context versions used for both players.
- Feature-definition version.
- Feature snapshot or snapshot ID.
- Model name and version.
- Raw and calibrated probabilities.
- Decision threshold.
- Rule evidence and explanation codes.
- Score timestamp and trace ID.

## Flink pipeline

The current Flink scorer uses no event-time watermarks and can broadcast pair
memory. That is acceptable for a small demo but should not be used for
company-wide user context.

Broadcast state is copied into every parallel operator and held in memory,
making it unsuitable for all company users. See the
[Flink Broadcast State documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/broadcast_state/).

Implement the following event-time pipeline:

1. Parse and validate hand events.
2. Assign event-time timestamps and watermarks.
3. Deduplicate by `event_id` and `hand_id`.
4. Expand each hand into six player-hand records.
5. Temporally join each player record to user context by `user_id`.
6. Temporally join session and account-link context when required.
7. Reassemble the enriched hand.
8. Expand six players into 15 candidate pairs.
9. Update keyed player and pair rolling state.
10. Compute pair features.
11. Publish versioned feature snapshots to `poker.pair-features.v1`.
12. Persist raw events and feature snapshots to Snowflake.

The Go `risk-scorer` then:

1. Collects all 15 pair rows for a hand.
2. Validates the feature-definition and model-contract versions.
3. Runs a small ONNX model locally or sends one batched request to Triton.
4. Applies calibration and the versioned decision policy.
5. Aggregates pair risk into player and hand risk.
6. Publishes `poker.risk-scores.v1` and `poker.risk-alerts.v1`.
7. Persists auditable model, feature, and policy metadata through the sink path.

Keeping scoring behind a Kafka contract allows the Go implementation to be
replaced by Rust later without redeploying Flink or retraining the model.

Use an event-time temporal join so a hand receives the context version valid
when the hand occurred. Flink temporal joins retrieve changing metadata at a
specific event time and require the context primary key in the join. See the
[Flink temporal joins documentation](https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/sql/queries/joins/).

Processing-time lookup joins may be used only as a temporary development
fallback because they are not deterministic under replay.

### Late and missing context policy

Initial policy:

- Allow 30 seconds of lateness locally; make it configurable in production.
- Represent missing context with explicit missing indicators.
- Never silently substitute current context for historical context.
- Route invalid context versions to the dead-letter topic.
- Record context version and join status in every feature snapshot.
- Permit deterministic replay after corrected context arrives.
- Define TTLs for keyed rolling state and test their effect on correctness.

## Pair-level prediction unit

For every six-player hand, produce the 15 possible player pairs and predict:

```text
P(pair is coordinating | hand, user histories, pair history, context)
```

The pair feature vector is:

```text
current-hand pair features
+ player A point-in-time context
+ player B point-in-time context
+ player differences and similarities
+ rolling history for A and B
+ rolling shared pair history
+ table and session context
```

Player risk is derived from the maximum or calibrated aggregation of the
player's pair scores. Hand risk is derived from the highest-risk pair or a
coalition aggregation model.

## Feature plan

### Current-hand pair features

- Both players' positions and actions.
- Bet, call, raise, and fold interaction.
- Amount and pot-relative differences.
- Direct benefit from the other player's action.
- Showdown and transfer outcome.
- Rule and action-motif outputs.

### Rolling user features

Compute windows over 10, 50, and 200 hands and over 1, 7, and 30 days:

- Aggression and fold rates.
- Stake and bankroll changes.
- Session frequency.
- Net win/loss.
- Behavioral deviation from personal baseline.
- Device/network churn.
- Opponent diversity.
- Table-selection distribution.

### Rolling pair features

- Co-play frequency.
- Shared sessions and tables.
- Directional chip transfer.
- Raise/fold benefit.
- Soft-play rate.
- Timing similarity.
- Shared device/network evidence.
- Outcome asymmetry.
- Pattern counts, intensity, and recency.

### Context crosses

- Stake versus bankroll.
- Skill difference and similarity.
- Account-age similarity.
- Country and timezone similarity.
- Device/network relationships.
- Acquisition-channel relationship.
- Session overlap.
- Behavioral-style similarity.

Offline Snowflake features and online Flink features must have parity tests
using identical event fixtures.

## Dataset strategy

Maintain three separate evaluation suites.

### Cold-start benchmark

Use disjoint users across splits:

| Split | Initial hands | Purpose |
|---|---:|---|
| Train | 20,000 | Model fitting |
| Validation | 5,000 | Early stopping, tuning, and threshold selection |
| Test | 5,000 | Untouched final evaluation |
| Challenge | 5,000 | Label-free Kafka replay |

Scale later to `200k/50k/50k/50k` for DGX experiments.

### Temporal benchmark

Use the same user population and split chronologically:

- First 70% of event time: train.
- Next 15%: validation.
- Final 15%: test.

This measures future behavior for previously observed users.

### New-relationship benchmark

Users may appear in training, but validation and test collusion pairs must be
unseen. This checks whether the model detects new harmful relationships rather
than memorizing pair IDs.

### Split and leakage rules

- Assign splits before generation.
- Keep all rows from a hand in one split.
- Keep challenge labels in local/restricted sidecars only.
- Fit scalers, categorical statistics, graph state, and embeddings on train only.
- Select early stopping and classification thresholds on validation only.
- Evaluate test once per registered model run.
- Permit features to use only events earlier than the scored hand.
- Track confirmed positive, confirmed negative, and unlabeled examples separately.
- Store the exact dataset manifest and feature-definition version with every run.

## Model roadmap

### Model v1: pair-level CatBoost

Train on `PAIR_TRAINING_EXAMPLES` using:

- Current-hand pair features.
- Rolling user features.
- Rolling pair features.
- Point-in-time user/session context.
- Context crosses.
- Rule and motif scores.

Add:

- Class weighting.
- Validation-selected decision threshold.
- Probability calibration.
- SHAP explanations.
- Recall at alert budget.
- Precision among top-ranked pairs.

This is the first deployable candidate.

### Model v2: DGX tabular challengers

Train on exactly the same feature rows:

- Residual MLP.
- FT-Transformer.
- DCN-V2.

Do not promote a neural model unless it beats CatBoost on both PR-AUC and
operational alert metrics.

Status: completed on the frozen `pair-full-v2` cold-start split. Residual MLP,
FT-Transformer, and DCN-V2 reached test PR-AUCs of `0.186673`, `0.182130`, and
`0.142649`, respectively, versus `0.362918` for CatBoost. Their paired
hand-bootstrap PR-AUC difference intervals were all strictly negative, and none
matched CatBoost on the full operational gate. CatBoost remains the champion;
the private challenge was not read.

### Model v3: multi-hand sequence model

A sequence should represent a user's or pair's previous hands, rather than
only the actions inside one hand.

Pretraining tasks:

- Masked-action prediction.
- Next-action prediction.
- Next stake/session prediction.
- Behavior-change prediction.
- Contrastive learning across behavioral windows.

Fine-tune the learned embeddings for pair risk.

Status: implemented and evaluated on the full frozen cold-start dataset. The
16-hand user/pair sequence artifact contains 450,000 aligned examples and
passes deterministic hashes, strict-prior timestamp checks, equal-timestamp
isolation, split-population checks, and challenge exclusion. Separate user and
pair Transformers were pretrained without labels using masked-step
reconstruction, next-step prediction, and contrastive window consistency, then
jointly fine-tuned with contextual tabular features. Run
`pair_history_fe49d3205cc2` reached test PR-AUC `0.181929`, F1 `0.258993`, and
recall `0.506667` at the 2% budget. CatBoost remains materially better at
`0.362918`, `0.421687`, and `0.706667`, respectively. The bootstrap difference
interval was strictly negative, so the acceptance lift was not met and no
private-challenge evaluation was opened.

### Model v4: temporal heterogeneous GNN

Nodes:

- Users.
- Devices.
- Network clusters.
- Sessions.
- Tables.
- Accounts.

Edges:

- Played with.
- Used device.
- Connected from network.
- Joined session.
- Transferred chips.
- Triggered action motif.

Train with time-ordered edges and inductive node features. Do not expose future
edges, future labels, or raw-ID-only embeddings.

Status: implemented and publicly evaluated. The frozen graph artifact aligns
750,000 cold-start and new-relationship examples with prior co-player, device,
network, session, table, and account-link neighborhoods. All source hashes and
event IDs match; equal-timestamp hands are isolated; every last edge precedes
the example; challenge artifacts are excluded; and the model contains zero raw
ID embeddings. Run `pair_graph_31b1df3bbf37` reached cold-start PR-AUC
`0.247934` versus CatBoost `0.362918`, and new-relationship PR-AUC `0.508470`
versus the public-only matching CatBoost baseline `0.615757`. Both paired
bootstrap intervals were strictly negative. The GNN improved over prior neural
models but did not meet stable incremental-lift acceptance, so no private
challenge evaluation was opened.

### Model v5: calibrated ensemble

Combine out-of-fold predictions from:

- Rules.
- CatBoost.
- Neural tabular model.
- Multi-hand sequence encoder.
- Temporal GNN.

Start with a calibrated logistic stacker. Use a more complex meta-model only if
it delivers stable improvement on all evaluation suites.

### Analyst AI layer

An LLM may later generate grounded analyst summaries from structured evidence,
rules, features, and model explanations. It must not be the primary numerical
risk scorer or receive unrestricted raw PII.

## Metrics and model gates

Track:

- PR-AUC.
- ROC-AUC as a secondary metric.
- Recall at a fixed alert volume.
- Precision among the top 100 and top 1,000 alerts.
- False positives per thousand hands.
- F1 at a validation-selected threshold.
- Calibration and Brier score.
- Cold-start performance.
- Known-user temporal performance.
- New-pair performance.
- Performance by stake and context segment.
- Inference latency and throughput.

No model should be promoted solely because it improves aggregate ROC-AUC.

## Testing plan

### Unit tests

- Context-generation determinism.
- Pair-label correctness.
- Exactly 15 pairs for a six-player hand.
- SCD2 interval construction.
- Rolling-window state updates.
- Missing-context defaults.
- Schema validation.
- User-context tombstones.
- Pair-key normalization.

### Leakage tests

- No labels on hand or context topics.
- No future context in historical joins.
- No player overlap in cold-start splits.
- No test data used for normalization or category statistics.
- No future graph edges during training.
- No raw-ID memorization as a primary feature.
- No analyst outcome available before its label timestamp.

### Streaming integration tests

- Context arrives before a hand.
- Context update arrives after a hand.
- Context arrives late but inside the watermark.
- Context arrives after allowed lateness.
- Missing context.
- Duplicate hand.
- Duplicate context version.
- Out-of-order hand.
- Invalid schema.
- Flink restart from checkpoint.
- Kafka replay produces identical scores.
- Snowflake persistence remains idempotent.

### Feature parity tests

For the same fixture:

```text
Snowflake offline feature row == Flink online feature row
```

Use explicit numeric tolerances and compare feature-definition versions.

### Load tests

Initial targets:

- 100 hands per second in accelerated mode.
- 1,500 pair scores per second.
- Zero synchronous database reads in the hot path.
- p95 end-to-end scoring latency below one second.
- Successful checkpoint recovery.
- Bounded keyed-state growth with configured TTL.
- Measured Kafka consumer lag and context-join miss rate.

### Security and governance tests

- Topic ACL validation.
- Snowflake role and masking-policy validation.
- No raw secrets or PII in logs.
- Tenant isolation.
- Context deletion/tombstone propagation.
- Model and feature audit completeness.

## Observability

Monitor:

- Kafka producer errors and consumer lag.
- Schema validation failures and DLQ rate.
- Context freshness and temporal-join miss rate.
- Late-event and duplicate-event rates.
- Flink checkpoint duration and failures.
- Keyed-state size and TTL eviction.
- Snowflake sink latency and rejected rows.
- Feature distribution drift.
- Score and alert-volume drift.
- Model latency and error rate.
- Analyst feedback rate and delayed precision.

Every alert should be traceable from Kafka event through feature snapshot,
model version, decision threshold, and Snowflake record.

## Repository implementation map

Keep the existing Python layout and add explicit Go, Flink/Java, schema, and
future Rust boundaries:

```text
pipeline/                         # Existing Python ML/data implementation
  events/
    envelope.py
    schemas.py
  context/
    models.py
    generator.py
    state.py
  generator/
    world.py
    realtime.py
  features/
    pair_features.py
    user_windows.py
    pair_windows.py
  ml/
    pair_dataset.py
    pair_train.py
    calibration.py
  dl/
    tabular_resnet.py
    ft_transformer.py
    dcn_v2.py
  gnn/
    temporal_graph.py
    temporal_train.py

services/go/
  go.mod
  event-gateway/
  context-adapter/
  risk-scorer/
  alert-dispatcher/
  risk-api/
  internal/
    events/
    kafka/
    observability/

streaming/flink-java/
  context-enrichment/               # Implemented Flink 1.19.1 Java module
  pair-features/
  action-patterns/

streaming/flink-sql/
  context-enrichment.sql
  rolling-features.sql

infra/local/
  compose.yaml
  postgres/init/
  connect/

sql/postgres/
  001_operational_context.sql
  002_outbox.sql

schemas/
  events/
  features/
  scores/
  alerts/

rust/                             # Created only after a profiling gate
  risk-scorer/
  risk-core/

scripts/
  generate_realtime_world.py
  replay_world.py
  load_context_postgres.py
  export_pair_dataset.py

sql/migrations/
  007_canonical_events_and_context.sql
  008_pair_feature_events.sql
  009_training_examples.sql
  010_feedback_and_registry.sql

tests/
  test_context_generator.py
  test_event_schemas.py
  test_temporal_join.py
  test_pair_features.py
  test_feature_parity.py
  test_realtime_world.py

tests/golden/
  events/
  features/
  model_inputs/
  scores/
```

## Delivery phases and acceptance gates

Implementation status: phases 1 through 5 are implemented. Canonical
contracts, context-rich frozen generation, direct multi-topic Kafka replay,
idempotent Snowflake canonical-event loading, SCD2 context history, and native
event-time context enrichment have passed bounded Confluent audits. Phase 6's
core pair-feature slice is also implemented: a bounded 20-hand replay produced
300/300 valid pair rows with exact Java/Flink versus Python parity, and the 300
feature facts were loaded and replayed idempotently in Snowflake. Durable
checkpoint/savepoint recovery testing remains before phase 6 is closed. Phase
7 is implemented and loaded into Snowflake. Phase 8's CatBoost training and
portable scoring slice is in progress.

### Phase 1: event contracts and Snowflake migrations

Deliverables:

- Common event envelope.
- Hand, user-context, session, account-link, label, score, and alert schemas.
- Topic configuration definitions.
- Snowflake migrations `007` through `010`.
- Shared Python and Go schema bindings.
- Golden cross-language event fixtures.

Acceptance:

- Schemas validate representative fixtures.
- Backward-compatibility tests pass.
- Migrations run on both DuckDB and Snowflake where applicable.

### Phase 2: deterministic synthetic context world

Deliverables:

- Synthetic users, devices, networks, sessions, and context changes.
- Context-label separation.
- Dataset manifests and hashes.

Acceptance:

- Repeated seeds reproduce identical logical events.
- Normal and colluding users have overlapping, non-trivial context distributions.
- No label fields appear in inference events.

### Phase 3: real-time Kafka generation

Deliverables:

- Real-time, accelerated, replay, and chaos modes.
- Context and hand producers.
- Go event-gateway skeleton and Kafka client package.
- Go risk-scorer skeleton with health and metrics endpoints.
- Topic-key and partition tests.

Acceptance:

- A bounded run publishes expected counts.
- Replaying a frozen world produces identical event IDs and payloads.
- Confluent accepts and consumers decode all schemas.

### Phase 4: Snowflake persistence and context history

Deliverables:

- Idempotent raw-event sinks.
- SCD2 context processing.
- Current context view/table.

Acceptance:

- Duplicate events do not create duplicate facts.
- Historical context intervals do not overlap.
- Point-in-time queries return the expected version.

### Phase 5: Flink event-time enrichment

Deliverables:

- Watermarks and event-time semantics.
- Temporal user-context join.
- Missing and late-context handling.
- Java restricted to the native Flink job and small domain functions.
- Flink SQL used for relational joins and windows where practical.

Acceptance:

- All temporal-join fixtures pass.
- Replay produces deterministic enriched records.
- No synchronous database reads occur during normal scoring.

### Phase 6: pair expansion and rolling features

Status: core implementation complete. The `pair-features-v1` contract,
prior-only offline oracle, native keyed-state Flink topology, managed Kafka
topic, idempotent migration/sink, correction fan-out, and real 300-row parity
audit are complete. The warehouse transaction was replayed twice into DuckDB
and remained at 300 facts. Snowflake execution is pending a renewed human MFA
token, and a savepoint restore drill is still required.

Deliverables:

- Fifteen candidate pairs per six-player hand.
- User and pair keyed state.
- Online pair-feature topic.
- Offline Snowflake feature generation.

Acceptance:

- Online/offline feature parity tests pass.
- State TTL and checkpoint recovery tests pass.

### Phase 7: frozen pair datasets

Status: implemented for `pair-features-v1`. The file-backed builder produces
cold-start, chronological 70/15/15, positive-relationship holdout, and isolated
challenge benchmarks. It also writes model-ready DGX Parquet exports, a schema,
source provenance, and SHA-256 hashes. The current smoke world produced 525
cold-start rows, 300 temporal rows, 300 new-relationship rows, and 75 isolated
challenge rows; all hash, hand-atomicity, player-leakage, relationship-leakage,
and challenge-boundary checks passed. Migration 009 defines restricted pair
labels and the point-in-time training-example view. Migration 009 and 450
non-challenge label rows were applied and replayed idempotently in Snowflake;
the point-in-time view currently joins 300 unique training examples.

Deliverables:

- Cold-start, temporal, new-pair, and challenge benchmarks.
- Point-in-time training-example table.
- Portable DGX export and manifest.

Acceptance:

- Leakage tests pass.
- Counts and hashes are reproducible.
- Challenge labels remain outside the inference path.

### Phase 8: pair-level CatBoost

Status: full training/serving slice implemented. It includes train-only
preprocessing, validation-only Platt calibration and alert-budget thresholding,
rules-only and player-only baselines, test/private-challenge reports, SHAP and
feature-importance summaries, native CatBoost and tensor-output ONNX artifacts,
artifact hashes, a complete-hand ONNX checker, and a Triton model repository.
The 20/5/5/5-hand smoke dataset is correctly blocked from promotion. A frozen
20k/5k/5k/5k v2 dataset now supplies 352/74/75 positive rows in the
train/validation/test partitions and passes all hashes and leakage checks. Its
validation-selected CatBoost candidate passes the strengthened promotion gate:
test PR-AUC 0.363 versus 0.239 player-only and 0.040 rules-only, test recall at
the 2% ranking budget 70.7%, and private-challenge PR-AUC 0.375 with 83.0%
recall at budget. Canonical dry replay verified 36,680 unique source events with
zero duplicates. The Go scorer core now verifies artifacts, reproduces the
train-fitted feature transform, assembles/reassembles corrected 15-pair hands,
calls the Triton V2 HTTP interface, applies calibration and decision policy,
aggregates player/hand risk, and exposes health/readiness/metrics/scoring HTTP
endpoints. The Go Kafka adapter now adds deterministic score/alert envelopes,
Confluent TLS/SASL support, synchronous acknowledged publishing, contiguous
offset commits, poison-record dead letters, and correction-aware replay. Topic
contracts and managed topic creation are implemented. The promoted ONNX model
is deployed to an ARM64 Triton container on DGX Spark behind a localhost-only
SSH tunnel. A bounded Confluent-to-Go-to-Triton replay published and validated
one complete v1 score event; Triton reported one successful 15-row batch and
zero failures. Phase 8's functional integration is complete. Production image
qualification, load/recovery testing, and the hand-keyed scale-out boundary
remain production-hardening work.

Deliverables:

- Calibrated CatBoost pair classifier.
- Validation-selected threshold.
- Test and challenge reports.
- Explanations and model artifacts.
- Go risk-scorer integration using the versioned pair-feature contract.
- Local ONNX path plus a batched Triton-compatible inference interface.

Acceptance:

- Beats rules-only and player-level baselines.
- Meets alert-budget and latency targets.

### Phase 9: DGX tabular challengers

Status: implemented and evaluated. Run `pair_challengers_7cd1845a955b` used the
identical frozen cold-start rows, train-only preprocessing, validation-only
checkpoint/calibration/threshold selection, one final public-test evaluation,
and 200 paired hand-bootstrap samples. Artifact hashes, split boundaries, row
counts, and challenge-label isolation passed. Residual MLP, FT-Transformer, and
DCN-V2 all remained materially below the CatBoost champion, so no model became
a promotion candidate and the isolated private challenge stayed sealed. See
`docs/dgx-pair-challengers-runbook.md` for exact results and commands.

Deliverables:

- Residual MLP, FT-Transformer, and DCN-V2 experiments.
- Identical frozen inputs and metrics.

Acceptance:

- Promotion only on statistically and operationally meaningful improvement.
- Completed with a correct non-promotion decision; CatBoost remains champion.

### Phase 10: multi-hand sequence model

Status: implementation and public evaluation complete; promotion acceptance
not met. The dataset builder, self-supervised pretraining, pair-risk
fine-tuning, DGX workflow, artifact checker, and leakage tests are implemented.
All 450,000 public examples are aligned to strictly earlier 16-hand histories.
The History Transformer underperformed contextual CatBoost, so CatBoost remains
the champion and challenge labels stay sealed. Before retrying this model class,
add real temporal signal such as stake/session transitions and device/network
changes rather than tuning repeatedly against the same public test set. See
`docs/dgx-pair-history-runbook.md`.

Deliverables:

- User and pair behavioral sequences.
- Self-supervised pretraining.
- Pair-risk fine-tuning.

Acceptance:

- Demonstrates incremental lift over contextual CatBoost.
- Current result: not met; no promotion.

### Phase 11: temporal heterogeneous GNN

Status: implementation and public evaluation complete; promotion acceptance
not met. The prior-only graph exporter, feature-only inductive GraphSAGE model,
matching new-relationship CatBoost baseline, DGX workflow, artifact checker,
and temporal leakage tests are implemented. Cold-start and new-relationship
quality gates both rejected promotion. See `docs/dgx-pair-graph-runbook.md`.

Deliverables:

- Time-ordered heterogeneous graph export.
- Inductive temporal model.
- Cold-start and new-pair evaluation.

Acceptance:

- No future-edge leakage.
- Unseen users receive feature-based embeddings.
- Demonstrates stable incremental lift.
- First two conditions passed; incremental lift was not met, so no promotion.

### Phase 12: ensemble and production hardening

Status: implementation and public evaluation complete. The leakage-safe OOF
stacker was rejected (`0.214408` test PR-AUC versus `0.362918` for the
CatBoost champion), so deployment remains unchanged. Registry, immutable
deployment and audit snapshots, drift monitoring, delayed analyst-feedback
contracts, tenant isolation/allowlists, restart recovery, concurrent load and
race tests, loopback-only profiling, and Prometheus/Grafana assets are
implemented. See `docs/phase12-production-hardening-runbook.md`.

Deliverables:

- Out-of-fold calibrated stacker.
- Model registry and deployment gates.
- Drift monitoring and analyst feedback loop.
- Multi-tenant security and audit controls.
- Stable Go service contracts and profiling dashboards.

Acceptance:

- End-to-end replay, recovery, load, security, and audit tests pass.
- Acceptance met; the ensemble quality gate correctly failed without opening
  the private challenge or changing production.

### Phase 13: optional Rust performance migration

This phase is activated only when production profiling identifies a material
Go bottleneck.

Deliverables:

- A benchmark and profiling report identifying the target service or kernel.
- Rust implementation behind the existing Kafka/gRPC/model contract.
- Go and Rust shadow-mode comparison.
- Golden score, behavior, failure, and load parity tests.

Acceptance:

- Meaningful improvement in CPU cost, memory, throughput, or p99 latency.
- Equivalent scores and decisions within defined numeric tolerances.
- No change required in Flink, Kafka topics, schemas, Snowflake, or model training.
- Operational ownership and rollback procedure are documented.

## Company-wide requirements

Include these fields and controls from the beginning:

- `tenant_id`.
- `product_id`.
- Namespaced entity IDs.
- Schema version.
- Event and ingestion timestamps.
- Trace and correlation IDs.
- Data classification.
- Retention class.
- Consent and deletion status.
- Feature-definition version.
- Model version.
- Decision-policy version.
- Service implementation and build version.

Keep poker payloads domain-specific. The following components should become
reusable company-wide:

- Common event envelope.
- Schema governance.
- Context history pattern.
- Feature-definition registry.
- Point-in-time dataset builder.
- Model registry and promotion gates.
- Analyst feedback contract.
- Observability and audit trail.
- Multi-tenant security controls.
- Cross-language golden fixtures.
- Go-to-Rust service replacement contracts.

## Immediate implementation slice

Start with phases 1 through 3:

1. Add the common event envelope and versioned schemas.
2. Add Snowflake context, session, account-link, pair-event, and feedback migrations.
3. Implement deterministic synthetic users and context changes.
4. Implement separate Kafka producers for user context, sessions, and hands.
5. Add the Go module, shared schema bindings, and event-gateway skeleton.
6. Add the Go risk-scorer skeleton with health, metrics, and Kafka interfaces.
7. Implement the real-time/accelerated/replay orchestration script.
8. Add schema, determinism, label-separation, topic-key, cross-language golden,
   and bounded integration tests.
9. Run a real-time smoke test through Confluent and verify event counts before
   changing the scorer.

This slice establishes live hand and user-context streams using the same
contracts that a future company CDC source will publish.
