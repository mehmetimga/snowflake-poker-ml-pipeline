# Data Generation, Storage, and Pipeline Plan

## Purpose

This document is the detailed data plan for the real-time poker risk project.
It defines how synthetic data is generated, where each class of data lives,
how events enter the pipeline, how labels remain isolated, and how identical
inputs are reproduced for local tests, Snowflake training, and DGX evaluation.

The companion
[real-time context and ML plan](realtime-context-ml-implementation-plan.md)
defines the models, services, features, and delivery roadmap. This document is
the source of truth for data ownership and movement.

The detailed scale-up design for 100 concurrent tables, 4–6 player occupancy,
multi-table users, alert cases, and leakage-safe benchmark products is in the
[100-table test data and alert plan](100-table-test-data-and-alert-plan.md).

## Architecture decision

Use each storage system for one clear responsibility:

| System | Responsibility | Not used for |
|---|---|---|
| Immutable files | Reproducible generated datasets, manifests, label sidecars, and replay | Live mutable state |
| PostgreSQL | Future external poker-server and company operational state | Analytical features or training scans |
| PostgreSQL outbox | Future atomic bridge from business changes to events | Long-term event retention |
| Kafka | The only live entrance to the downstream pipeline and the replay transport | Ad hoc training queries |
| Flink state | Point-in-time online context and rolling user/pair features | Permanent system of record |
| Snowflake | Durable events, context history, features, labels, scores, and training datasets | Per-hand synchronous lookups |
| DGX Spark storage | Frozen, secret-free dataset exports and model artifacts | Source-of-truth data |

The core rule is:

```text
Files preserve reproducibility.
PostgreSQL represents future external operational state.
Kafka feeds the live pipeline.
Snowflake feeds model training.
```

The main frozen dataset path still feeds Kafka directly. An isolated local
simulation now also uses PostgreSQL and Debezium to prove the future CDC
boundary. Access to a real poker server is not a runtime dependency.

No producer may bypass Kafka and write a live hand directly into a feature or
training table. A batch loader may ingest a frozen dataset into raw Snowflake
tables for offline tests, but it must use the same event contracts and produce
the same normalized records as the streaming path.

## End-to-end data flow

### Current direct-publish mode

Build this first because it has the fewest moving parts and validates the data
contracts quickly:

```text
                         immutable dataset directory
SyntheticPokerWorld ---> events/*.jsonl + labels/*.jsonl + manifest.json
                                  |
                             replay/publish
                                  |
                                  v
                               Kafka
              +-------------------+-------------------+
              |                                       |
              v                                       v
      Flink context/feature jobs              Snowflake raw sinks
              |                                       |
              v                                       v
      poker.pair-features.v1              raw and historical tables
              |
              v
        Go risk scorer
              |
              +----> risk scores and alerts ----> Kafka ----> Snowflake
```

The generator writes the frozen world first. Separate publishers then read the
files and publish context, session, account-link, and hand events in event-time
order. This keeps generation deterministic even when Kafka timing varies.

### Future poker-server and company CDC mode

This is the planned production integration, not part of the current project
runtime:

```text
Poker server transaction
    +----> PostgreSQL binary hand-history row
    +----> hand-completed outbox row
                    |
                    v
          Debezium / Kafka Connect
                    |
                    v
       restricted integration topic
                    |
                    v
      versioned hand-history adapter
                    |
                    v
          poker.hands.raw.v1

Company user/account DB -> outbox/CDC -> canonical context topics
```

The preferred poker-server change is to insert the binary hand history and a
`hand_completed` outbox row in the same PostgreSQL transaction. Debezium then
publishes the committed change without an unsafe application-level dual write.
The frozen C2 mapping consumes the raw PostgreSQL change envelope so the
versioned adapter retains the original binary payload, transaction, and LSN.
Debezium's
[Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
is an optional routing layer only if all frozen payload and lineage fields are
provably retained.

If the external poker server cannot add an outbox, Debezium may capture its
append-only hand-history table. A boundary adapter must decode the binary using
an explicit codec version, validate completion and checksum, and publish the
canonical hand event. Raw CDC envelopes and poker-server binary formats never
become contracts consumed by Flink, Snowflake transformations, or models.
Debezium supports PostgreSQL `BYTEA`, as documented by the
[PostgreSQL connector](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).

The current SyntheticPokerWorld and PokerKit publisher sends hands and context
directly to Kafka. It must produce the same canonical schemas expected from the
future adapter.

For parity fixtures, direct and CDC modes must emit semantically equivalent
canonical payloads after CDC envelope translation. Downstream Flink, Snowflake,
and scoring components must not know which integration produced an event.
The executable source row, envelope, codec seam, lineage headers, and parity
gate are documented in
[`docs/debezium-hand-history-ingress.md`](debezium-hand-history-ingress.md).

### Production replacement

In a company deployment, replace the direct synthetic publishers with the real
poker, user, and account sources:

```text
poker transaction -> outbox/CDC -> hand adapter -> canonical hand event -> Kafka
company transaction -> outbox/CDC -> canonical context event -> Kafka
```

The event schemas, topic keys, downstream processing, historical tables, and
model inputs remain unchanged.

## Deterministic synthetic world

Implement one Python `SyntheticPokerWorld` that owns all correlated synthetic
state. Do not run independent random generators for hands, users, and context;
they would produce relationships that are internally inconsistent.

The world owns:

- Users and accounts.
- Devices and network clusters.
- Sessions and login cadence.
- Account links and shared resources.
- Geographic and timezone buckets.
- Account age, KYC state, bankroll, preferred stakes, and skill.
- Tables, seats, game time, and stakes.
- Collusion groups, strategies, intensity, and active periods.
- Context changes and their effective times.
- Legal poker state, actions, cards, and settlement through PokerKit.
- Private ground-truth labels and scenario metadata.

PokerKit remains responsible for valid poker mechanics. The world layer decides
who plays, which contextual relationships exist, when they change, and whether
a hand follows a collusive scenario.

### Generation sequence

For each dataset split:

1. Derive a split-specific seed from the root dataset seed.
2. Create a disjoint population of users and stable synthetic identifiers.
3. Assign realistic, overlapping user traits and behavioral distributions.
4. Create devices, network clusters, account links, and normal shared-resource
   cases.
5. Select private collusion groups without exposing membership in events.
6. Schedule logins, sessions, context changes, and tables on one logical clock.
7. Use PokerKit to generate legal hands and actions for seated players.
8. Inject scenario behavior at variable strengths, including weak multi-hand
   signals.
9. Expand every six-player hand into all 15 canonical unordered player pairs
   for private pair-level labeling.
10. Write public event files and private label sidecars separately.
11. Produce counts, hashes, configuration, and scenario summaries in a manifest.

Normal users must sometimes share devices, networks, sessions, countries, or
playing schedules. Colluders must not always share those properties. Otherwise
the model will learn a synthetic shortcut instead of behavior.

### Time model

Every event contains both:

- `occurred_at`: the logical business time used for joins and windows.
- `emitted_at`: the publisher time used to measure delay.

The generator first produces a canonical event-time schedule. Publishers may
then simulate delivery delay, duplicates, or disorder without changing the
logical dataset.

Context updates include `effective_at` and a monotonically increasing
`context_version` per user. Training joins and Flink joins select the latest
version effective at the hand time, never the newest version available now.

### Identifiers

Identifiers are deterministic UUIDs derived from:

```text
dataset_id + split + entity type + logical entity index
```

Event IDs are derived from the logical event identity, not wall-clock publish
time. Replaying the same dataset therefore produces the same IDs and supports
idempotent sinks.

Player-pair keys are canonical:

```text
pair_key = min(user_a, user_b) + ":" + max(user_a, user_b)
```

Use synthetic or tokenized IDs and coarse context buckets. Do not generate raw
PII that is unnecessary for model behavior.

## Dataset layout on disk

Use this layout for every frozen world:

```text
data/datasets/<dataset_id>/
  manifest.json
  config.json
  schemas.json
  train/
    events/
      hands.jsonl
      user_context.jsonl
      sessions.jsonl
      account_links.jsonl
    labels/
      hand_labels.jsonl
      pair_labels.jsonl
    snapshots/
      users.jsonl
      devices.jsonl
      network_clusters.jsonl
  validation/
    events/
    labels/
    snapshots/
  test/
    events/
    labels/
    snapshots/
  challenge/
    events/
    private_labels/
    snapshots/
```

JSONL is the canonical human-readable replay format. Large benchmark datasets
may additionally include Parquet mirrors, but their manifest must state that
they were derived from the canonical event records.

The manifest records:

- Dataset and schema versions.
- Root and split seeds.
- Generator commit and PokerKit version.
- Requested and actual entity/event counts.
- Earliest and latest event times.
- Public event file SHA-256 hashes.
- Private label file SHA-256 hashes.
- Expected Kafka counts by topic.
- Expected Snowflake counts by table.
- Positive, negative, and unlabeled pair counts.
- Scenario distribution and intensity ranges.
- Split population-disjointness evidence.

Generated datasets and model artifacts remain ignored by Git. Small golden
fixtures used by tests may be committed under `tests/golden/`.

## Future PostgreSQL operational model

This section defines the future integration contract. These tables are not
required to run the current direct-publish project. A real company deployment
will normally integrate with externally owned schemas rather than recreate
them in this repository.

### Tables

`users`

- `user_id` primary key.
- Tenant and product IDs.
- Creation and update timestamps.
- Account status and current context version.

`user_context_current`

- `user_id` primary key and foreign key.
- Current coarse country, timezone, acquisition, KYC, bankroll, stake, and
  skill fields.
- `context_version` and `effective_at`.

`user_sessions`

- `session_id` primary key.
- `user_id`, device, network cluster, login/logout times, and state.

`user_devices`

- User/device relationship, first/last seen times, trust class, and active
  flag.

`user_network_links`

- User/network-cluster relationship and first/last seen times.

`account_links`

- Canonical relationship between two accounts, coarse relationship type,
  confidence, effective time, and version.

`outbox_events`

- `event_id` primary key.
- Aggregate type and aggregate ID.
- Canonical event type, schema version, partition key, and JSON payload.
- `occurred_at`, `created_at`, and optional publication metadata.

### Write rule

Each business update and its outbox row are committed in the same PostgreSQL
transaction. Never update a context table and publish to Kafka in two unrelated
operations; a crash between them would lose or invent an event.

Debezium reads the outbox and publishes it. A small Go context adapter may
unwrap the connector envelope and validate the canonical event, but should not
reimplement database log reading.

PostgreSQL is not used for rolling pair features, full raw hand history, model
training tables, or analytical joins.

## Kafka contracts and feeding

Kafka is the live interface between producers and all downstream consumers.

| Topic | Key | Cleanup policy | Producer |
|---|---|---|---|
| `poker.hands.raw.v1` | `table_id` | Delete | Python PokerKit replay or Go gateway |
| `poker.user-context.v1` | `user_id` | Compact | Direct publisher or CDC adapter |
| `poker.session-context.v1` | `session_id` | Delete | Direct publisher or CDC adapter |
| `poker.account-links.v1` | `user_id` | Compact/delete | Direct publisher or CDC adapter |
| `poker.pair-features.v1` | `pair_key` | Delete | Flink |
| `poker.rule-evidence.v1` | `entity_type:entity_key` | Delete | Flink/Go rule engines through Go publisher |
| `poker.risk-scores.v1` | `hand_id` | Delete | Go risk scorer |
| `poker.review-decisions.v1` | `hand_id` | Delete | Go review policy |
| `poker.risk-alerts.v1` | `hand_id` | Delete | Go review policy |
| `poker.labels.v1` | `example_id` | Restricted/delete | Delayed label loader only |
| `poker.pipeline.dead-letter.v1` | `event_id` | Delete | Contract validators and consumers |

Hands use `table_id` as the partition key so table order is preserved. Context
uses its lookup key so updates for an entity are ordered. Each consumer dedupes
by deterministic `event_id`.

The v1 pair-feature topic remains keyed by `pair_key` to preserve pair update
order. Consequently, the first Go scoring consumer runs as a single replica
and assembles hands across assigned partitions. Horizontal scoring requires a
small repartition stage keyed by `hand_id`; this is a scaling boundary, not a
model or contract change.

### Publisher modes

`generate`

- Creates the immutable files and exits without external writes.

`realtime`

- Reads a frozen world and publishes according to event-time spacing at a
  configurable wall-clock scale.

`accelerated`

- Publishes at a bounded configured rate for integration and load tests.

`replay`

- Republishes the exact canonical events with stable IDs and payloads.

`chaos`

- Applies a recorded delivery schedule containing duplicates, delays,
  reordering, malformed test records, and temporary pauses. Canonical source
  files are not mutated.

All publishers merge source files through an event-time priority queue. They
serialize through one schema library, require broker acknowledgements, retain
stable event IDs for deduplication, expose success/error metrics, and write a
run report containing attempted and acknowledged counts.

Historical replay keeps business time in `occurred_at` and in a Kafka header,
but uses the current publish time as Kafka's record timestamp. Setting the
record timestamp to an old synthetic event time can make delete-policy topics
expire a valid replay immediately. The current Python client uses `acks=all`,
bounded retries, one in-flight request, stable event IDs, and downstream
deduplication; a future client migration may add native producer idempotence.

For a live test against a long-running event-time job, generate a new world
with an explicit UTC anchor instead of editing timestamps after generation:

```bash
python scripts/generate_realtime_world.py \
  --output-dir /tmp/poker-live-smoke \
  --dataset-id poker-live-smoke \
  --train-hands 10 --validation-hands 1 --test-hands 1 --challenge-hands 1 \
  --players 24 --tables 1 --pairs 4 --seed 610 \
  --hand-start-at 2026-07-21T18:30:00Z
```

Choose an anchor later than the job's current watermark but safely behind wall
clock. An unbounded stream also needs later events to advance the watermark and
release its newest buffered window. Never future-date events merely to flush a
test: rule-evidence governance correctly rejects an `emitted_at` earlier than
`occurred_at`.

## Flink online data path

Flink consumes Kafka only. It must not perform a synchronous PostgreSQL or
Snowflake query while scoring a hand.

The jobs:

1. Validate envelopes and route invalid records to the dead-letter topic.
2. Deduplicate deterministic event IDs.
3. Maintain versioned user, session, and account-link state.
4. Apply watermarks and a documented allowed-lateness policy.
5. Join each hand with context effective at the hand's event time.
6. Mark missing, late, stale, and corrected context explicitly.
7. Expand a six-player hand to 15 unordered candidate pairs.
8. Maintain rolling user and pair windows with checkpointed keyed state.
9. Publish versioned pair-feature snapshots.
10. Send raw, enriched, and feature events to idempotent Snowflake sinks.

Late context that falls within the correction window may produce a corrected
feature snapshot with a higher revision. Events outside that window remain in
history and trigger a data-quality signal instead of silently changing a score.

## Snowflake data model

Snowflake is the durable analytical source of truth.

### Raw and historical data

- `RAW_HANDS`
- `RAW_ACTIONS`
- `RAW_PLAYERS`
- `USER_CONTEXT_EVENTS`
- `USER_SESSION_EVENTS`
- `ACCOUNT_LINK_EVENTS`
- `RAW_RISK_SCORES`
- `USER_CONTEXT_HISTORY`
- `USER_CONTEXT_CURRENT`
- `USER_SESSION_HISTORY`
- `ACCOUNT_LINK_HISTORY`

Raw sinks are idempotent on `event_id`. Historical context uses SCD Type 2
intervals with `effective_from`, `effective_to`, `context_version`, and
`is_current`. Intervals for one entity may not overlap.

### Features, labels, and governance

- `HAND_PLAYER_CONTEXT_SNAPSHOT`
- `PAIR_HAND_EVENTS`
- `USER_ROLLING_FEATURES`
- `PAIR_ROLLING_FEATURES`
- `PAIR_TRAINING_EXAMPLES`
- `LABEL_EVENTS`
- `FEATURE_DEFINITIONS`
- `DATASET_MANIFESTS`
- `MODEL_RUNS`
- `MODEL_METRICS`
- `MODEL_ARTIFACTS`
- `ALERTS`
- `ANALYST_FEEDBACK`

Every training example records the exact hand time, context versions, feature
definition, label provenance and availability time, split, and dataset ID.
Every score records the input snapshot, feature version, model version,
threshold, probabilities, evidence codes, event IDs, and trace ID.

Training reads Snowflake point-in-time tables. It does not consume an arbitrary
current Kafka state or current PostgreSQL row.

## Labels and leakage controls

Labels are a separate data product, never a field on hand, user-context,
session, account-link, or pair-feature events.

Synthetic generation writes:

- Public inference events under `events/`.
- Private hand and pair labels under `labels/`.
- Private scenario details needed only for evaluation.

For train, validation, and test, an authorized label loader writes labels to
the restricted `LABEL_EVENTS` table or topic after raw events exist. Challenge
labels remain outside Kafka and Snowflake until replay and scoring finish.

Label records contain `label_available_at`. A training example can use a label
only when the selected training cutoff is on or after that timestamp. Analyst
feedback follows the same rule.

Automated tests scan every inference schema and serialized event for forbidden
fields such as collusion membership, scenario name, label, future outcome, or
split-derived shortcuts.

## Train, validation, test, and challenge data

Assign the split before generating any entity. Use split-specific populations,
seeds, time ranges, and collusion groups.

| Split | Used for | Label visibility |
|---|---|---|
| Train | Fit preprocessing and model parameters | Available to training jobs |
| Validation | Early stopping, calibration, and thresholds | Available to evaluation jobs |
| Test | One final frozen comparison | Hidden until the model/threshold is fixed |
| Challenge | End-to-end Kafka replay and operational evaluation | Held outside the pipeline until scoring ends |

Maintain several benchmarks:

- Cold-start: fully disjoint users and pairs across splits.
- Temporal: later time windows for known populations with point-in-time joins.
- New-relationship: known users but unseen user-pair relationships.
- Challenge: label-free online replay with delayed reveal.

Fit encoders, normalizers, vocabularies, graph construction rules, sampling
ratios, and feature selection on train only. Validation chooses checkpoints and
thresholds. Test is evaluated once. No raw identifier embedding is allowed to
act as a split or collusion lookup.

## DGX export and artifact flow

DGX training receives a frozen export, not database credentials:

```text
Snowflake point-in-time dataset
    -> local export job
    -> NPZ/Parquet tensors and tables
    -> manifest + schema + hashes
    -> secure copy to DGX
    -> containerized training
    -> metrics + model artifacts
    -> artifact registry / controlled fetch
```

The export contains only model inputs, approved labels for the selected split,
stable example IDs, and metadata required for reproducibility. It excludes
Snowflake, Kafka, and PostgreSQL secrets.

The manifest captures source dataset IDs, Snowflake query/version, feature
definition, split counts, preprocessing fitted on train, library versions, and
hashes. The trained artifact points back to this manifest.

## Data-quality and parity tests

### Generator tests

- A repeated seed produces identical logical events, labels, counts, and hashes.
- Different splits have disjoint populations where required.
- PokerKit hands remain legal and settlements balance.
- Every six-player hand produces exactly 15 canonical pairs.
- Public events contain no label or private scenario field.
- Normal and collusive context distributions overlap.

### Future direct-versus-CDC parity

For the same context commands:

1. Publish once through the direct file publisher.
2. Apply once through PostgreSQL plus outbox and Debezium.
3. Normalize connector envelopes.
4. Compare canonical topic key, headers, envelope, and payload.

The canonical outputs must match, excluding connector transport metadata.

### Pipeline tests

- Topic keys preserve required entity/table order.
- Duplicate events do not create duplicate warehouse rows or scores.
- Flink recovery from a checkpoint produces the same feature snapshots.
- Late and missing context follow the documented policy.
- Online Flink features equal offline Snowflake features for golden examples.
- Replaying a dataset produces identical model inputs and scores for a fixed
  model version.
- Kafka acknowledgements and Snowflake row counts match the manifest.
- Challenge labels cannot be read by inference identities.

## Repository additions

```text
pipeline/
  events/                         # Canonical envelopes and schemas
  context/                        # Context domain and deterministic generation
  generator/                      # SyntheticPokerWorld and PokerKit integration

services/go/
  event-gateway/
  context-adapter/                # CDC envelope -> canonical event
  risk-scorer/

streaming/flink-java/
  context-enrichment/
  pair-features/

schemas/
  events/
  features/
  scores/
  alerts/

infra/local/                       # Future CDC integration
  compose.yaml                    # PostgreSQL, Kafka, Connect, Flink for local tests
  postgres/init/
  connect/
    debezium-outbox.json

sql/postgres/                      # Future external-schema fixtures
  001_operational_context.sql
  002_outbox.sql

sql/migrations/
  007_canonical_events_and_context.sql
  008_pair_feature_events.sql
  009_training_examples.sql
  010_feedback_and_registry.sql

scripts/
  generate_realtime_world.py
  replay_world.py
  load_context_postgres.py         # Future CDC integration
  export_pair_dataset.py

tests/golden/
  events/
  cdc/
  features/
  model_inputs/
```

## Implementation order and acceptance gates

Current status: Stages 1 and 2 are implemented. Stage 1 provides
Python/Pydantic contracts, exported JSON Schemas, deterministic context-rich
split generation, private player/pair label sidecars, manifests, hashes, and a
small generated `context-v1` sample. Stage 2 provides managed topic definitions,
event-time stream merging, replay/realtime/accelerated/chaos delivery, broker
acknowledgement reports, and consumer-side source verification. A bounded train
split was published to and verified from Confluent Cloud. The durable-ingestion
half of Stage 3 is also implemented and verified: canonical envelopes flow from
Confluent into idempotent Snowflake raw tables, context history is reconstructed
by effective time, and Kafka offsets are committed only after warehouse success.
The first two native Flink slices are implemented. Hands expand to player rows,
join to context effective at hand time, and publish versioned
matched/late/missing/corrected records. The next job keeps prior-only user and
pair state, reassembles hands, expands six players to 15 pairs, and publishes
`pair-features-v1`. A bounded 20-hand Confluent audit produced all 300 expected
rows with exact online/offline payload parity. A production savepoint restore
drill and the Snowflake pair-feature write after renewed MFA remain open.

### Stage 1: contracts and immutable files

- Implement the common envelope and versioned domain schemas.
- Implement `SyntheticPokerWorld` and frozen split directories.
- Produce manifests, labels, hashes, and golden fixtures.

Accept when seeds reproduce identical hashes, PokerKit legality checks pass,
and no inference record contains private label data.

### Stage 2: direct Kafka replay

- Implement real-time, accelerated, replay, and chaos scheduling.
- Publish hands and context to separate topics with correct keys.
- Record acknowledged counts and validate them against the manifest.

Accept when a bounded frozen world can be replayed twice with identical IDs and
decoded payloads.

### Stage 3: Flink and Snowflake path

- Persist raw Kafka events idempotently.
- Build point-in-time context history.
- Join hands with effective context and publish pair features.

Accept when online/offline golden features match and replay does not duplicate
facts.

Status: Snowflake/DuckDB migrations, envelope audit, normalized raw loading,
SCD2 context history, replay idempotency, the multi-topic warehouse sink, and
native Java/Flink player-context enrichment are complete. A bounded Confluent
audit produced and validated all 120 expected player-hand rows (106 `matched`,
14 `matched_late`, zero future-context joins). The native pair-feature job then
produced all 300 expected snapshots for 20 hands with exact Python parity.
Migration 008 and the idempotent sink were replayed twice into DuckDB without
duplicates; the equivalent Snowflake write awaits a fresh MFA login.

### Stage 4: scoring and labels

- Add the Go risk scorer and score/alert topics.
- Load delayed labels into restricted Snowflake tables.
- Build point-in-time pair training examples.

Accept when challenge inference completes without label access and its scores
can be evaluated only after the private label reveal.

### Stage 5: frozen DGX datasets

Status: the pair-level file product is implemented. It derives features from
immutable context-world inputs with prior-only state, writes cold-start,
temporal, new-relationship, and challenge Parquet benchmarks, excludes
challenge from DGX exports, and records reproducible artifact hashes. The
current representative build contains 1,050 DGX rows across the three labeled
benchmark suites and passes the leakage audit.

- Export train/validation/test datasets and manifests.
- Copy secret-free bundles to DGX.
- Train, evaluate, and register artifacts with full lineage.

Accept when a second export from the same source produces identical hashes and
a training run can be traced to source events, features, labels, and code.

### Stage 5.1: point-in-time multi-hand histories

Status: implemented for the full `context-full-v2`/`pair-full-v2` cold-start
dataset. The builder aligns 300,000 train, 75,000 validation, and 75,000 test
pair examples with left-padded histories of the two users and their pair. It
processes equal-timestamp hands as an isolated group, verifies every last-seen
timestamp is strictly earlier than the example, fits no statistics while
building, reads no challenge artifacts, and emits deterministic NPZ files with
SHA-256 lineage. The compressed dataset is approximately 44 MB.

- Build histories locally from immutable hand events.
- Verify event order, source hashes, timestamp boundaries, and split isolation.
- Copy only the public sequence bundle, public pair rows, and CatBoost public
  evaluation artifacts to DGX.
- Fit normalization and self-supervised encoders on train histories only.

The first DGX sequence model did not improve on CatBoost, so the artifact is a
research input rather than a promoted serving dependency.

### Stage 5.2: prior-only heterogeneous graphs

Status: implemented for cold-start and new-relationship evaluation. The graph
builder replays immutable context and hand events in timestamp order, captures
typed neighborhoods before applying the current timestamp's hands, and exports
750,000 pair-aligned snapshots. Relations cover prior co-play, devices,
networks, sessions, tables, and explicit account-link evidence. Raw identifiers
are used only to construct topology and are never model inputs or embedding
indices. All graph tensors, masks, alignments, and source hashes are recorded in
a deterministic 62 MB artifact.

- Cold-start rows retain disjoint user populations.
- New-relationship rows retain hand-atomic protected-pair assignments.
- A matching public-only CatBoost baseline is built without challenge access.
- DGX receives only public pair rows, graph tensors, public predictions, and
  artifact manifests.

The first GraphSAGE run improved over the neural tabular/sequence candidates
but remained below CatBoost on both benchmarks, so it is not a serving input.

### Stage 6: CDC-shaped real-time simulation

- [x] Generate deterministic PokerKit hands as Protobuf bytes in PostgreSQL.
- [x] Filter allowed game types transactionally into an immutable outbox.
- [x] Run Debezium against logical WAL and publish only to the isolated local
  simulation ingress topic.
- [x] Run the versioned Go hand adapter locally and validate canonical output,
  lineage, topic keys, and zero-DLQ acceptance.
- [x] Prove exact checksum, malformed-binary, game-mismatch, unknown-codec,
  sanitized-DLQ, and post-publish commit-recovery behavior locally.
- [ ] Run the same adapter image and accepted fault/replay manifest in
  `POKER_ADAPTER_SIM` against isolated Confluent topics.
- Feed simulated user/account context directly to separate Kafka topics.

Accept when the simulation produces expected canonical/DLQ counts, complete
lineage, deterministic replay, and no records on production topic names.

## First implementation slice

The next build slice is deliberately small:

1. Add event and context schemas.
2. Implement deterministic users, devices, networks, sessions, and context
   changes around the existing PokerKit generator.
3. Write one small frozen dataset with all four splits and label sidecars.
4. Implement the direct Kafka replay path.
5. Verify topic keys, counts, hashes, determinism, and label separation.
6. Persist raw events and temporal context in Snowflake.
7. Preserve a versioned canonical boundary for future Debezium integration.

The offline contract, local PostgreSQL/Debezium simulation, Protobuf codec, Go
publish-or-DLQ-before-commit runtime, Docker image, and private simulation SPCS
specification are now implemented. Local fault/replay coverage is accepted;
the next slice is the isolated Confluent/SPCS replay. The external poker-server
database and real binary codec remain outside current project scope.
