# Active-user context and Flink architectural refactoring plan

Status: F1 complete; F2 in progress
Last reviewed: 2026-07-23

## 1. Outcome

Refactor the proven hand-driven JDBC context lookup into a production-shaped
`POKER_FLINK` implementation without adding Redis or another context service.

The permanent online service boundaries remain:

1. `POKER_ADAPTER` — hand CDC validation and decoding;
2. `POKER_FLINK` — active-player context enrichment and pair features; and
3. `POKER_RISK` — model inference, rules, and policy.

`POKERKIT_SIMULATOR` is the only simulator. PostgreSQL is the source of the
narrow user-context projection. Kafka carries hands and pipeline outputs, but
does not carry the canonical user-context table.

This document refines R2 and R3 of
[the SPCS service rationalization plan](spcs-service-rationalization-plan.md).
It does not authorize changing a remote SPCS service or deleting a legacy
service, topic, savepoint, table, or artifact.

## 2. Architecture decisions

| Decision | Direction |
|---|---|
| Hand ingestion | PostgreSQL hand history -> Debezium -> Kafka -> `POKER_ADAPTER` |
| Context ingestion | Lazy point-in-time PostgreSQL lookup inside `POKER_FLINK` |
| Initial lookup method | Synchronous JDBC with strict limits and Flink restart semantics |
| Active-player cache | Typed Flink keyed `ValueState`, 36-hour inactivity TTL |
| Context refresh | 60 minutes initially; tune using measured staleness and load |
| Full-table bootstrap | Never on the online path |
| Context Kafka topic | Legacy rollback path only; remove after cutover |
| Redis | Not justified at the expected 10,000 daily active users |
| New context microservice | Not justified; PostgreSQL remains the source |
| Async I/O | Implement only when load and backpressure gates require it |
| Permanent Flink service count | One `POKER_FLINK` SPCS service |
| Production authorization | Not the ML cache's responsibility |

The 60-minute cache refresh applies to analytical/ML context. It must not be
used as an authorization mechanism for suspensions, KYC blocks, or gameplay
eligibility. Those decisions stay in the authoritative poker/account path or a
separate rule gate with its own freshness requirement.

## 3. Review of the current implementation

The current JDBC slice is a successful proof:

- one six-player PokerKit hand seeds exactly six context rows;
- Flink loads only the six players observed in the hand;
- repeated hands can use keyed state;
- a 36-hour read/write TTL and a separate 60-minute refresh exist;
- point-in-time lookup uses `effective_at <= played_at`; and
- the bounded local Kafka/Flink/PostgreSQL test emitted six enriched rows and
  no unexpected DLQ records.

It is not ready for a production deployment yet. The following issues should
be fixed before package-level cleanup or SPCS cutover.

### 3.1 Critical correctness and security findings

| Finding | Risk | Required change |
|---|---|---|
| State and SQL are keyed only by `player_id` | Cross-tenant/product context collision | Key state and SQL by tenant, product, and player |
| JDBC password is a field on a serialized Flink function | Secret can enter the job graph, logs, or snapshots | Resolve credentials inside TaskManager `open()` |
| Every JDBC exception is converted to a DLQ and processing continues | A database outage can silently discard scoring work | Classify failures; restart on transient infrastructure errors |
| JDBC records are wrapped to resemble Kafka context events | Misleading lineage and contract semantics | Introduce an explicit JDBC context-resolution contract |
| The new path still emits the legacy event-time join policy name | Consumers cannot distinguish lookup behavior | Publish a versioned v2 enriched contract |
| Raw expanded hands are copied into DLQ records | Hole-card and user data exposure | Minimize DLQ payload and secure replay data separately |

### 3.2 Reliability and maintainability findings

- `ContextEnrichmentJob` owns topology construction, two incompatible context
  modes, argument parsing, Kafka properties, validation, and deployment
  defaults. These responsibilities need separate classes.
- The JDBC repository keeps one connection per subtask but has no reconnect,
  transient retry, validation, statement-timeout policy, or SQLSTATE
  classification.
- The synchronous lookup blocks its operator subtask. This is acceptable only
  while explicit latency, burst, and backpressure gates pass.
- Cached context is stored as JSON text and repeatedly parsed. Typed state with
  a declared serializer/schema version is safer and cheaper.
- The canonical SPCS spec still configures the context Kafka topic and does not
  mount PostgreSQL credentials in the TaskManager.
- The deploy helper adds an EAI only during `CREATE SERVICE IF NOT EXISTS`.
  Updating an existing service spec does not reconcile the required EAI set.
- Deployment code still contains `_SIM` service names and `poker.sim.*`
  terminology that conflict with the accepted canonical names.

## 4. Target production flow

```text
client/poker environment                         Snowpark Container Services

PostgreSQL hand history
        |
     Debezium
        |
Confluent Kafka: cdc-hand-outbox.v1
        |
        +-----------------------> POKER_ADAPTER (Go)
                                   validate + decode
                                          |
                              hands.raw.v1 |
                                          v
Context PostgreSQL <--- lazy JDBC --- POKER_FLINK (Java/Flink)
narrow versioned projection             |
                                        +-- validate hand
                                        +-- expand six players
                                        +-- key by tenant/product/player
                                        +-- active context TTL state
                                        +-- point-in-time context lookup
                                        +-- expand fifteen player pairs
                                        +-- stateful pair features
                                                   |
                                      pair-features.v1/v2
                                                   |
                                                   v
                                      POKER_RISK (Go + Triton)
                                      CatBoost + rules + policy
                                                   |
                                   scores / evidence / decisions
                                                   |
                                              POKER_SINK
                                                   |
                                          Snowflake tables
                                                   |
                                             POKER_ADMIN
```

`POKER_FLINK` is one SPCS service containing the Flink JobManager,
TaskManager, and submitter containers. It can submit separate context and pair
feature jobs to the same Flink cluster. That is not two SPCS services.

## 5. Target Flink topology

```text
Kafka hand source
    -> contract validation
    -> flat-map hand to player rows
    -> keyBy(ContextKey(tenant_id, product_id, player_id))
    -> ActiveUserContextFunction
         1. inspect typed ValueState
         2. return fresh effective cached context, or
         3. query PostgreSQL for latest effective version
         4. update state
         5. emit an explicit context-resolution result
    -> enriched hand-player v2 sink
    -> pair expansion and feature job
```

Use stable explicit operator UIDs. The canonical JDBC topology must have its
own UID namespace. Do not restore a savepoint from the legacy two-input Kafka
temporal join into the JDBC topology.

### 5.1 State key

Use a typed logical key:

```text
ContextKey {
  tenant_id,
  product_id,
  player_id
}
```

Never concatenate unescaped values into an ambiguous key. If Flink requires a
string transport key, use a versioned canonical encoding and test collisions.

### 5.2 Typed state

Replace `ValueState<String>` with:

```text
ActiveContextCacheEntry {
  schema_version,
  context_record_id,
  context_version,
  effective_at,
  loaded_at,
  model_feature_projection
}
```

Configuration:

- state TTL: 36 hours;
- update mode: `OnReadAndWrite`;
- visibility: `NeverReturnExpired`;
- freshness: a separate `loaded_at + refresh_interval`;
- first serializer/schema version: `active-context-cache-v1`.

The TTL is an inactivity/residency policy, not a historical retention policy.
PostgreSQL and Snowflake retain the authoritative history. Old historical
replay outside the supported live horizon belongs in the batch path.

### 5.3 Point-in-time lookup

The logical key and index become:

```sql
WHERE tenant_id = ?
  AND product_id = ?
  AND user_id = ?
  AND effective_at <= ?
ORDER BY effective_at DESC, context_version DESC
LIMIT 1
```

Add a new migration instead of rewriting the already-applied
`003_user_context_lookup.sql`. Backfill the POC tenant and product, replace
the primary/unique keys, and add the matching covering lookup index.

If a cached record is too new for a late hand, query PostgreSQL again for the
latest record effective at that hand's `played_at`. Do not apply a future
context version to an older hand.

## 6. Context and output contracts

### 6.1 Narrow model projection

Only cache fields required by online features, rules, lineage, and audit. Do
not copy the complete account record into Flink state.

Suggested groups:

- identity key: tenant, product, and pseudonymous player ID;
- temporal lineage: context record ID, version, effective time, and load time;
- stable model attributes: account age, country/region bucket, acquisition
  bucket, and skill segment;
- semi-dynamic model attributes: bankroll/stake segment and device/network
  risk aggregates; and
- explicit missingness flags.

Sensitive raw identifiers and fields unused by the model do not belong in the
Kafka output, state, logs, or DLQ.

### 6.2 Enriched contract v2

Do not silently change the meaning of the current v1 join fields. Introduce an
enriched player-hand v2 contract containing:

```text
context_resolution {
  mode: "postgresql_point_in_time",
  policy_version: "jdbc-effective-at-v1",
  source: "postgresql",
  context_record_id,
  context_version,
  context_effective_at
}
```

The context row is a database snapshot, not a synthetic Kafka event. Retain
source hand event IDs and dataset/run lineage independently.

Do not put `context_loaded_at`, cache hit/miss, or lookup latency in this
contract. Those values depend on the particular execution and belong in
metrics. Excluding them keeps deterministic replay byte-stable.

### 6.3 Failure semantics

Fail closed: never fabricate a context and never score an incomplete
player-hand row unless a future model contract explicitly supports that mode.

| Failure | Behavior |
|---|---|
| No effective context exists | Data-quality quarantine with a small diagnostic envelope |
| Invalid hand/context contract | Terminal DLQ with reason code; raw replay payload stored in a restricted location |
| Transient timeout/reset/connection SQLSTATE | Retry once with jitter, then throw so Flink restarts from its checkpoint |
| Authentication, permission, or schema failure | Fail the job immediately and alert |
| PostgreSQL broadly unavailable | Flink restart backoff prevents offsets from advancing and avoids silently dropping work |

An in-operator circuit breaker is not required in the first synchronous
version. Flink restart/backoff is the simpler outage boundary. Reconsider a
circuit breaker when async lookup or batching is introduced.

## 7. Proposed Java structure

Keep one Maven build initially. Split the legacy job into a separate artifact
only if packaging or independent retirement requires it.

```text
com.aicampions.poker.context
  app/
    ActiveContextEnrichmentJob
    LegacyKafkaTemporalContextJob
  config/
    ContextJobConfig
    JdbcLookupConfig
    KafkaConfig
    TopicBoundary
  domain/
    ContextKey
    UserContext
    ActiveContextCacheEntry
    ContextResolution
    LookupFailure
  ports/
    UserContextRepository
    CredentialProvider
    TimeProvider
  infra/jdbc/
    PostgresUserContextRepository
    JdbcConnectionManager
    SqlStateClassifier
  flink/
    HandPlayerExpander
    ActiveUserContextFunction
    ContextStateDescriptor
  contracts/
    HandEventReader
    EnrichedPlayerHandV2
    DiagnosticEnvelope
  legacy/
    LegacyTemporalJoinFunction
    LegacyTemporalJoinLogic
```

Rules:

- `domain` does not import Flink, JDBC, Kafka, or Snowflake classes;
- `ports` describe external access without deployment knowledge;
- TaskManager `open()` constructs the repository and resolves secrets;
- config parsing returns non-secret typed configuration;
- topology entrypoints wire components but contain no SQL or JSON-building
  logic; and
- the legacy entrypoint is rollback-only, uses separate consumer groups and
  topics, and has a removal date.

## 8. JDBC design for the synchronous phase

Use a small reconnecting connection manager per operator subtask:

- one active connection, with a maximum of two only if validation/reconnect
  requires it;
- TLS required;
- connection timeout and statement timeout;
- read-only database role;
- prepared point-in-time query;
- connection validation before reuse after an error;
- one bounded retry for transient SQLSTATE classes; and
- no retry for authentication, permission, schema, or not-found results.

Do not open a connection per hand or player. With parallelism `P`, plan for
approximately `P` steady PostgreSQL connections and verify the database limit
before increasing Flink parallelism.

The initial synchronous capacity approximation is:

```text
lookup capacity per subtask ~= 1 / p95 lookup latency
```

Ten thousand unique players spread over a day are a very small average lookup
rate. The meaningful test is a tournament/reconnect burst, not the daily
average.

## 9. When to move to Flink Async I/O

Keep synchronous JDBC while all initial gates pass:

- warm cache hit ratio is at least 95%;
- PostgreSQL lookup p95 is at most 100 ms and p99 at most 250 ms;
- Kafka lag is stable in normal traffic and drains within five minutes after
  the agreed cold-player burst;
- lookup wait contributes less than 10% sustained operator backpressure;
- database error rate is below 0.1%; and
- the database connection count remains inside its reserved budget.

These values are initial engineering hypotheses. Record real baselines and
tune them. Start the async implementation when a limit is breached in three
consecutive five-minute windows or the cold-start load test misses its exit
gate.

The async change should replace only the repository execution adapter:

- bounded capacity;
- ordered or unordered completion chosen explicitly;
- lookup timeout;
- retry classification identical to the sync path; and
- checkpoint/restart tests.

Do not add Redis automatically after async I/O. Consider Redis only if
PostgreSQL remains the proven bottleneck after query/index, connection,
parallelism, and async improvements, and if the team accepts cache
invalidation and another production dependency.

## 10. SPCS networking, secrets, and deployment

`POKER_FLINK` needs outbound access to both Kafka and the context database.
Use least-privilege integrations:

- `POKER_FLINK_KAFKA_EAI`; and
- `POKER_FLINK_CONTEXT_DB_EAI`.

The PostgreSQL network rule should name only the required hostname and port.
Use private connectivity when available. Grant a read-only database identity
access only to the narrow context projection.

Create a Snowflake Secret such as `CONTEXT_DB_CREDENTIALS`. Mount it only in
the TaskManager container as the username/password environment variables read
during operator `open()`. The submitter may receive the non-secret JDBC URL,
table name, timeouts, and refresh settings, but not the password as a job
argument or serialized config field.

Refactor `deploy_service()` to:

1. accept an ordered collection of EAI names;
2. create a missing service with the complete EAI set;
3. reconcile an existing service with
   `ALTER SERVICE ... SET EXTERNAL_ACCESS_INTEGRATIONS = (...)`;
4. update the spec;
5. read back the effective service configuration; and
6. fail if the actual EAI or secret references differ from the declared
   catalog.

Changing the EAI list replaces the existing list, so the reconciler must
always send the complete intended set.

## 11. Migration and implementation phases

### F0 — Freeze decisions and baseline

Status: complete.

- [x] Hands remain on Kafka.
- [x] User context moves to hand-driven lazy PostgreSQL lookup.
- [x] Redis and a new context service are excluded.
- [x] Local six-player JDBC proof passes.
- [x] Existing code, SPCS spec, and deploy helper reviewed.

Exit gate: this plan and its critical findings are accepted.

### F1 — Correctness and secret safety

Status: complete locally on 2026-07-23.

- [x] Add tenant/product columns and indexes in a new PostgreSQL migration.
- [x] Seed tenant/product from the source hand scope.
- [x] Introduce `ContextKey` and use it for state and repository lookup.
- [x] Add cross-tenant collision tests using the same player ID.
- [x] Remove username/password from `JobConfig` and serialized operator fields.
- [x] Resolve credentials through a TaskManager-side provider in `open()`.
- [x] Classify JDBC errors and fail on infrastructure/configuration errors.
- [x] Minimize diagnostic/DLQ payloads and sanitize error messages.

Exit gate: no cross-tenant lookup is possible; no JDBC password appears in
safe config summaries, rendered arguments, serialized job inspection, or
logs; outage tests do not advance Kafka work silently.

Validation evidence:

- migration `004_scope_user_context.sql` applied twice successfully and
  backfilled six existing rows to `demo/poker`;
- the database primary and effective-time keys include tenant, product, and
  user;
- the scoped seeder wrote exactly six contexts from the source hand scope;
- 23 Java tests passed, including the real PostgreSQL integration test;
- 20 relevant Python contract/seed tests passed;
- the shaded JAR contains and explicitly loads the PostgreSQL driver under
  Flink's child-first TaskManager classloader; and
- a bounded local Kafka -> Flink -> PostgreSQL run emitted exactly six
  matched rows, no duplicates, and zero DLQ records.

Migration guard: the newly scoped context record ID changes lineage relative
to records produced before F1. Publishing the new topology into the old v1
output topic caused the deterministic collision checker to reject the mixed
history. F1 was therefore verified on fresh migration-only topics. Do not
deploy it onto the old v1 output; F2 must provide the versioned output
contract/topic before canonical cutover.

The local `poker.synthetic.hand-player-context.v1` topic now contains both
pre-F1 and F1 records from that detection run and is not a clean verification
source. It was retained rather than destructively recreated. The accepted F1
evidence is on `poker.synthetic.hand-player-context.f1.v1`; its paired
`poker.synthetic.pipeline.dead-letter.f1.v1` topic has zero records.

### F2 — Contract and package separation

Status: in progress locally on 2026-07-23.

- [x] Define enriched player-context v2 and its compatibility policy.
- [x] Replace synthetic Kafka context envelopes with explicit JDBC lineage.
- [ ] Create the remaining target domain/port/adapter package boundaries.
- [ ] Extract configuration parsing from topology construction.
- [x] Create `ActiveContextEnrichmentJob`.
- [x] Move the Kafka temporal join behind `LegacyKafkaTemporalContextJob`.
- [x] Give canonical and legacy operators separate UIDs, groups, and outputs.
- [x] Add an explicit v1/v2 adapter to the pair-feature job and offline oracle.

Exit gate: the canonical job has no context Kafka source, and its domain tests
run without Flink or PostgreSQL.

Validation evidence:

- schema-v2 Pydantic and JSON Schema fixtures reject legacy temporal-join
  fields and inconsistent PostgreSQL lineage;
- the canonical JDBC event builder is deterministic across different cache
  load times;
- Java unit/package tests and the targeted Python v1/v2 contract tests pass;
- a bounded local hand -> JDBC context-v2 run emitted exactly six matched
  player rows with no duplicates;
- the schema-v2 pair adapter emitted exactly 15 feature snapshots for the
  six-player hand and matched the independent Python offline oracle; and
- `poker.synthetic.pipeline.dead-letter.f2.v1` remained at zero offsets on
  all three partitions.

The isolated accepted evidence topics are
`poker.synthetic.hand-player-context.v2` and
`poker.synthetic.pair-features.context-v2.v1`. They were retained for audit.
No SPCS service was changed.

### F3 — Robust synchronous repository

Status: blocked on F2.

1. Add connection validation/reconnect and strict timeouts.
2. Add prepared-query and SQLSTATE tests.
3. Add one jittered transient retry.
4. Configure Flink failure-rate restart/backoff.
5. Add lookup latency, result, retry, reconnect, and failure metrics.
6. Document the connection budget per parallelism.

Exit gate: connection reset, slow query, database outage, bad credentials, and
missing-context scenarios produce the declared behavior.

### F4 — Typed Flink state and recovery

Status: blocked on F2.

1. Replace JSON state with `ActiveContextCacheEntry`.
2. Declare state schema and serializer versions.
3. Add stable operator UIDs.
4. Test 36-hour inactivity TTL and 60-minute refresh with a fake clock.
5. Test late hands against current and prior context versions.
6. Test checkpoint and savepoint restore with the same build.
7. Test and document behavior for a state-schema upgrade.

Exit gate: restart/restore preserves correct context and no legacy-join
savepoint is accepted by the canonical topology.

### F5 — SPCS network, secrets, and declarative deployment

Status: blocked on F1 and F3.

1. Add the PostgreSQL network rule, EAI, and Secret creation templates.
2. Mount the context Secret in the TaskManager spec.
3. Configure `FLINK_CONTEXT_SOURCE=jdbc` as canonical.
4. Remove the context topic from the canonical spec.
5. Refactor EAI deployment to accept and reconcile a full list.
6. Add the canonical service catalog and rendered-spec secret tests.
7. Complete the `_SIM` to canonical naming/topic changes in R2.

Exit gate: a dry run shows exact resources; the rendered spec contains no
credential value; a read-only service inspection matches the catalog.

### F6 — Shadow, load, and chaos validation

Status: blocked on F3–F5.

Run the canonical JDBC job with a new consumer group and output topic. Keep
the legacy path only as a temporary comparison/rollback job.

Test datasets:

1. one hand, six active players, fifteen pairs;
2. repeated hands for A–F to prove cache hits;
3. player G joining later to prove one new lazy load;
4. same player ID in two tenants to prove isolation;
5. current, prior, and future context versions;
6. context missing and invalid rows;
7. 10,000 unique players arriving over 5, 15, and 60 minutes; and
8. sustained hands after cache warmup.

Chaos tests:

- PostgreSQL unavailable;
- delayed queries;
- connection reset;
- password rotation/restart;
- Kafka restart/rebalance; and
- Flink checkpoint restore.

Compare player rows, pair vectors, scores, lineage, DLQ/quarantine records,
consumer lag, state behavior, and database load.

Exit gate: all synchronous performance gates in section 9 pass, or F8 async is
completed before cutover.

### F7 — Canonical cutover and legacy retirement

Status: blocked on F6.

1. Record image digests, specs, consumer offsets, savepoints, and rollback
   commands.
2. Cut downstream consumers to the v2 canonical output.
3. Observe the agreed POC window.
4. Exercise rollback using the recorded legacy group/spec.
5. Remove the legacy job from the `POKER_FLINK` deployment.
6. Remove its code only after the retention window and explicit approval.

Exit gate: `POKER_FLINK` runs hands-only JDBC context enrichment and pair
features; no deployed job consumes the context Kafka topic.

### F8 — Conditional async lookup

Status: conditional, not scheduled.

Implement only when F6 or production metrics breach the section 9 gates.
Repeat recovery, ordering, timeout, load, and chaos tests before enabling it.

## 12. Verification matrix

| Layer | Required checks |
|---|---|
| Domain unit | key equality, effective-time selection, freshness, error classification |
| Contract unit | v2 schema, sensitive-field allowlist, deterministic lineage |
| Repository integration | real PostgreSQL index/query, timeout, reconnect, tenant isolation |
| Flink operator | miss, hit, refresh, TTL expiry, late hand, missing context |
| Flink recovery | checkpoint, savepoint, rebalance, serializer compatibility |
| End to end | 6 player rows, 15 pair vectors, scores, evidence, zero unexpected DLQ |
| Load | 10k cold players under burst shapes, warm steady state, DB connection budget |
| Chaos | DB/Kafka interruption, password rotation, slow/reset connection |
| Security | no secrets or unnecessary raw hand/context in job graph, logs, state, or DLQ |
| Deployment | rendered spec, complete EAI reconciliation, actual-vs-declared service diff |

## 13. Operational metrics and alerts

Expose at least:

- `context_cache_hit_total`;
- `context_cache_miss_total`;
- `context_cache_refresh_total`;
- `context_lookup_not_found_total`;
- `context_lookup_retry_total`;
- `context_lookup_failure_total`;
- `context_lookup_latency_ms`;
- `context_connection_reconnect_total`;
- enriched and quarantined row counts;
- Flink checkpoint duration/failure;
- operator busy/backpressured time; and
- Kafka consumer lag.

Alert on:

- database authentication/schema errors immediately;
- sustained lookup failure or not-found rate;
- cache hit ratio below the measured baseline;
- consumer lag that does not drain;
- repeated Flink restart loops;
- checkpoint failure; and
- actual service configuration drifting from the service catalog.

## 14. Explicitly deferred work

Do not include these in the current refactor:

- Redis;
- a `POKER_CONTEXT` microservice;
- full user-table loading;
- a user-context Kafka bootstrap;
- Rust migration;
- changing CatBoost/rules/GNN model scope;
- removing remote `_SIM` services;
- deleting old topics or savepoints; or
- using the ML cache as an account-authorization source.

## 15. Immediate implementation slice

Finish F2 by moving configuration, domain records, repository ports, JDBC
adapters, Flink operators, and event-contract builders into explicit packages.
Keep the compatibility dispatcher only until deployment and runbook callers
use the two explicit entrypoints. Re-run the same bounded v2 evidence after
the move, then begin F3 repository reconnect/retry work.

Do not start SPCS mutation until F1–F5 local and rendered-spec gates pass.

## 16. Official references

- [Flink stateful stream processing](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)
- [Flink state TTL](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/state/)
- [Flink asynchronous I/O](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/operators/asyncio/)
- [Snowflake CREATE SERVICE](https://docs.snowflake.com/en/sql-reference/sql/create-service)
- [Snowflake ALTER SERVICE](https://docs.snowflake.com/en/sql-reference/sql/alter-service)
- [SPCS service networking](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/service-network-communications)
- [SPCS secrets and service specifications](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-services)
- [SPCS guidelines and limits](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/spcs-guidelines-and-limitations)
