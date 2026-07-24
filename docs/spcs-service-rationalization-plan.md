# SPCS service rationalization and canonical naming plan

## 1. Decision

There is one simulator:

- `POKERKIT_SIMULATOR`

The following are real pipeline services, even while the project is a proof of
concept:

- `POKER_ADAPTER`;
- `POKER_FLINK`; and
- `POKER_RISK`.

The POC/synthetic distinction belongs in data-plane configuration, Kafka
topics, dataset lineage, consumer groups, credentials, and the Snowflake
environment. It does not belong in the service name.

Canonical POC resources:

- topic prefix: `poker.synthetic.*`;
- adapter EAI: `POKER_ADAPTER_KAFKA_EAI`; and
- adapter secret: `KAFKA_ADAPTER_CREDENTIALS`.

The same application service names and images will be used later. If POC and
production must run concurrently, isolate them by Snowflake account,
database/schema, compute pool, credentials, topics, and state—not by adding
`_SIM` to the component name.

Example:

```text
POKER_POC.SPCS.POKER_FLINK
POKER_PROD.SPCS.POKER_FLINK
```

## 2. Objective

Consolidate the duplicate `_SIM` deployment into one canonical service stack,
remove historical SPCS job clutter, retire the legacy Python monolith after its
remaining persistence responsibility is replaced, and create a clear path
from synthetic POC data to future poker-server data.

This plan does not authorize deleting a remote service, Kafka topic,
checkpoint, model, stage artifact, block snapshot, or Snowflake table.

### Implementation progress — 2026-07-23

- [x] Decision changed from full context-topic bootstrap to hand-driven lazy
  PostgreSQL lookup.
- [x] PostgreSQL `public.poker_user_context` migration added for versioned,
  point-in-time lookup.
- [x] PokerKit-hand context seeding command added for the local POC.
- [x] Java/Flink JDBC context-source mode added behind
  `FLINK_CONTEXT_SOURCE=jdbc`.
- [x] Active-user `ValueState` uses 36-hour read/write TTL and a separate
  60-minute refresh interval.
- [x] Lookup failures and missing contexts go to the pipeline DLQ.
- [x] Local Java unit, H2 JDBC, and real PostgreSQL lookup contract tests pass.
- [x] One PokerKit hand seeds exactly its six users into PostgreSQL.
- [x] Local bounded Kafka/Flink/JDBC run emits six matched player-context rows
  and zero DLQs.
- [x] Production-shape architectural review and staged context/Flink
  refactoring plan completed in
  [`active-user-context-refactoring-plan.md`](active-user-context-refactoring-plan.md).
- [x] Context refactoring F1 completed locally: tenant/product/player
  isolation, TaskManager-only JDBC credentials, classified failures,
  minimized diagnostics, and bounded shaded-JAR validation.
- [x] Context refactoring F2 completed locally: explicit
  PostgreSQL lineage in `poker.hand-player-context.v2`, isolated canonical and
  legacy entrypoints, schema-v2 pair-feature compatibility, and explicit
  config/domain/port/JDBC/contract/Flink package boundaries.
- [x] A bounded v2 run emitted six context rows and fifteen pair snapshots,
  passed offline/online parity, and produced zero F2 DLQ records.
- [x] The post-extraction bounded replay again produced six context rows and
  fifteen parity-checked pair snapshots with an empty DLQ.
- [x] Context refactoring F3 completed locally: connection validation,
  reconnect, bounded transient retry, strict timeouts, lookup metrics, and
  Flink failure-rate restart/backoff.
- [x] The F3 success replay produced six context rows and fifteen
  parity-checked pair snapshots with an empty DLQ. A controlled PostgreSQL
  outage stopped after the configured restart limit without advancing Kafka
  or writing an enriched/DLQ record.
- [x] Context refactoring F4 completed locally: versioned typed Flink state,
  fake-clock refresh/TTL tests, late-hand cache protection, and durable
  checkpoint/savepoint recovery.
- [x] An exact-build restore produced six cache hits and zero lookups; a
  canonical restore from a legacy-topology savepoint was rejected by the
  isolated operator UID namespaces.
- [x] The clean F4 replay produced six deterministic context rows and fifteen
  parity-checked pair snapshots with an empty DLQ.
- [x] Canonical context storage moved inside Snowflake; the SPCS service-token
  Python sidecar, Java localhost adapter, history table seeder, and
  Secret-free rendered spec exist. JDBC was removed from this boundary because
  SPCS service-token SQL does not support the JDBC driver.
- [x] Canonical `POKER_FLINK` SPCS cutover is live on immutable image
  `c58b05dd3c4c`; all four containers are ready and the six-context/fifteen-pair
  smoke test passes.

## 3. Current baseline and target decision

The current baseline is from the last verified state on 2026-07-22. Refresh it
with MFA before any mutation.

| Current object | Current role | Target |
|---|---|---|
| `POKER_ADMIN` | Streamlit UI | Keep; eventually on-demand |
| `POKER_REALTIME` | Legacy Python ingest/feature/score/persistence monolith | Replace and remove after parity |
| `POKER_ADAPTER_SIM` | Real Go adapter restricted to old `poker.sim.*` topics | Replace with `POKER_ADAPTER` |
| `POKER_FLINK` | Real Java/Flink service | Keep and configure for `poker.synthetic.*` during POC |
| `POKER_RISK` | Real Go/Triton scoring service | Keep and configure for `poker.synthetic.*` during POC |
| `POKER_FLINK_SIM` | Duplicate Flink service for isolated test topics | Remove after canonical cutover |
| `POKER_RISK_SIM` | Duplicate scoring service for isolated test topics | Remove after canonical cutover |
| `POKER_FLINK_SAVEPOINT_*` | Five historical job objects | Archive evidence, then remove objects |
| `POKER_TRAIN_JOB` | Completed historical job object | Verify artifacts, then remove object |

Current object count:

- 13 service/job objects;
- 5 long-running services;
- 2 suspended duplicate downstream services; and
- 6 completed or failed historical jobs.

Target persistent service set:

1. `POKER_ADAPTER`;
2. `POKER_FLINK`;
3. `POKER_RISK`;
4. `POKER_SINK` after it is implemented; and
5. `POKER_ADMIN`.

Training and savepoint controllers remain ephemeral job services, not
persistent applications.

## 4. Canonical synthetic data flow

```text
                         POKERKIT_SIMULATOR
                         seed + run manifest
                          /                 \
                         v                   v
               Snowflake context     PostgreSQL hand history
                history table                 |
                         |             outbox game filter
                         |                     |
                         |                 Debezium
                         |                     |
                         |      poker.synthetic.cdc-hand-outbox.v1
                         |                     |
                         |             POKER_ADAPTER (Go)
                         |                     |
                         |      poker.synthetic.hands.raw.v1
                         |                     |
                         +--- internal lookup -+
                                               |
                                  POKER_FLINK (Java/Flink)
                              active-player context TTL state
                                                |
                      poker.synthetic.pair-features.context-v2.v1
                                                |
                                  POKER_RISK (Go + Triton)
                                  model + rules + policy
                                                |
                 +-------------------------+-------------------------+
                 v                         v                         v
        risk scores/evidence       review decisions              alerts
                 \_________________________|_________________________/
                                           |
                                     POKER_SINK
                                           |
                                      Snowflake tables
                                           |
                                      POKER_ADMIN
```

`POKERKIT_SIMULATOR` runs locally or in a client-side test environment.
`POKER_ADAPTER`, `POKER_FLINK`, and `POKER_RISK` are the real services running
in SPCS. Only hand data crosses Kafka into the selected online path. User
context is loaded lazily from the internal Snowflake history table.

## 5. Canonical synthetic topics

| Contract | Topic |
|---|---|
| Raw Debezium outbox | `poker.synthetic.cdc-hand-outbox.v1` |
| Canonical completed hands | `poker.synthetic.hands.raw.v1` |
| Enriched player-hand context | `poker.synthetic.hand-player-context.v2` |
| Pair features | `poker.synthetic.pair-features.context-v2.v1` |
| Risk scores | `poker.synthetic.risk-scores.v1` |
| Rule evidence | `poker.synthetic.rule-evidence.v1` |
| Review decisions | `poker.synthetic.review-decisions.v1` |
| Risk alerts | `poker.synthetic.risk-alerts.v1` |
| Pipeline DLQ | `poker.synthetic.pipeline.dead-letter.v1` |

Recommended synthetic consumer groups:

- `poker-adapter-synthetic-v1`;
- `flink-active-context-synthetic-v2` for hands only;
- `flink-pair-features-context-synthetic-v2`;
- `poker-risk-scorer-synthetic-v1`; and
- `poker-sink-synthetic-v1`.

The runtime should receive an explicit configuration such as:

```text
PIPELINE_DATA_PLANE=synthetic
PIPELINE_TOPIC_PREFIX=poker.synthetic.
```

Java and Go must fail closed when a configured topic does not match the
selected data plane. Replace `simulation_mode` terminology with
`data_plane=synthetic`; the services are not simulators.

## 6. How active-player user context is generated and loaded

PokerKit generates valid gameplay but does not natively know account, device,
KYC, bankroll, acquisition, or network information. The
`POKERKIT_SIMULATOR` wrapper creates deterministic context rows for the same
player IDs and stores them in
`POKER_ML_DEMO.SPCS.POKER_USER_CONTEXT_HISTORY`.

No full-table bootstrap is performed. `POKER_FLINK` begins with an empty
context cache.

### 6.1 Lazy lookup

When the first hand containing players A, B, C, D, E, and F arrives:

1. expand the hand to player-keyed rows;
2. check Flink keyed state for each player;
3. perform a synchronous internal Snowflake lookup only for missing or stale
   players;
4. store the returned narrow context in TTL-managed state; and
5. enrich the hand and continue downstream.

Later hands for the same active players use local keyed state. A new player G
causes only G to be loaded.

The point-in-time query selects the latest version satisfying:

```text
user_id = player_id
effective_at <= hand.played_at
```

### 6.2 Retention and freshness

The first implementation uses two independent clocks:

- residency TTL: 36 hours after the most recent state read or write; and
- refresh interval: 60 minutes after the row was loaded.

An active player therefore remains in state while playing, but the Snowflake
row is periodically refreshed. Flink uses `NeverReturnExpired`; physical
RocksDB cleanup may occur later.

### 6.3 Failure policy

The JDBC lookup is synchronous in the first POC because only about 10,000
unique players are expected per day. It must use a persistent connection,
indexed point lookup, strict query timeout, bounded retry, and lookup metrics.

A missing row produces a minimized data-quality DLQ record. Transient
infrastructure failures fail the operator so Flink restarts from its
checkpoint without advancing the Kafka offset. The pipeline must not fabricate
context or score the incomplete player-hand row. If synchronous lookup latency
creates Kafka backpressure, replace the lookup implementation with Flink Async
I/O without changing the hand or enriched event contracts.

### 6.4 Generated and production sources

For the POC:

- hands: PokerKit -> Kafka for the first live smoke; the CDC simulator remains
  available for end-to-end tests;
- user context: PokerKit wrapper -> Snowflake history table -> lazy internal
  Flink lookup.

For future production:

- hands: poker server -> PostgreSQL -> Debezium -> Kafka;
- user context: account/device systems -> governed Snowflake history table ->
  lazy Flink lookup.

Only the hand-history CDC stream crosses from the poker environment into the
ML platform. Snowflake context access stays on Snowflake's internal network
and uses the SPCS service identity, so it needs neither an external network
rule/EAI nor a database password Secret.

## 7. Target service responsibilities

| Service | Runtime | Responsibility |
|---|---|---|
| `POKER_ADAPTER` | Go | Validate CDC lineage/checksum, parse binary hand data, publish canonical hands |
| `POKER_FLINK` | Java/Flink | Context join, event time, rolling state, six-player expansion, 15 pair vectors, stateful evidence |
| `POKER_RISK` | Go + Triton | CatBoost inference, Rules v2, review policy, risk outputs |
| `POKER_SINK` | Go initially | Idempotent Kafka-to-Snowflake persistence and replay audit |
| `POKER_ADMIN` | Python/Streamlit | Read-only analyst and operational UI |
| Training job | Python | Reproducible batch training, never a permanent service |

Do not merge these components merely to reduce the service count. They have
different state, scaling, rollout, security, and failure boundaries.

## 8. Why `POKER_REALTIME` cannot be removed immediately

The legacy Python service consumes `hands.raw` and currently writes:

- `RAW_HANDS`;
- `RAW_PLAYERS`;
- `RAW_ACTIONS`;
- `FEATURES`;
- `RULE_FLAGS`; and
- `ALERTS`.

`POKER_ADMIN` reads several of these legacy tables. The canonical
`POKER_FLINK`/`POKER_RISK` path ends in Kafka and does not yet persist every
event-native output into admin-facing Snowflake tables.

Therefore:

> Do not remove `POKER_REALTIME` until `POKER_SINK`, admin migration, and a
> bounded legacy-versus-canonical parity report are accepted.

## 9. Target Snowflake structure

Use the same canonical service names in each environment:

```text
POKER_POC.SPCS.POKER_ADAPTER
POKER_POC.SPCS.POKER_FLINK
POKER_POC.SPCS.POKER_RISK

POKER_PROD.SPCS.POKER_ADAPTER
POKER_PROD.SPCS.POKER_FLINK
POKER_PROD.SPCS.POKER_RISK
```

POC configuration:

- `poker.synthetic.*` topics;
- `synthetic-*` dataset IDs;
- synthetic consumer groups;
- `POKER_ADAPTER_KAFKA_EAI`;
- `KAFKA_ADAPTER_CREDENTIALS`;
- POC state/checkpoint volume; and
- POC compute pool.

Future production configuration:

- production Kafka namespace;
- production dataset lineage;
- production-scoped consumer groups;
- production Secret/EAI;
- production state/checkpoint volume; and
- production compute pool.

The service images and component names remain unchanged.

## 10. Refactoring phases

### R0 — Inventory and freeze names

Status: pending.

1. Refresh Snowflake MFA.
2. Export services, containers, pools, endpoints, image digests, consumer
   groups, lag, stages, and volume references.
3. Freeze the canonical names in this document.
4. Add a read-only service catalog at `infra/snowflake/services.yaml`.
5. Stop creating any new service or spec with `_SIM`.

Exit gate: every remote object has an owner, lifecycle, dependency map, and
target disposition.

### R1 — Low-risk historical cleanup

Status: pending.

1. Export an evidence manifest for each `POKER_FLINK_SAVEPOINT_*` job:
   - final status;
   - image digest;
   - logs;
   - Flink job ID; and
   - savepoint URI, when present.
2. Verify `POKER_TRAIN_JOB` model artifacts and hashes.
3. Add dry-run cleanup commands.
4. After explicit approval, drop the five savepoint job objects and the
   completed training job object.
5. Do not delete savepoints, model files, stage artifacts, snapshots, or
   event-table logs.

Expected count: 13 objects become 7 without changing application behavior.

### R2 — Rename the synthetic data plane in code

Status: in progress.

The detailed correctness, secret handling, package separation, JDBC
reliability, state recovery, and SPCS migration work for item 9 is tracked in
[`active-user-context-refactoring-plan.md`](active-user-context-refactoring-plan.md).
Its F1 correctness/security phase must precede remote cutover work.

1. Rename `poker.sim.*` constants to `poker.synthetic.*`.
2. Rename `ShadowSimulationTopics`/`CdcSimulationTopics` to synthetic
   terminology.
3. Replace `simulation_mode` with an explicit `data_plane=synthetic` guard in
   Python, Java, and Go.
4. Change dataset IDs from `sim-*` to `synthetic-*`.
5. Rename specs:
   - `adapter-sim.yaml.template` -> `adapter.yaml.template`;
   - remove `flink-sim.yaml.template` after cutover; and
   - remove `risk-sim.yaml.template` after cutover.
6. Rename Snowflake integration configuration:
   - `POKER_ADAPTER_SIM_KAFKA_EAI` -> `POKER_ADAPTER_KAFKA_EAI`;
   - `KAFKA_ADAPTER_SIM_CREDENTIALS` ->
     `KAFKA_ADAPTER_CREDENTIALS`.
7. Rename Make targets and documentation from simulation/shadow terminology to
   synthetic POC terminology.
8. Keep temporary compatibility readers only where needed for a bounded
   migration.
9. Make JDBC the canonical `POKER_FLINK` context source:
   - keep the old Kafka context join only as a temporary rollback mode;
   - configure 36-hour active-user TTL and 60-minute freshness refresh;
   - add a read-only PostgreSQL user and indexed point lookup;
   - add latency, hit, miss, refresh, not-found, and failure metrics; and
   - remove the context topic from the canonical hand-driven flow after
     cutover.

Exit gate: local contract, Python, Java, Go, image, and rendered-spec tests pass
with no `poker.sim.*` runtime configuration.

### R3 — Canonical service cutover

Status: blocked on R2.

1. Create the exact `poker.synthetic.*` topics and scoped ACLs.
2. Create `POKER_ADAPTER` from the accepted Go image.
3. Take non-cancelling savepoints and record the existing `POKER_FLINK` state,
   offsets, image digest, and rollback spec.
4. Record the existing `POKER_RISK` group offsets, image digest, model run,
   policy, and rollback spec.
5. Configure the canonical `POKER_FLINK` and `POKER_RISK` services for the
   synthetic topics and new consumer groups.
6. Create a POC-only PostgreSQL network rule, EAI, and read-only Secret for
   `POKER_FLINK`; never embed the JDBC password in a spec or image.
7. Use a POC-specific Flink state volume; do not restore an incompatible
   production-topic savepoint into the synthetic data plane.
8. Run the full bounded flow:
   - `POKERKIT_SIMULATOR`;
   - PostgreSQL/Debezium;
   - `POKER_ADAPTER`;
   - `POKER_FLINK`;
   - `POKER_RISK`; and
   - the offset-bounded verifier.
9. Require:
   - one canonical hand;
   - exactly six lazily loaded user contexts for a six-player first hand;
   - cache hits for the same users on subsequent hands;
   - fifteen online/offline-identical pair vectors;
   - one complete score;
   - intact evidence/decision/alert references; and
   - zero unexpected DLQs.
10. Exercise rollback to the recorded canonical service specs.

Exit gate: the canonical services pass the previously accepted end-to-end
contract on `poker.synthetic.*`.

### R4 — Remove duplicate `_SIM` services

Status: blocked on R3.

1. Suspend `POKER_ADAPTER_SIM`, `POKER_FLINK_SIM`, and `POKER_RISK_SIM`.
2. Export their final specs, image digests, group offsets, logs, state
   references, and accepted run manifests.
3. Observe the canonical services for the agreed POC window.
4. Confirm no active consumer or deployment command references an `_SIM`
   service.
5. Obtain explicit approval.
6. Drop the three duplicate service objects.
7. Deprecate `poker.sim.*` topics. Delete them only under a separate retention
   and data-destruction approval.

Expected count after R1 and R4:

- `POKER_ADMIN`;
- `POKER_REALTIME`;
- `POKER_ADAPTER`;
- `POKER_FLINK`; and
- `POKER_RISK`.

Five persistent objects remain before `POKER_SINK` is added.

### R5 — Add `POKER_SINK` and migrate admin

Status: complete; live exit gate passed on 2026-07-24.

1. [x] Define event-native Snowflake tables for canonical lineage, player
   context, pair revisions, scores, rule evidence, decisions, alerts, and DLQ
   audit.
2. [x] Build `POKER_SINK` as a separate multi-topic Go Kafka consumer with a
   private SPCS-token Snowflake writer sidecar.
3. [x] Use immutable event IDs, event hashes, and projected revisions for
   idempotency and collision detection.
4. [x] Commit offsets only after acknowledged Snowflake persistence.
5. [x] Test replay, collision, writer failure, partial transaction failure,
   schema version, poison sanitization, and the full Go suite.
6. [x] Update `POKER_ADMIN` to read `POKER_ALERT_REVIEW_V`.
7. [x] Keep the legacy reader behind `ADMIN_DATA_MODE=legacy`.
8. [x] Commit the verified source, publish the Git-SHA image, apply
   `infra/snowflake/sql/sink.sql`, and deploy `POKER_SINK`.
9. [x] Reconcile the D7 dataset with `make r5-sink-verify` and deploy the
   canonical admin image.
10. [x] Prove admin freshness while `POKER_REALTIME` is suspended.

Implementation details and failure semantics are in
[`poker-sink.md`](poker-sink.md).

Exit gate passed.

Live evidence, 2026-07-24:

- initial commit `0ae4a9b00930` sink and admin images were published as
  `sha256:bea33e...a5be` and `sha256:9911c6...de23`;
- all sink tables and canonical views were created successfully;
- the compact-JSON identity repair was committed as `904903c5e49d`, published
  as sink digest `sha256:65d7f6...306cf5`, and deployed with spec digest
  `d6c44c6b...f842`;
- `POKER_SINK` pulled the repaired digest and both containers became ready;
- replay halted without committing the blocked record when the same
  deterministic hand event appeared with identical compact JSON but different
  whitespace/raw Kafka bytes;
- the service was suspended without skipping an offset, then resumed from that
  exact record after the repair;
- the repair now uses compact-JSON SHA-256 for immutable event identity and
  retains raw Kafka SHA-256 separately for byte-level lineage; and
- sink lag reached zero on all eight topics without an offset reset;
- sealed dataset `multitable-alert-acceptance-v1` passed with exactly 16 hands,
  96 player contexts, 240 pair features, 16 scores, 176 evidence rows, 16
  decisions, 14 alerts, all required consumer commits, and zero target D7 dead
  letters;
- the sink verifier now treats the passed upstream SPCS
  `schema_v2_lineage` IDs as the runtime handoff contract instead of comparing
  them with older offline pre-enrichment projections;
- `POKER_ADMIN` was deployed in canonical mode with immutable image digest
  `sha256:9911c6...de23` and spec digest `c13529fc...211f`; and
- with `POKER_REALTIME` fully suspended, the admin remained running, returned
  all 14 exact runtime alert IDs, and retained zero sink lag. The legacy service
  was restored to its original running spec `2941d733...249d` with a ready
  container and zero restarts.

### R6 — Retire `POKER_REALTIME`

Status: ready; not started.

1. Dual-run legacy and canonical paths on the same bounded synthetic dataset.
2. Compare inputs, features, scores, decisions, persisted rows, DLQs, lag, and
   latency.
3. Confirm no active producer or consumer depends only on `hands.raw` or
   `alerts.out`.
4. Suspend `POKER_REALTIME` for 24 hours in the POC.
5. Verify admin freshness and canonical lag.
6. Test rollback using the exact immutable legacy image/spec.
7. Obtain explicit approval, then drop `POKER_REALTIME`.
8. Retain its evidence and rollback manifest for the agreed retention window.

Target persistent count after R6 and `POKER_SINK`:

- `POKER_ADAPTER`;
- `POKER_FLINK`;
- `POKER_RISK`;
- `POKER_SINK`; and
- `POKER_ADMIN`.

Four data-plane services run continuously according to SLA; admin may be
on-demand.

### R7 — Separate generic image roles and batch lifecycle

Status: pending.

1. Stop using the generic legacy `poker-pipeline` image for unrelated roles.
2. Maintain separate immutable images for adapter, Flink, risk, sink, admin,
   and training.
3. Run training and savepoint controllers only as ephemeral jobs.
4. Give every image a non-root runtime, minimal dependencies, SBOM,
   vulnerability scan, immutable tag, build identity, and role-specific smoke
   test.
5. Remove `snow-deploy-realtime` only after R6.

## 11. Service catalog

The proposed catalog should express component and data plane independently:

```yaml
services:
  POKER_ADAPTER:
    component: cdc-adapter
    lifecycle: persistent
    data_plane: synthetic
    topic_prefix: poker.synthetic.
    expected_state: running
    public: false
    eai: POKER_ADAPTER_KAFKA_EAI
    secret: KAFKA_ADAPTER_CREDENTIALS

  POKER_FLINK:
    component: stream-features
    lifecycle: persistent
    data_plane: synthetic
    topic_prefix: poker.synthetic.
    expected_state: running
    public: false

  POKER_RISK:
    component: risk-scorer
    lifecycle: persistent
    data_plane: synthetic
    topic_prefix: poker.synthetic.
    expected_state: running
    public: false
```

The reconciler must be read-only by default. Mutations require an explicit
subcommand and exact service names.

## 12. Safety and deletion guards

Any cleanup command must:

1. list exact targets;
2. reject globs and schema-wide deletion;
3. require allowed final states such as `DONE`, `FAILED`, or `SUSPENDED`;
4. export an evidence manifest before mutation;
5. verify stage/savepoint references independently;
6. support `--dry-run`; and
7. never delete Kafka topics, savepoints, block snapshots, model artifacts, or
   Snowflake tables as a side effect.

Before changing a compute-pool maximum, report the node limits, instance-family
credit rate, estimated hourly delta, pending services, expected duration, and
the cleanup action restoring the previous limit.

## 13. Acceptance criteria

- [ ] `POKERKIT_SIMULATOR` is the only component named or described as a
  simulator.
- [ ] canonical services are `POKER_ADAPTER`, `POKER_FLINK`, and `POKER_RISK`.
- [ ] POC topics use `poker.synthetic.*`.
- [ ] synthetic dataset IDs, groups, state, EAI, and Secret are isolated.
- [ ] deterministic PokerKit user context is stored in PostgreSQL before its
  first hand is emitted.
- [ ] no full user-context table bootstrap occurs.
- [ ] only users observed in hands occupy the active Flink context state.
- [ ] inactive user context expires after the configured sliding TTL.
- [ ] the canonical services pass the bounded CDC-to-score verifier.
- [ ] the six historical job objects are archived and removed.
- [ ] duplicate `_SIM` services complete an observation window and are removed
  with approval.
- [ ] `POKER_SINK` persists all modern contracts idempotently.
- [ ] admin no longer depends on new writes from `POKER_REALTIME`.
- [ ] the legacy/canonical parity report passes.
- [ ] `POKER_REALTIME` completes its suspension and rollback gates.
- [ ] every service has an owner, lifecycle, data plane, pool, expected state,
  and runbook.

## 14. Recommended next slice

1. Implement F2 from the
   [active-user context refactoring plan](active-user-context-refactoring-plan.md):
   versioned JDBC lineage/output contract, canonical/legacy entrypoints, and
   package boundaries.
2. Complete R0 inventory after MFA.
3. Implement R1 historical job evidence export and dry-run cleanup.
4. Continue R2 naming/topic/guard changes locally.
5. Execute R3 only after the context plan's F1–F5 local and rendered-spec
   gates pass.
