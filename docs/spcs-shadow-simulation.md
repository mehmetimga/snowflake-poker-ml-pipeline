# Isolated CDC-to-score SPCS shadow simulation

## Purpose

This phase extends the accepted synthetic CDC adapter through the real Java
Flink feature jobs and the real Go/CatBoost/Triton scorer without reading or
writing any production Kafka topic. It validates transport, event-time joins,
feature parity, model serving, rules, policy routing, lineage, and replay as one
bounded system. It does not claim real-data model quality and cannot enforce a
user action.

## Deployment boundary

```text
Local test boundary                         Snowflake SPCS / managed Kafka

PokerKit -> PostgreSQL -> Debezium ------> poker.sim.cdc-hand-outbox.v1
                                                   |
                                                   v
                                      POKER_ADAPTER_SIM (Go)
                                                   |
                                                   v
                                           poker.sim.hands.raw.v1
                                                   |
deterministic user context ----------------> poker.sim.user-context.v1
                                                   |
                                                   v
                                      POKER_FLINK_SIM (Java/Flink)
                                        context join + pair features
                                                   |
                                                   v
                                        poker.sim.pair-features.v1
                                                   |
                                                   v
                                      POKER_RISK_SIM (Go + Triton)
                                      CatBoost + Rules v2 + policy
                                                   |
                     +-----------------------------+------------------------+
                     v                 v                v                   v
             risk-scores       rule-evidence    review-decisions      risk-alerts
                       (every topic is prefixed with poker.sim.)
```

Only PostgreSQL, Debezium, and the PokerKit writer run locally. The adapter,
Flink jobs, Go scorer, and Triton server run in separate private SPCS services.
`POKER_FLINK` and `POKER_RISK` remain on their existing production-shaped
topics and images; this phase does not alter those services.

## Exact topic and state isolation

The shadow services use:

| Role | Topic |
|---|---|
| Debezium source | `poker.sim.cdc-hand-outbox.v1` |
| Canonical hand | `poker.sim.hands.raw.v1` |
| User context | `poker.sim.user-context.v1` |
| Enriched hand/player | `poker.sim.hand-player-context.v1` |
| Pair features | `poker.sim.pair-features.v1` |
| Risk score | `poker.sim.risk-scores.v1` |
| Rule evidence | `poker.sim.rule-evidence.v1` |
| Review decision | `poker.sim.review-decisions.v1` |
| Optional alert | `poker.sim.risk-alerts.v1` |
| Shared simulation DLQ | `poker.sim.pipeline.dead-letter.v1` |

The Flink base groups are `flink-context-enrichment-sim-v1` and
`flink-pair-features-sim-v1`; the scorer group is
`poker-go-risk-scorer-sim-v1`. `POKER_FLINK_SIM` owns a separate block volume,
so it never restores or mutates `POKER_FLINK` checkpoints or savepoints.

Both specs use the simulation-only `POKER_ADAPTER_SIM_KAFKA_EAI` and
`KAFKA_ADAPTER_SIM_CREDENTIALS` Snowflake objects. The accepted demo currently
stores the shared Confluent principal in that separate secret. Replace it with
a topic/group-scoped principal before treating the boundary as production
least privilege.

## Fail-closed controls

Java and Go validate the boundary in process, not only in YAML:

- simulation mode accepts only the exact topic and consumer-group names above;
- normal mode rejects every `poker.sim.*` topic;
- simulation events whose `dataset_id` does not start with `sim-` go to the
  simulation DLQ and cannot be scored;
- the SPCS specs contain no production topic and have no public endpoint;
- release/deployment targets require a clean worktree and an image tag equal to
  the current 12-character Git SHA; and
- model probability, threshold, review policy, and rule rollout remain the
  existing governed artifacts.

## Bounded test data and watermark strategy

`make shadow-sim-e2e` creates exactly one eligible cash hand in the retained
local PostgreSQL simulator. The replay reads the actual Debezium record, not a
reconstructed envelope. Before publishing that source record to managed
Kafka, it publishes deterministic inference-safe context for the six players.

The verifier needs event-time output promptly even though the deployed jobs
are unbounded. The replay therefore publishes explicit simulation-only
watermark records:

- one unused context event per context-topic partition; and
- one direct canonical watermark hand two event-time minutes after the CDC
  target, in the same canonical partition.

The manifest labels these records and the verifier excludes them from target
counts. The simulation Flink source-idleness timeout is five seconds, while
the production-shaped service retains its normal value. No wall-clock sleep is
used as correctness evidence.

## Verification contract

The run manifest records input/output offset bounds, source digests, target
hand and player IDs, context event IDs, watermark positions, consumer groups,
and immutable adapter/Flink/risk build versions. By default the adapter lineage
is the already deployed accepted release `7ef0e7dd16d5`; override
`C2_SHADOW_ADAPTER_BUILD_VERSION` only after intentionally replacing that
service. The verifier waits for the
adapter source commit and then requires, for the target hand:

- exactly one canonical CDC hand with matching Kafka lineage;
- exactly six context-matched player rows tied to this run's context IDs;
- exactly fifteen unique pair rows with Python/Java offline-online payload
  parity;
- exactly one complete score containing fifteen pairs and six players;
- exactly one review decision referencing that score;
- complete score/decision references to every emitted Rules v2 evidence event;
- zero or one alert according to the governed score decision, with an intact
  decision reference; and
- zero simulation DLQ records inside the run's output bounds.

Old topic records cannot satisfy a new run because reads begin at the captured
per-partition offsets and target identity must match the manifest.

## Commands

Run local contracts and package checks:

```bash
make phase-c2-shadow-packaging-check
```

After committing and pushing the clean release:

```bash
make shadow-sim-topics
make c1-build
make c1-push
make shadow-sim-deploy
make shadow-sim-e2e
```

`make shadow-sim-deploy` creates or updates only `POKER_FLINK_SIM` and
`POKER_RISK_SIM`. It does not deploy the existing `POKER_FLINK` or `POKER_RISK`
services.

## Current status

The topic definitions, SPCS specifications, runtime guards, deterministic
context/watermark publisher, offset-bounded verifier, and packaging tests are
implemented locally. Remote topic creation, immutable image release, service
deployment, and the first accepted full-shadow run remain pending a clean
committed and pushed revision.
