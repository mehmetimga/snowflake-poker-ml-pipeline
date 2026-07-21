# Phase C1: Go and Flink deployment on Snowpark Container Services

Phase C1 replaces the legacy single Python realtime container with two explicit
production-shaped services:

```text
Confluent Cloud
      |
      v
POKER_FLINK (poker-flink:<git-sha>)
  jobmanager + taskmanager + job supervisor
  context enrichment -> pair features -> stateful rule evidence
      |
      v
POKER_RISK (poker-risk:<git-sha> + pinned Triton sidecar)
  assemble 15 pairs -> CatBoost -> calibration -> review policy
      |
      v
acknowledged Kafka outputs -> future POKER_SINK -> Snowflake tables
```

This phase packages and validates the two services. It does not move the poker
server, PostgreSQL, Debezium, or Confluent into Snowflake. It also does not
change the champion model, decision threshold, rule rollout, or shadow-only
enforcement policy.

## Images

| Image | Contents | Runtime |
|---|---|---|
| `poker-risk:<git-sha>` | Static Go Kafka scorer, governed review policy and rule rollout, runtime contract verifier | Non-root UID/GID `65532`; model read from a Snowflake stage |
| `poker-flink:<git-sha>` | Flink 1.19.1, Java 17, two shaded jobs, Kafka connectors, RocksDB state backend, Prometheus reporter, job supervisor | One session cluster inside `POKER_FLINK` |
| `tritonserver:25.12-py3` | Pinned NVIDIA Triton CPU sidecar mirrored into the Snowflake image repository | Localhost inference inside each `POKER_RISK` instance |

The Go and Flink Dockerfiles use multi-stage builds. Neither contains data,
Kafka credentials, `.env`, or the private challenge. The local development tag
is `dev-<current-head>`. A registry push or SPCS deployment is rejected unless
the worktree is clean and the tag exactly equals the current 12-character Git
SHA.

Risk and Flink image/build revisions are rendered independently through
`C1_RISK_IMAGE_TAG` and `C1_FLINK_IMAGE_TAG`. This matters for a component-only
hotfix: the service-reported build version must describe the image actually
running, even when the other component remains on an earlier reviewed commit.

## Model artifact boundary

`scripts/build_risk_runtime_bundle.py` reads the governed champion manifest,
verifies every selected SHA-256 hash, and builds a minimal seven-file runtime
bundle. Training predictions, SHAP outputs, and other evaluation-only files are
not copied into serving storage.

The deployment command uploads the bundle to:

```text
@POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS/risk/pair_7a1c58c1046b
```

`POKER_RISK` mounts that stage at `/opt/models`. The Go container validates the
runtime manifest before starting. Triton mounts only its nested `triton/`
folder. Runtime configuration therefore uses a governed stage URI rather than
a developer-machine path.

## Flink recovery boundary

`POKER_FLINK` has one 20 GiB Snowflake block volume mounted at
`/opt/flink/state` in the JobManager, TaskManager, and job-supervisor
containers. It contains:

- `/opt/flink/state/checkpoints` for periodic recovery checkpoints;
- `/opt/flink/state/savepoints` for controlled upgrades and rule rollback; and
- a deletion snapshot retained for 30 days if the service is dropped.

The service fixes `MIN_INSTANCES=1` and `MAX_INSTANCES=1`, as required for a
service using block storage. The job supervisor waits for Flink REST, submits
both jobs, avoids duplicate submission after its own restart, and exits if
either required job stops. SPCS then restarts the failed supervisor.

Before an upgrade, create one savepoint per running job and place its URI in
`FLINK_CONTEXT_SAVEPOINT_PATH` or `FLINK_PAIR_SAVEPOINT_PATH` in the rendered
spec. Do not delete the previous image or volume snapshot until replay and
probability-invariance checks pass.

## Readiness and monitoring

All declared endpoints are private:

| Service | Endpoint | Purpose |
|---|---|---|
| `POKER_FLINK` | `8081 /overview` | JobManager readiness and Flink REST |
| `POKER_FLINK` | `9249`, `9250` | JobManager and TaskManager Prometheus metrics |
| `POKER_RISK` | `9091 /healthz` | Go service readiness after Kafka and Triton startup checks |
| `POKER_RISK` | `9091 /metrics` | acknowledged scoring, policy, rule, and lineage metrics |
| `POKER_RISK` | `8002 /metrics` | Triton metrics |

The specs also export service logs and Snowflake platform `system`, `network`,
`storage`, and `status` metric groups to the account event table. Flink exposes
watermarks, Kafka offsets/lag, checkpoint duration/failures, state size, late
events, and rule firings. Go exposes acknowledged hands, pairs, evidence,
rollout enablement, request latency, failures, and policy volume.

## Local validation

```bash
make phase-c1-check \
  MAVEN=/private/tmp/apache-maven-3.9.9/bin/mvn \
  MAVEN_REPO=/private/tmp/codex-m2
make c1-build
make c1-image-smoke
```

The smoke gate runs the amd64 containers even on an ARM Mac. It verifies:

- the Go executable and build version;
- all seven runtime model hashes and the exact 58-feature scoring contract;
- both shaded Flink main classes;
- the RocksDB and Prometheus runtime modules; and
- shell syntax for the Flink job supervisor.

## Release and deployment sequence

Deploy only from a reviewed, clean commit:

```bash
make snow-mfa-login
make snow-bootstrap
make snow-configure-kafka

make c1-build
make c1-image-smoke
make c1-release-check

snow spcs image-registry login
make c1-mirror-triton
make c1-push
make c1-upload-model
make c1-render C1_ALLOWED_TENANTS=tenant-a
make c1-deploy
make snow-status
```

`c1-release-check` deliberately fails in a dirty worktree. Pushing images,
uploading the model, and deploying services are external mutations and are not
part of the local packaging gate.

After deployment, verify service/container status and logs, both Flink jobs,
checkpoint creation, Kafka lag, Go/Triton readiness, a small canonical replay,
acknowledged output counts, deterministic IDs, and zero model probability
delta. Keep all decisions in shadow mode.

`C1_RISK_SCORER_GROUP_ID` is an explicit validated deployment setting. Keep it
stable during normal restarts. Change it only for a controlled cutover or
recovery boundary, because a new group with no committed offsets starts at the
current pair-topic tail. Retain the previous group and its offsets for audit.

## Live C1 evidence — 2026-07-21

The account now runs:

| Component | Immutable release | Live result |
|---|---|---|
| Go risk service | `poker-risk:8807659415f7` | ready; zero input lag on all six partitions; model run `pair_7a1c58c1046b` |
| Flink service | `poker-flink:42fe62acc2d1` | three containers ready; both restored jobs running and checkpointing |
| Triton sidecar | `tritonserver:25.12-py3` | ready; CPU ONNX model loaded |

The pre-upgrade jobs remained running while a private, bounded SPCS controller
created these savepoints without exposing Flink REST publicly:

```text
context file:/opt/flink/state/savepoints/savepoint-b9cfc0-0055eed2478b
pair    file:/opt/flink/state/savepoints/savepoint-3afb63-00d800620756
```

The replacement service restored Kafka split offsets and keyed RocksDB state
into context job `5aa19a1c1043c6078bb5725cf15c3ff3` and pair job
`4a461ee41f659f4da98734b8eaf5f8a8`. Both jobs then completed new periodic
checkpoints. The controller job was private, synchronous, bounded, and left the
source jobs running until the replacement was explicitly deployed.

The first post-restore diagnostic found conflicting records with the same
derived event ID. The SPCS inventory contained only one context job, while a
host process check found an orphan local Flink smoke job connected to the same
Confluent brokers. Stopping that process removed the second producer. Operators
must therefore confirm no local poker Flink process is active before a governed
replay; local integration jobs should use isolated topics.

Validation was strengthened so exact at-least-once retries are counted, while
the same event ID with a different payload always fails. Pair-feature checks
apply the same invariant to their enriched input instead of silently selecting
the last collision. The Go input contract now dead-letters a pair whose
`emitted_at` precedes `occurred_at`. Scoring uses a logical `scored_at` no earlier
than any validated input emission, which preserves causal evidence under small
producer clock skew without changing CatBoost probability.

Final acceptance dataset `spcs-c1-final-8807659` produced:

- 29/29 acknowledged canonical inputs with zero producer duplicates;
- 24 raw and 24 unique player-hand enrichments with zero exact duplicates;
- 60 pair rows for four complete hands and exact offline/online payload parity;
- four scores, four review decisions, and 22 rule-evidence events;
- 15 pair scores and six player scores in every hand score;
- zero broken decision references and zero missing evidence references; and
- zero risk-consumer lag on all six pair-topic partitions.

The final risk build is `8807659415f7`. Intermediate build `ede123f11f5a`
correctly added causal input rejection but did not handle an internally valid
near-future input timestamp; it is retained as immutable incident evidence and
is not the accepted release.

Negative and diagnostic runs are intentionally excluded from acceptance counts:

- a multi-partition accelerated replay exceeded the configured 30-second
  event-time disorder budget and was fail-closed into the DLQ; and
- a future-dated test was rejected because governed evidence cannot be emitted
  before its business event occurs; and
- watermark-only hands used to advance restored event time were not counted as
  accepted target hands or scores.

Phase C1 is complete. The live service remains shadow-only: no model promotion,
decision threshold change, hard rule, or automated enforcement was introduced.

## Source references

- [Snowflake service specification](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/specification-reference)
- [Block storage volumes](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/block-storage-volume)
- [Snowflake stage volumes](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/snowflake-stage-volume)
- [SPCS service networking](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/service-network-communications)
- [SPCS image repositories](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/working-with-registry-repository)
- [Monitoring SPCS services](https://docs.snowflake.com/en/developer-guide/snowpark-container-services/monitoring-services)
- [Flink checkpointing](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/fault-tolerance/checkpointing/)
- [Flink savepoints](https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/ops/state/savepoints/)
