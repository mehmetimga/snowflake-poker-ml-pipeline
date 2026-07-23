# Snowflake container deployment

This directory deploys the first production-shaped slice of the demo to
Snowpark Container Services (SPCS):

```text
local generator -> managed Kafka -> SPCS realtime scorer -> Snowflake tables
                                             |
Snowflake CPU training job -> model stage ----+
                                             |
                                   SPCS Streamlit admin
```

## Current implementation versus target deployment

The legacy demo commands still deploy one Python application image for the
training job, admin service, and `POKER_REALTIME`. Phase C1 now also packages
the production-shaped Go scorer and Java/Flink jobs as separate
`POKER_RISK` and `POKER_FLINK` services. Their images and specs are locally
validated; registry push and live service deployment remain explicit release
operations.

Phase C2 also packages a separate `poker-adapter:<git-sha>` image and private
`POKER_ADAPTER_SIM` spec. It accepts only isolated synthetic CDC envelopes on
`poker.sim.*` topics. It is not a real poker-server or Debezium integration.

That is the current test topology, not the final production boundary. The
target is:

```text
client/source                 external managed             Snowflake ML platform
PokerKit today -----------+                            +--> SPCS POKER_FLINK
poker server + Postgres --+--> Confluent Cloud Kafka --+--> SPCS POKER_RISK
Debezium CDC in future ---+                            +--> SPCS POKER_SINK
                                                        +--> SPCS jobs/admin
                                                        +--> Snowflake data services
```

Target SPCS images are `poker-flink:<git-sha>`, `poker-risk:<git-sha>`,
`poker-adapter:<git-sha>`, `poker-sink:<git-sha>`, `poker-train:<git-sha>`, and
`poker-admin:<git-sha>`. An optional pinned CPU Triton container can run beside
Go in each `POKER_RISK` service instance and be called over localhost. For the
current small CatBoost ONNX model, embedding ONNX in Go is simpler and is the
recommended default.

The physical DGX is not part of the target realtime path. Keep it only as an
optional offline research accelerator. If a later production model requires a
GPU, verify GPU instance-family availability in the Snowflake account and run
Triton on an SPCS GPU compute pool in a supported region.

The current bootstrap uses the `CPU_X64_S` family in AWS `eu-central-2`. GPU
family availability changes by account and region, so do not assume a specific
GPU family is deployable; verify it with
`SHOW COMPUTE POOL INSTANCE FAMILIES`. This deployment runs the small demo
models on CPU.

## Why Kafka is external

SPCS public ingress endpoints support HTTP/HTTPS, not public arbitrary TCP.
Therefore a Kafka broker running inside SPCS cannot expose the Kafka wire
protocol directly to a producer on a laptop. Use a managed Kafka endpoint that
both the local generator and the SPCS consumer can reach. The SPCS consumer
connects outbound through a narrowly scoped External Access Integration.

## 1. Provision suspended infrastructure

```bash
make snow-bootstrap
```

This creates an initially suspended `CPU_X64_S` compute pool, image repository,
service-spec stage, model-artifact stage, and ML-job payload stage. Creating the
pool does not provision nodes; deploying a service or job resumes it.

## 2. Build and push the application image

Install and configure the Snowflake CLI, then authenticate Docker:

```bash
snow spcs image-registry login
make snow-build
make snow-push
```

SPCS currently requires `linux/amd64`. `.dockerignore` excludes `.env`, local
data, models, Git metadata, and the virtual environment from the image context.

## 3. Configure managed Kafka securely

Put the managed broker addresses and SASL credentials in the existing ignored
`.env` file, or export them only in your shell:

```bash
export KAFKA_BOOTSTRAP_SERVERS='broker-1.example.com:9092,broker-2.example.com:9092'
# Include every hostname advertised in Kafka metadata when it differs from the
# bootstrap hostname (as it does for Confluent Cloud).
export KAFKA_EGRESS_BROKERS='broker-1.example.com:9092,broker-2.example.com:9092'
export KAFKA_SASL_USERNAME='...'
export KAFKA_SASL_PASSWORD='...'
make snow-configure-kafka
```

The credentials are written to a Snowflake password Secret. They are not
written to YAML, `.env`, or the container image.

The command also creates the dedicated `POKER_FLINK_KAFKA_EAI` used by the
canonical Flink service. Risk/realtime services retain `POKER_KAFKA_EAI`, so
each service's complete egress set can be reconciled independently.

The C2 simulation adapter uses a separate Secret, network rule, and EAI. Prefer
a Confluent principal restricted to the three `poker.sim.*` topics and the
`poker-go-hand-adapter-sim-v1` consumer group:

```bash
export KAFKA_ADAPTER_SIM_SASL_USERNAME='...'
export KAFKA_ADAPTER_SIM_SASL_PASSWORD='...'
make c2-adapter-sim-topics
make c2-adapter-configure-kafka
```

For a bounded demo, shared Kafka credentials require the explicit
`C2_ADAPTER_KAFKA_CONFIG_FLAGS=--allow-shared-credentials` override.

## 4. Seed the internal Snowflake user-context projection

The canonical `POKER_FLINK` service consumes only hand events from Kafka. On a
cache miss, its TaskManager lazily reads the active player's point-in-time
context from `POKER_ML_DEMO.SPCS.POKER_USER_CONTEXT_HISTORY`. No context topic,
external PostgreSQL connection, Redis service, context EAI, or database
password Secret is required.

Create the history table, current view, and a deterministic PokerKit-backed
test dataset:

```bash
make snow-seed-user-context
```

The generated hand fixture is written to
`data/runs/spcs-snowflake-context-v1/hands.jsonl`. Every player referenced by
that fixture has a matching versioned context row effective before the hand.

Inside SPCS, Snowflake automatically mounts a rotating service OAuth token at
`/snowflake/session/token` and supplies the account host. The Flink
TaskManager rereads that token whenever it creates a JDBC connection and
executes SQL as the `POKER_FLINK` service owner. This is internal Snowflake
traffic and does not require an External Access Integration.

Run the complete local/rendered deployment gate before a live service update:

```bash
make phase-f5-check
```

[`services.yaml`](services.yaml) is the deployment source of truth. For
`POKER_FLINK` it declares only `POKER_FLINK_KAFKA_EAI`; the submitter is the
only container that receives the Kafka Secret. Deployment replaces the full
EAI set, updates the service spec, reads it back with `DESCRIBE SERVICE`, and
fails if the effective EAI or Secret references differ from the catalog.

## 5. Render and deploy

```bash
make snow-render
make snow-validate-catalog
make snow-deploy-admin
make snow-deploy-realtime
make snow-status
```

For the immutable Flink release, use `make c1-deploy-flink` after its exact
Git-SHA image has been built, pushed, and rendered. Then perform a read-only
catalog comparison:

```bash
make snow-inspect-flink
```

Snowflake rotates the mounted service token. The connection factory rereads
the token file on reconnect, so no user-managed Snowflake credential rotation
is needed in the container.

The realtime service mounts `MODEL_ARTIFACTS` at `/opt/models`, consumes
`hands.raw`, computes features/rules/scores, and persists history and alerts.
The admin service exposes Streamlit through a Snowflake-authenticated public
HTTP endpoint.

The public admin endpoint keeps its service active. Suspend it when you are not
using the demo so the compute pool can auto-suspend after five minutes:

```bash
make snow-suspend-admin
make snow-resume-admin  # when needed again
```

## 6. Run CPU training

After loading the frozen train/validation/test sets:

```bash
make dataset
make load-dataset
make snow-train
```

The CPU job trains XGBoost, CatBoost, and LightGBM, exports ONNX, runs batch
scoring, and uploads model artifacts to
`@POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS`. DL, GNN, and the PyTorch meta-learner are
excluded from this phase. Restart the realtime service after a completed
training run so it reloads the updated model files.

The frozen challenge stream is never loaded for training. Once the realtime
service is connected to managed Kafka, replay it from the laptop and evaluate
the persisted alerts:

```bash
make replay-challenge REPLAY_RATE=25
make evaluate-challenge
```

## Phase two

- Deploy Qdrant with an SPCS block volume and private HTTP endpoint.
- Register trained models in Snowflake Model Registry and move orchestration
  from the generic SPCS training job to Snowflake ML Jobs/Tasks.
- Move training to a GPU-capable Snowflake region, build a CUDA runtime image,
  and enable `scripts/train.py --profile full` for DL, GNN, and meta-learning.

## Simulation adapter

The simulation adapter is built and rendered separately:

```bash
make phase-c2-packaging-check
make c2-adapter-build
make c2-adapter-image-smoke
make c2-adapter-render
```

After releasing and deploying the private service, run the offset-bounded
local-Debezium to Confluent/SPCS replay with `make c2-adapter-remote-e2e`.

Push and `POKER_ADAPTER_SIM` deployment are guarded release operations. See
[`docs/spcs-c2-adapter-simulation.md`](../../docs/spcs-c2-adapter-simulation.md).
