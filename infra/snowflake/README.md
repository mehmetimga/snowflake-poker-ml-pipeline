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

The current Snowflake account is in AWS `eu-central-2`. CPU container families
are available there, but the GPU families used by the existing SageMaker design
are not. This deployment therefore runs the small demo models on CPU.

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

## 4. Render and deploy

```bash
make snow-render
make snow-deploy-admin
make snow-deploy-realtime
make snow-status
```

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

## 5. Run CPU training

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
- Package the PyFlink jobs with Java and the Kafka connector JAR.
- Register trained models in Snowflake Model Registry and move orchestration
  from the generic SPCS training job to Snowflake ML Jobs/Tasks.
- Move training to a GPU-capable Snowflake region, build a CUDA runtime image,
  and enable `scripts/train.py --profile full` for DL, GNN, and meta-learning.
