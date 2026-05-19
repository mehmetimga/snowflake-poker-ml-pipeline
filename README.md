# snowflake-poker-ml-pipeline

End-to-end demo ML pipeline for detecting collusion in synthetic No-Limit Hold'em hand data. Runs locally on a laptop with DuckDB, against Snowflake with one env var, or in AWS with MSK/ECS/Qdrant/SageMaker infrastructure.

## Architecture

![Snowflake Poker ML Pipeline architecture](docs/pipeline.png)

## What's in here

- **Synthetic hand generator** — 6-max NLHE cash hands with injected collusion patterns (soft-play, chip-dump, squeeze-collude, fold-benefit).
- **Kafka stream** — hands published to `hands.raw`; consumer batches them into the warehouse.
- **Warehouse** — Snowflake or DuckDB (toggle with `WAREHOUSE_BACKEND`). Same SQL migrations run on both.
- **Feature engineering** — ~60 numeric features per `(hand_id, player_id)` from raw actions.
- **Rule engine** — 5 simple Python rules ported from the original Rust engine.
- **Classical ML** — XGBoost, CatBoost, LightGBM with 80/20 stratified split, ONNX export.
- **Deep learning** — LSTM encoder + Transformer encoder over action sequences.
- **GNN** — VGAE + simple HGT over the player-pair graph.
- **Qdrant** — similarity search over pair-pattern embeddings.
- **Wide-and-Deep meta-learner** — small PyTorch stacker that fuses all model outputs into a final risk score.
- **Realtime processor** — Kafka hot path that computes features/rules in memory, scores with trained artifacts, and optionally persists history/alerts.
- **Flink hot path** — optional PyFlink jobs that consume `hands.raw`, maintain rolling pair memory, detect action motifs, and publish alert JSON to `alerts.out`.
- **Streamlit admin** — alerts, hand viewer, model metrics, graph explorer, retrain trigger, similarity search.
- **AWS deployment** — Terraform for VPC, ECR, S3, ECS/Fargate, optional MSK/Qdrant, and a SageMaker training pipeline.

## Quickstart (local, no Snowflake account)

```bash
cp .env.example .env       # ships with WAREHOUSE_BACKEND=duckdb
make install
make demo                  # services + migrate + generate + consume + features + train + seed-qdrant
make admin                 # streamlit at http://localhost:8501
```

## Real-time processing

`make demo` runs the batch demo path: generate Kafka data, persist it, then compute features/train/score from the warehouse. For live processing, use the realtime targets instead.

Training reads historical data from the configured warehouse (`duckdb` locally or Snowflake in cloud mode). Live hands are processed directly by the realtime pipeline from Kafka and model artifacts; the warehouse is only used afterward for optional history/admin/training persistence:

```bash
make demo                  # builds warehouse history and model artifacts once
make realtime              # terminal 1: Kafka -> in-memory ML/DL scoring -> ALERTS
make generate              # terminal 2: publish new live hands
```

For a one-command live smoke run:

```bash
make demo-realtime
```

By default, `demo-realtime` does not write to the warehouse. To keep realtime
history/alerts for later training or admin inspection, run migrations first and
drop the no-persist flags:

```bash
make migrate
make demo-realtime REALTIME_FLAGS=--from-beginning
```

`scripts/realtime.py` computes features/rules from each Kafka batch in memory, runs ML/DL/GNN-score lookup from existing model artifacts, generates alerts, and then optionally stores raw/features/rules/alerts for durability. Use `--batch-size` for latency/throughput, `--no-persist-history` to avoid warehouse history writes, and `--no-persist-alerts` to avoid alert table writes.

Qdrant pattern search can be added to realtime as a bounded, fail-open
enrichment:

```bash
python scripts/realtime.py --enable-pattern-search
```

When enabled, realtime only queries candidate pairs from hands that already have
rule signals or preliminary high-risk scores. Qdrant failures or missing
collections are logged and skipped so the hot path can still generate alerts.

## Flink hot path

The Flink implementation is the production-oriented replacement for the direct
Python Kafka loop. It consumes complete hand events from `hands.raw`, reuses the
same in-memory feature/rule/model scoring path, maintains rolling player-pair
memory, detects short action motifs for candidate pair review, optionally
enriches candidates with Qdrant pattern search, and publishes alert JSON records
to `alerts.out`.

```bash
make install-flink         # optional PyFlink runtime
make services              # Kafka + Qdrant
make demo                  # build historical warehouse + model artifacts once
make flink-realtime        # terminal 1: Flink/PyFlink Kafka -> alerts.out
make generate              # terminal 2: publish new live hands
```

For checkpointable pair memory, run the keyed-state job beside the alert scorer:

```bash
make flink-pair-memory FLINK_PAIR_MEMORY_FLAGS=--from-beginning
make flink-realtime FLINK_FLAGS="--use-pair-memory-topic --enable-pattern-search"
```

This reads `hands.raw`, expands each hand into player-pair updates, keys by
`player_a|player_b`, stores rolling pair state in Flink state, and publishes
feature snapshots to `pair.memory`. When `flink-realtime` runs with
`--use-pair-memory-topic`, it keeps that topic in broadcast state and uses the
latest pair-memory rows for Qdrant candidate gating.

For action-level candidate motifs, run the action-pattern job:

```bash
make flink-action-patterns FLINK_ACTION_PATTERN_FLAGS=--from-beginning
```

This reads `hands.raw`, expands each complete hand into normalized action
events, detects patterns such as preflop squeeze, raise/fold benefit,
call-down transfer, and passive soft-play chains, then publishes pair-level
signals to `patterns.action`. This is a low-latency candidate stream for the
next scorer/Qdrant enrichment pass; the current alert scorer still reads
complete hands plus optional `pair.memory`.

Useful knobs:

```bash
make flink-realtime FLINK_FLAGS="--from-beginning --threshold 0.25"
make flink-realtime FLINK_FLAGS="--enable-pattern-search"
make flink-realtime FLINK_FLAGS="--enable-pattern-search --pattern-candidate-pair-memory-score 0.55"
make flink-action-patterns FLINK_ACTION_PATTERN_FLAGS="--max-gap 4 --min-call-amount-bb 3.0"
```

If your PyFlink runtime does not bundle the Kafka connector, download the
matching Flink Kafka connector JAR and pass it with
`--kafka-connector-jar` or `FLINK_KAFKA_CONNECTOR_JAR`.

On machines where the default Python is newer than PyFlink's dependency stack,
pin the worker executable so the client and Flink workers use the same runtime:

```bash
PYFLINK_PYTHON=/path/to/python3.10 make flink-action-patterns \
  FLINK_ACTION_PATTERN_FLAGS="--python-executable /path/to/python3.10 --from-beginning"
```

This Flink slice still uses complete hand events as the source of truth. The
alert scorer can either keep operator-local pair memory for simple local demos
or consume `pair.memory` as a broadcast enrichment stream for checkpointed pair
features. The action-pattern job derives action-level candidate events from the
same hand stream and publishes them separately to `patterns.action`.

## Switching to Snowflake

```bash
# Edit .env:
#   WAREHOUSE_BACKEND=snowflake
#   SNOWFLAKE_ACCOUNT=...
#   SNOWFLAKE_USER=...
#   SNOWFLAKE_PASSWORD=...
#   SNOWFLAKE_WAREHOUSE=DEMO_WH
#   SNOWFLAKE_DATABASE=POKER_ML_DEMO
make migrate
make demo
```

No code changes. Same Python modules, same Streamlit admin, same ONNX artifacts.

## AWS / SageMaker architecture

Terraform under `infra/terraform` provisions the cloud demo stack:

- VPC, security groups, ECR, S3 buckets, CloudWatch logs, and IAM roles.
- Optional MSK Serverless for the `hands.raw` stream.
- Optional ECS/Fargate services for the Streamlit admin, stream consumer, and Qdrant with EFS storage.
- A SageMaker Pipeline assembled by `infra/sagemaker_pipeline.py`.

The SageMaker pipeline uses the BYOC GPU image for every stage and runs:

```text
generate -> ingest -> features/rules/pair stats
              -> XGBoost + CatBoost + LightGBM + DL + GNN
              -> Wide-and-Deep meta-learner
              -> batch score -> ALERTS
```

The cloud path defaults to DuckDB backed by S3 parquet (`DUCKDB_S3_BUCKET` / `DUCKDB_S3_PREFIX`) so the same warehouse adapter is used locally, on ECS, and in SageMaker. Snowflake remains available by setting `WAREHOUSE_BACKEND=snowflake`.

Useful targets:

```bash
make tf-init
make tf-plan
make tf-apply
make build-byoc
make push-byoc
```

## Project layout

```
pipeline/                  # Library code (pip install -e .)
  warehouse/               # Snowflake + DuckDB adapters
  generator/               # Synthetic hand + collusion injection
  kafka/                   # Producer + consumer
  features/                # Feature engineering
  rules/                   # Rule engine (5 rules)
  realtime/                # Kafka hot path: in-memory features/rules/scoring
  flink/                   # PyFlink hot path entrypoint
  ml/                      # XGBoost / CatBoost / LightGBM + ONNX
  dl/                      # LSTM + Transformer
  gnn/                     # VGAE + simple HGT
  qdrant/                  # Embedding + pattern store
  meta/                    # Wide-and-Deep meta-learner
  inference/               # Ensemble scorer
  sm/                      # SageMaker entrypoints
admin/                     # Streamlit multipage app
sql/migrations/            # 001-005 DDL (Snowflake; DuckDB-translated automatically)
scripts/                   # CLI entrypoints
infra/                     # Terraform + SageMaker pipeline definition
tests/                     # pytest smoke tests
```

## Data flow

Batch/offline demo path:

```
generator
    │
    ▼
Kafka topic `hands.raw`
    │
    ▼
stream consumer ──► RAW_HANDS / RAW_ACTIONS / RAW_PLAYERS
    │
    ▼
feature engineering + rules + pair stats
    │
    ▼
FEATURES + RULE_FLAGS + PAIR_STATS
    │
    ├──► XGB / CatBoost / LightGBM ONNX
    ├──► LSTM / Transformer
    └──► VGAE / HGT graph scores
           │
           ▼
Wide-and-Deep meta-learner
           │
           ▼
batch inference -> ALERTS -> Streamlit admin
```

Realtime hot path:

```
Kafka topic `hands.raw`
    │
    ▼
PyFlink job or scripts/realtime.py fallback
    │
    ├──► in-memory features + rule flags
    ├──► rolling pair memory (`pair.memory` with Flink keyed state)
    ├──► action motifs (`patterns.action`)
    ├──► trained ML/DL artifacts + GNN score lookup
    └──► optional Qdrant pattern enrichment
           │
           ▼
live ALERTS
           │
           ├──► optional warehouse persistence for admin/history/training
           └──► Streamlit admin
```

## License

Demo / educational use. No production data is included.
