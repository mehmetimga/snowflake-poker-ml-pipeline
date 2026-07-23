.PHONY: help install install-flink check-kafka check-flink services flink-services down migrate dataset world-dataset pair-dataset pair-dataset-check pair-labels pair-train pair-model-check pair-challengers-test pair-challengers-train pair-challengers-check pair-history-dataset pair-history-dataset-check pair-history-test pair-history-train pair-history-check pair-graph-baseline pair-graph-dataset pair-graph-dataset-check pair-graph-test pair-graph-train pair-graph-check pair-ensemble-test pair-ensemble-train pair-ensemble-check model-stability-test model-stability model-stability-check model-seed-stability-test model-seed-stability model-seed-stability-check model-scenario-holdout-test model-scenario-holdout model-scenario-holdout-check model-card-test model-card model-card-check phase12-model-card model-drift model-registry-test model-registry model-registry-check phase12-operational phase12-check phase12 rule-evidence-test pair-rules-test stateful-rules-test review-policy-test rule-governance-test rule-evaluation rule-evaluation-check phase-b1-check phase-b2-check phase-b3-check phase-b4-check phase-b5-check go-risk-test go-risk-race go-risk-benchmark go-risk-check go-risk-run go-risk-kafka-check go-risk-kafka risk-scores-check world-topics enrichment-topics scoring-topics canonical-flink-topics world-replay world-replay-dry world-verify world-ingest pair-features-check pair-features-ingest load-dataset generate replay-challenge evaluate-challenge consume realtime flink-realtime flink-pair-memory flink-action-patterns flink-context-build flink-context-test flink-pair-features-build flink-pair-features-test features train train-full cpu-validate dl-export dl-train-local dgx-sync dgx-train-dl dgx-fetch-dl dgx-pair-challengers-sync dgx-pair-challengers-train dgx-pair-challengers-fetch dgx-pair-history-sync dgx-pair-history-train dgx-pair-history-fetch dgx-pair-graph-sync dgx-pair-graph-train dgx-pair-graph-fetch dgx-triton-sync dgx-triton-start dgx-triton-status dgx-triton-tunnel seed-qdrant admin demo demo-realtime test clean build-byoc push-byoc tf-init tf-plan tf-apply snow-bootstrap snow-mfa-login snow-configure-kafka snow-seed-user-context snow-validate-catalog snow-inspect-flink snow-render snow-build snow-push snow-deploy-admin snow-suspend-admin snow-resume-admin snow-deploy-realtime snow-train snow-status
.PHONY: rule-monitoring-test rule-monitor-window rule-monitoring rule-monitoring-check phase-b6-check
.PHONY: c1-package-test c1-risk-bundle c1-render c1-build-risk c1-build-flink c1-build c1-image-smoke c1-release-check c1-push c1-mirror-triton c1-upload-model c1-deploy-risk c1-deploy-flink c1-deploy phase-c1-check
.PHONY: cdc-contract-test cdc-fixture-check phase-c2-readiness-check
.PHONY: go-hand-adapter-test go-hand-adapter go-hand-adapter-sim go-hand-adapter-kafka-check phase-c2-runtime-check
.PHONY: c2-adapter-package-test c2-adapter-render c2-adapter-build c2-adapter-image-smoke c2-adapter-release-check c2-adapter-push c2-adapter-deploy-sim c2-adapter-configure-kafka c2-adapter-sim-topics c2-adapter-remote-replay c2-adapter-remote-verify c2-adapter-remote-e2e phase-c2-packaging-check
.PHONY: shadow-sim-package-test shadow-sim-java-test shadow-sim-topics shadow-sim-render shadow-sim-deploy-flink shadow-sim-deploy-risk shadow-sim-deploy shadow-sim-generate shadow-sim-replay shadow-sim-verify shadow-sim-e2e phase-c2-shadow-packaging-check
.PHONY: cdc-sim-config-check cdc-sim-up cdc-sim-migrate cdc-sim-seed-user-context cdc-sim-topics cdc-sim-register cdc-sim-status cdc-sim-generate cdc-sim-verify cdc-sim-e2e cdc-sim-fault-generate cdc-sim-fault-verify cdc-sim-fault-e2e cdc-sim-recovery-e2e cdc-sim-fault-replay-e2e cdc-sim-stop phase-c2-cdc-simulation-check
.PHONY: f5-package-test phase-f5-check

PY ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)
PIP ?= $(shell [ -x .venv/bin/pip ] && echo .venv/bin/pip || echo pip)
STREAMLIT ?= $(shell [ -x .venv/bin/streamlit ] && echo .venv/bin/streamlit || echo streamlit)
KAFKA_TOPIC ?= hands.raw
FLINK_ALERTS_TOPIC ?= alerts.out
FLINK_PAIR_MEMORY_TOPIC ?= pair.memory
FLINK_ACTION_PATTERNS_TOPIC ?= patterns.action
MAVEN ?= mvn
MAVEN_REPO ?= /tmp/snowflake-poker-ml-maven-repository
GO ?= go
FLINK_CONTEXT_DIR ?= streaming/flink-java/context-enrichment
FLINK_PAIR_FEATURES_DIR ?= streaming/flink-java/pair-features
FLINK_GROUP ?= flink-realtime-$(shell date +%s)
FLINK_PAIR_MEMORY_GROUP ?= flink-pair-memory-$(shell date +%s)
FLINK_ACTION_PATTERN_GROUP ?= flink-action-patterns-$(shell date +%s)
FLINK_FLAGS ?=
FLINK_PAIR_MEMORY_FLAGS ?=
FLINK_ACTION_PATTERN_FLAGS ?=
REALTIME_HANDS ?= 5000
REALTIME_BATCH_SIZE ?= 25
REALTIME_THRESHOLD ?= 0.0
REALTIME_GROUP ?= realtime-demo-$(shell date +%s)
REALTIME_FLAGS ?= --from-beginning --no-persist-history --no-persist-alerts
DATASET_DIR ?= data/datasets/cpu-v1
WORLD_DATASET_DIR ?= data/datasets/context-v1
WORLD_DATASET_ID ?= context-v1
PAIR_DATASET_DIR ?= data/datasets/pair-v1
PAIR_DATASET_FLAGS ?=
PAIR_LABEL_FLAGS ?=
PAIR_MODEL_DIR ?= models/pair-catboost-v1
PAIR_TRAIN_FLAGS ?=
PAIR_MODEL_CHECK_FLAGS ?=
PAIR_CHALLENGER_DATASET ?= data/datasets/pair-full-v2
PAIR_CHALLENGER_BASELINE ?= models/pair-catboost-full-v2
PAIR_CHALLENGER_OUTPUT ?= models/pair-challengers-full-v2
PAIR_CHALLENGER_MODELS ?= residual_mlp,ft_transformer,dcn_v2
PAIR_CHALLENGER_EPOCHS ?= 20
PAIR_CHALLENGER_BATCH_SIZE ?= 1024
PAIR_CHALLENGER_PATIENCE ?= 4
PAIR_CHALLENGER_BOOTSTRAP_SAMPLES ?= 200
PAIR_CHALLENGER_FLAGS ?=
PAIR_HISTORY_SOURCE ?= data/datasets/context-full-v2
PAIR_HISTORY_DATASET ?= data/datasets/pair-sequences-full-v2
PAIR_HISTORY_OUTPUT ?= models/pair-history-full-v2
PAIR_HISTORY_MAX_HANDS ?= 16
PAIR_HISTORY_PRETRAIN_EPOCHS ?= 5
PAIR_HISTORY_EPOCHS ?= 15
PAIR_HISTORY_PRETRAIN_BATCH_SIZE ?= 512
PAIR_HISTORY_BATCH_SIZE ?= 1024
PAIR_HISTORY_PATIENCE ?= 4
PAIR_HISTORY_BOOTSTRAP_SAMPLES ?= 200
PAIR_HISTORY_DATASET_FLAGS ?=
PAIR_HISTORY_FLAGS ?=
PAIR_GRAPH_DATASET ?= data/datasets/pair-graph-full-v2
PAIR_GRAPH_OUTPUT ?= models/pair-graph-full-v2
PAIR_GRAPH_NEW_BASELINE ?= models/pair-catboost-new-relationship-v2
PAIR_GRAPH_BENCHMARKS ?= cold_start,new_relationship
PAIR_GRAPH_USER_NEIGHBORS ?= 8
PAIR_GRAPH_RESOURCE_NEIGHBORS ?= 4
PAIR_GRAPH_EPOCHS ?= 15
PAIR_GRAPH_BATCH_SIZE ?= 1024
PAIR_GRAPH_PATIENCE ?= 4
PAIR_GRAPH_BOOTSTRAP_SAMPLES ?= 200
PAIR_GRAPH_BASELINE_FLAGS ?=
PAIR_GRAPH_DATASET_FLAGS ?=
PAIR_GRAPH_FLAGS ?=
PAIR_ENSEMBLE_OUTPUT ?= models/pair-ensemble-full-v2
PAIR_ENSEMBLE_FOLDS ?= 5
PAIR_ENSEMBLE_BOOTSTRAP_SAMPLES ?= 500
PAIR_ENSEMBLE_FLAGS ?=
MODEL_REGISTRY_DIR ?= models/registry
MODEL_STABILITY_BOOTSTRAP_SAMPLES ?= 1000
MODEL_STABILITY_SEED ?= 42
MODEL_STABILITY_FLAGS ?=
MODEL_SEED_STABILITY_SEEDS ?= 11,23,42,67,101
MODEL_SEED_STABILITY_FLAGS ?=
MODEL_SCENARIO_SOURCE ?= data/datasets/context-full-v2
MODEL_SCENARIO_BOOTSTRAP_SAMPLES ?= 300
MODEL_SCENARIO_FLAGS ?=
MODEL_CARD_OWNER ?= poker-ml-platform
MODEL_CARD_REVIEW_DATE ?= $(shell date -u +%F)
MODEL_CARD_FLAGS ?=
GO_RISK_DIR ?= services/go
GO_RISK_MODEL_DIR ?= $(abspath models/pair-catboost-full-v2)
GO_RISK_LISTEN ?= :8080
TRITON_HTTP_URL ?= http://127.0.0.1:8000
GO_RISK_FLAGS ?=
GO_RISK_KAFKA_FLAGS ?=
GO_HAND_ADAPTER_FLAGS ?=
CDC_SIM_HANDS ?= 8
CDC_SIM_EXPECTED_CANONICAL ?= 4
CDC_SIM_SOURCE_DATASET_ID := sim-cdc-smoke-$(shell date -u +%Y%m%d%H%M%S)
CDC_SIM_START_AT := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
CDC_SIM_GROUP_ID := poker-go-hand-adapter-cdc-sim-$(shell date +%s)
CDC_SIM_POSTGRES_DSN ?= postgresql://poker_sim:poker_sim@localhost:5433/poker_sim
CDC_SIM_FAULT_SOURCE_DATASET_ID := sim-cdc-fault-$(shell date -u +%Y%m%d%H%M%S)
CDC_SIM_FAULT_GROUP_ID := poker-go-hand-adapter-fault-$(shell date +%s)
CDC_SIM_RECOVERY_BASELINE_DATASET_ID := sim-cdc-recovery-baseline-$(shell date -u +%Y%m%d%H%M%S)
CDC_SIM_RECOVERY_SOURCE_DATASET_ID := sim-cdc-recovery-$(shell date -u +%Y%m%d%H%M%S)
CDC_SIM_RECOVERY_GROUP_ID := poker-go-hand-adapter-recovery-$(shell date +%s)
C2_REMOTE_SIM_SOURCE_DATASET_ID := sim-cdc-remote-$(shell date -u +%Y%m%d%H%M%S)
C2_REMOTE_SIM_MANIFEST ?= build/c2/remote/$(C2_REMOTE_SIM_SOURCE_DATASET_ID).json
C2_SHADOW_SOURCE_DATASET_ID := sim-shadow-$(shell date -u +%Y%m%d%H%M%S)
C2_SHADOW_START_AT ?= $(shell $(PY) -c 'from datetime import datetime,timedelta,timezone; print((datetime.now(timezone.utc)-timedelta(minutes=15)).replace(microsecond=0).isoformat().replace("+00:00","Z"))')
C2_SHADOW_MANIFEST ?= build/c2/shadow/$(C2_SHADOW_SOURCE_DATASET_ID).json
C2_SHADOW_ADAPTER_BUILD_VERSION ?= 7ef0e7dd16d5
C2_ADAPTER_KAFKA_CONFIG_FLAGS ?=
RISK_SCORE_CHECK_FLAGS ?=
WORLD_REPLAY_MODE ?= accelerated
WORLD_REPLAY_RATE ?= 100
WORLD_REPLAY_FLAGS ?=
WORLD_INGEST_FLAGS ?=
PAIR_FEATURE_CHECK_FLAGS ?=
PAIR_FEATURE_INGEST_FLAGS ?=
TRAIN_HANDS ?= 20000
VALIDATION_HANDS ?= 5000
TEST_HANDS ?= 5000
CHALLENGE_HANDS ?= 5000
DATASET_PLAYERS ?= 200
DATASET_PAIRS ?= 30
DATASET_SEED ?= 42
REPLAY_RATE ?= 25
LOAD_BATCH_SIZE ?= 2000
DL_DATASET ?= data/datasets/dgx-v1/dl_sequences.npz
DL_OUTPUT_DIR ?= models/dgx
DGX_HOST ?= IcardiSpark
DGX_PROJECT_DIR ?= /home/mehmet/snowflake-poker-ml-pipeline
DGX_IMAGE ?= nvcr.io/nvidia/pytorch:25.12-py3
DGX_EPOCHS ?= 20
DGX_BATCH_SIZE ?= 512
DGX_PATIENCE ?= 4
DGX_TRITON_IMAGE ?= nvcr.io/nvidia/tritonserver:25.12-py3-igpu
DGX_TRITON_CONTAINER ?= poker-triton
DGX_TRITON_MODEL_DIR ?= $(DGX_PROJECT_DIR)/models/pair-catboost-full-v2/triton
DGX_TRITON_LOCAL_PORT ?= 18000

export PYTHONPATH := $(CURDIR):$(PYTHONPATH)

help:
	@echo "Targets:"
	@echo "  install      Install Python dependencies"
	@echo "  install-flink Install optional PyFlink dependencies"
	@echo "  services     Start Kafka + Qdrant via docker compose"
	@echo "  flink-services Start Kafka + Qdrant + local Flink cluster via docker compose"
	@echo "  down         Stop docker compose services"
	@echo "  migrate      Apply SQL migrations to warehouse (DuckDB or Snowflake)"
	@echo "  dataset      Build frozen PokerKit train/validation/test/challenge files"
	@echo "  world-dataset Build context-rich, multi-topic PokerKit dataset files"
	@echo "  pair-dataset Build frozen pair benchmarks and DGX Parquet exports"
	@echo "  pair-dataset-check Audit pair dataset hashes and leakage boundaries"
	@echo "  pair-labels Load restricted pair-label sidecars into the warehouse"
	@echo "  pair-train   Train/calibrate/export the pair-level CatBoost model"
	@echo "  pair-model-check Verify artifacts and score a 15-pair hand through ONNX"
	@echo "  pair-challengers-test Test neural tabular architectures and promotion gates"
	@echo "  pair-challengers-train Train Phase 9 tabular challengers locally"
	@echo "  pair-challengers-check Verify Phase 9 hashes, splits, and promotion gates"
	@echo "  pair-history-dataset Build strictly prior multi-hand user/pair histories"
	@echo "  pair-history-dataset-check Verify Phase 10 hashes, alignment, and timestamps"
	@echo "  pair-history-test Test Phase 10 dataset, pretraining, and model contracts"
	@echo "  pair-history-train Pretrain and fine-tune the Phase 10 model locally"
	@echo "  pair-history-check Verify Phase 10 model artifacts and promotion gate"
	@echo "  pair-graph-baseline Train label-safe new-relationship CatBoost comparison"
	@echo "  pair-graph-dataset Build prior-only cold-start and new-pair graph snapshots"
	@echo "  pair-graph-dataset-check Verify graph hashes, lineage, and temporal edges"
	@echo "  pair-graph-test Test the Phase 11 graph builder and inductive model"
	@echo "  pair-graph-train Train Phase 11 graph models locally"
	@echo "  pair-graph-check Verify Phase 11 artifacts and multi-benchmark gates"
	@echo "  pair-ensemble-train Train the leakage-safe Phase 12 OOF stacker"
	@echo "  pair-ensemble-check Verify OOF isolation, hashes, and portable scoring"
	@echo "  model-stability-test Test hand-grouped bootstrap and report validation"
	@echo "  model-stability Build hand-grouped confidence intervals for the champion"
	@echo "  model-stability-check Recompute and verify champion stability evidence"
	@echo "  model-seed-stability Train five validation-only seeds and report robustness"
	@echo "  model-seed-stability-check Verify seed evidence without opening test/challenge"
	@echo "  model-scenario-holdout Train leave-one-scenario-family-out benchmarks"
	@echo "  model-scenario-holdout-check Verify scenario lineage and holdout evidence"
	@echo "  model-card Build the governed champion model card in JSON and Markdown"
	@echo "  model-card-check Verify model-card hashes, identities, and rendering"
	@echo "  model-drift Build validation-window reference and evaluate test drift"
	@echo "  model-registry Build immutable registry/deployment/audit snapshots"
	@echo "  phase12-operational Run replay, recovery, load, race, and security checks"
	@echo "  phase12-check Verify all existing Phase 12 artifacts and controls"
	@echo "  phase12      Train and verify the complete Phase 12 workflow"
	@echo "  phase-b1-check Verify Python/Go rule-evidence contracts and persistence"
	@echo "  phase-b2-check Verify governed pair-rule parity and acknowledged scoring output"
	@echo "  phase-b3-check Verify stateful Python/Flink parity and Go evidence transport"
	@echo "  phase-b4-check Verify separated review-policy decisions and Kafka audit output"
	@echo "  rule-evaluation Build the hash-bound Rules v2 public-test report"
	@echo "  rule-evaluation-check Deterministically recompute rule governance evidence"
	@echo "  phase-b5-check Verify independent labels, metrics, monitoring, and rollback"
	@echo "  rule-monitoring Build delayed-label status, alert, and Prometheus artifacts"
	@echo "  rule-monitoring-check Recompute the B6 monitoring window and outputs"
	@echo "  phase-b6-check Verify dashboards, alert lineage, runtime metrics, and B1-B5"
	@echo "  phase-c1-check Verify C1 runtime bundles, SPCS specs, Go, and Flink packages"
	@echo "  cdc-contract-test Test the future immutable Debezium hand boundary"
	@echo "  cdc-fixture-check Verify direct and CDC canonical-hand parity"
	@echo "  phase-c2-readiness-check Run the complete offline C2 contract gate"
	@echo "  go-hand-adapter-test Test publish/DLQ/commit recovery behavior"
	@echo "  go-hand-adapter Run the future CDC adapter (fixture codec requires explicit flag)"
	@echo "  go-hand-adapter-sim Run the adapter on isolated poker.sim.* topics"
	@echo "  go-hand-adapter-kafka-check Verify adapter Kafka authentication only"
	@echo "  phase-c2-runtime-check Run the complete offline C2 runtime gate"
	@echo "  phase-c2-packaging-check Verify the isolated simulation adapter image/spec"
	@echo "  cdc-sim-up   Start local PostgreSQL, Kafka, and Debezium containers"
	@echo "  cdc-sim-migrate Apply idempotent schema updates to a retained local volume"
	@echo "  cdc-sim-e2e  Run the PostgreSQL -> Debezium -> Kafka -> Go smoke test"
	@echo "  cdc-sim-fault-replay-e2e Verify poison DLQs and live commit recovery"
	@echo "  cdc-sim-stop Stop Debezium and PostgreSQL while retaining their volume"
	@echo "  c2-adapter-build Build the linux/amd64 simulation adapter image"
	@echo "  c2-adapter-image-smoke Check the locally built adapter executable"
	@echo "  c2-adapter-push Push a clean-commit adapter image to Snowflake registry"
	@echo "  c2-adapter-deploy-sim Deploy the private POKER_ADAPTER_SIM service"
	@echo "  c2-adapter-sim-topics Create only the isolated poker.sim.* Confluent topics"
	@echo "  shadow-sim-topics Create isolated Flink/risk shadow topics"
	@echo "  shadow-sim-deploy Deploy POKER_FLINK_SIM and POKER_RISK_SIM"
	@echo "  shadow-sim-e2e Run one bounded CDC-to-score managed shadow replay"
	@echo "  c2-adapter-remote-e2e Replay local Debezium faults through Confluent/SPCS"
	@echo "  c1-build     Build versioned linux/amd64 poker-risk and poker-flink images"
	@echo "  c1-image-smoke Smoke-test the two locally built C1 images"
	@echo "  c1-push      Push the two versioned C1 images to Snowflake registry"
	@echo "  c1-deploy    Deploy separate POKER_FLINK and POKER_RISK SPCS services"
	@echo "  go-risk-test Test the Go complete-hand scorer and Triton client"
	@echo "  go-risk-check Verify Go can load the promoted artifact contract"
	@echo "  go-risk-run Run the Go HTTP scorer against a Triton V2 endpoint"
	@echo "  go-risk-kafka Run the Go Kafka scorer against Confluent and Triton"
	@echo "  go-risk-kafka-check Verify the Go adapter can authenticate to Kafka"
	@echo "  risk-scores-check Validate versioned risk scores consumed from Kafka"
	@echo "  world-topics Create missing canonical Kafka topics"
	@echo "  canonical-flink-topics Create missing poker.synthetic Flink topics"
	@echo "  enrichment-topics Create Flink enrichment output and dead-letter topics"
	@echo "  scoring-topics Create score, rule-evidence, alert, and dead-letter topics"
	@echo "  world-replay Publish the context-rich dataset to canonical Kafka topics"
	@echo "  world-replay-dry Validate and count the replay without Kafka writes"
	@echo "  world-verify Consume and verify frozen events from canonical Kafka topics"
	@echo "  world-ingest Consume canonical topics into the configured warehouse"
	@echo "  pair-features-check Validate pair rows and optional online/offline parity"
	@echo "  pair-features-ingest Persist pair-feature snapshots idempotently"
	@echo "  load-dataset Load labeled train/validation/test splits and compute features"
	@echo "  generate     Generate synthetic hands and publish to Kafka"
	@echo "  replay-challenge Replay the label-free frozen challenge stream to Kafka"
	@echo "  evaluate-challenge Compare persisted alerts with challenge labels"
	@echo "  consume      Consume from Kafka and write to warehouse"
	@echo "  realtime     Consume Kafka and score each batch immediately"
	@echo "  flink-realtime Consume Kafka with PyFlink and publish alert JSON"
	@echo "  flink-pair-memory Build keyed rolling pair memory with PyFlink"
	@echo "  flink-action-patterns Detect action motifs with PyFlink"
	@echo "  flink-context-build Build the native Java event-time context job"
	@echo "  flink-context-test Test the native Java event-time context job"
	@echo "  flink-pair-features-build Build the native Java pair-feature job"
	@echo "  flink-pair-features-test Test the native Java pair-feature job"
	@echo "  features     Compute FEATURES + RULE_FLAGS tables"
	@echo "  train        CPU phase: train classical ML, export ONNX, and score"
	@echo "  train-full   Later phase: also train DL, GNN, and meta models"
	@echo "  cpu-validate Build/load frozen data and run the CPU training phase"
	@echo "  dl-export    Export frozen, secret-free DL arrays from the warehouse"
	@echo "  dl-train-local Train LSTM + Transformer from the exported arrays"
	@echo "  dgx-sync     Copy code and the exported DL bundle to DGX Spark"
	@echo "  dgx-train-dl Run leakage-safe DL training in the NVIDIA PyTorch container"
	@echo "  dgx-fetch-dl Copy trained DL artifacts back from DGX Spark"
	@echo "  dgx-pair-challengers-sync Copy label-safe pair splits and challenger code to DGX"
	@echo "  dgx-pair-challengers-train Train Residual MLP, FT-Transformer, and DCN-V2"
	@echo "  dgx-pair-challengers-fetch Copy Phase 9 metrics and checkpoints from DGX"
	@echo "  dgx-pair-history-sync Copy label-safe Phase 10 histories and code to DGX"
	@echo "  dgx-pair-history-train Pretrain and fine-tune the multi-hand model on DGX"
	@echo "  dgx-pair-history-fetch Copy Phase 10 metrics and checkpoints from DGX"
	@echo "  dgx-pair-graph-sync Copy public Phase 11 graph benchmarks and baselines"
	@echo "  dgx-pair-graph-train Train cold-start and new-pair GraphSAGE models on DGX"
	@echo "  dgx-pair-graph-fetch Copy Phase 11 metrics and checkpoints from DGX"
	@echo "  dgx-triton-sync Copy the promoted Triton model repository to DGX Spark"
	@echo "  dgx-triton-start Start or reuse the localhost-only DGX Triton container"
	@echo "  dgx-triton-status Check DGX Triton container, server, and model readiness"
	@echo "  dgx-triton-tunnel Forward localhost:18000 to the DGX Triton HTTP API"
	@echo "  seed-qdrant  Seed Qdrant collusion/normal pattern collections"
	@echo "  admin        Launch Streamlit admin on :8501"
	@echo "  demo         End-to-end: services + migrate + generate + consume + features + train + seed-qdrant"
	@echo "  demo-realtime End-to-end live path: Kafka -> realtime processor -> ALERTS"
	@echo "  test         Run pytest smoke tests"
	@echo "  clean        Remove generated data and models"
	@echo "  snow-bootstrap Provision suspended Snowpark Container Services resources"
	@echo "  snow-mfa-login Cache a Snowflake MFA token using a local TOTP prompt"
	@echo "  snow-configure-kafka Store remote Kafka network access + credentials"
	@echo "  snow-seed-user-context Create and seed internal Snowflake context tables"
	@echo "  snow-render  Render SPCS service specs"
	@echo "  snow-validate-catalog Compare rendered specs with the service catalog"
	@echo "  snow-inspect-flink Read-only compare live POKER_FLINK with the catalog"
	@echo "  phase-f5-check Test and render the internal Snowflake-context deployment"
	@echo "  snow-build   Build the linux/amd64 SPCS application image"
	@echo "  snow-push    Tag and push the image to Snowflake (login first)"
	@echo "  snow-deploy-admin Deploy Streamlit admin to SPCS"
	@echo "  snow-suspend-admin Suspend Streamlit and allow the compute pool to stop"
	@echo "  snow-resume-admin Resume the Streamlit service"
	@echo "  snow-deploy-realtime Deploy remote-Kafka scorer to SPCS"
	@echo "  snow-train   Submit the containerized CPU training job"
	@echo "  snow-status  Show Snowflake container deployment status"

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

install-flink:
	$(PIP) install -r requirements-flink.txt

check-kafka:
	@$(PY) -c "import kafka" >/dev/null 2>&1 || ( \
		echo "Missing Python Kafka client for $(PY)."; \
		echo "Run: make install"; \
		echo "Or:  $(PIP) install kafka-python-ng==2.2.2"; \
		exit 1; \
	)

check-flink:
	@$(PY) -c "import pyflink" >/dev/null 2>&1 || ( \
		echo "Missing PyFlink for $(PY)."; \
		echo "Run: make install-flink"; \
		echo "If PyFlink is not available for this Python version, run via a Flink Python container."; \
		exit 1; \
	)

services:
	docker compose up -d kafka qdrant

flink-services:
	docker compose --profile flink up -d kafka qdrant flink-jobmanager flink-taskmanager

down:
	docker compose down

migrate:
	$(PY) scripts/migrate.py

dataset:
	$(PY) scripts/build_dataset.py --output-dir $(DATASET_DIR) \
		--train-hands $(TRAIN_HANDS) --validation-hands $(VALIDATION_HANDS) \
		--test-hands $(TEST_HANDS) --challenge-hands $(CHALLENGE_HANDS) \
		--players $(DATASET_PLAYERS) --pairs $(DATASET_PAIRS) --seed $(DATASET_SEED)

world-dataset:
	$(PY) scripts/generate_realtime_world.py --output-dir $(WORLD_DATASET_DIR) \
		--dataset-id $(WORLD_DATASET_ID) --train-hands $(TRAIN_HANDS) \
		--validation-hands $(VALIDATION_HANDS) --test-hands $(TEST_HANDS) \
		--challenge-hands $(CHALLENGE_HANDS) --players $(DATASET_PLAYERS) \
		--pairs $(DATASET_PAIRS) --seed $(DATASET_SEED)

pair-dataset:
	$(PY) scripts/build_pair_dataset.py --source-dir $(WORLD_DATASET_DIR) \
		--output-dir $(PAIR_DATASET_DIR) $(PAIR_DATASET_FLAGS)

pair-dataset-check:
	$(PY) scripts/check_pair_dataset.py --dataset $(PAIR_DATASET_DIR)

pair-labels:
	$(PY) scripts/load_pair_labels.py --dataset $(PAIR_DATASET_DIR) $(PAIR_LABEL_FLAGS)

pair-train: pair-dataset-check
	$(PY) scripts/train_pair_catboost.py --dataset $(PAIR_DATASET_DIR) \
		--output-dir $(PAIR_MODEL_DIR) $(PAIR_TRAIN_FLAGS)

pair-model-check:
	$(PY) scripts/check_pair_model.py --dataset $(PAIR_DATASET_DIR) \
		--model-dir $(PAIR_MODEL_DIR) $(PAIR_MODEL_CHECK_FLAGS)

pair-challengers-test:
	$(PY) -m pytest -q tests/test_pair_challengers.py

pair-challengers-train:
	$(PY) scripts/train_pair_challengers.py --dataset $(PAIR_CHALLENGER_DATASET) \
		--baseline-dir $(PAIR_CHALLENGER_BASELINE) --output-dir $(PAIR_CHALLENGER_OUTPUT) \
		--models $(PAIR_CHALLENGER_MODELS) --epochs $(PAIR_CHALLENGER_EPOCHS) \
		--batch-size $(PAIR_CHALLENGER_BATCH_SIZE) --patience $(PAIR_CHALLENGER_PATIENCE) \
		--bootstrap-samples $(PAIR_CHALLENGER_BOOTSTRAP_SAMPLES) $(PAIR_CHALLENGER_FLAGS)

pair-challengers-check:
	$(PY) scripts/check_pair_challengers.py --model-dir $(PAIR_CHALLENGER_OUTPUT)

pair-history-dataset:
	$(PY) scripts/build_pair_history_dataset.py --source-dir $(PAIR_HISTORY_SOURCE) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) --output-dir $(PAIR_HISTORY_DATASET) \
		--max-history $(PAIR_HISTORY_MAX_HANDS) $(PAIR_HISTORY_DATASET_FLAGS)

pair-history-dataset-check:
	$(PY) scripts/check_pair_history_dataset.py --dataset $(PAIR_HISTORY_DATASET) \
		--source-dir $(PAIR_HISTORY_SOURCE) --pair-dataset $(PAIR_CHALLENGER_DATASET)

pair-history-test:
	$(PY) -m pytest -q tests/test_pair_history.py

pair-history-train:
	$(PY) scripts/train_pair_history.py --history-dataset $(PAIR_HISTORY_DATASET) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) --baseline-dir $(PAIR_CHALLENGER_BASELINE) \
		--output-dir $(PAIR_HISTORY_OUTPUT) --pretrain-epochs $(PAIR_HISTORY_PRETRAIN_EPOCHS) \
		--epochs $(PAIR_HISTORY_EPOCHS) --pretrain-batch-size $(PAIR_HISTORY_PRETRAIN_BATCH_SIZE) \
		--batch-size $(PAIR_HISTORY_BATCH_SIZE) --patience $(PAIR_HISTORY_PATIENCE) \
		--bootstrap-samples $(PAIR_HISTORY_BOOTSTRAP_SAMPLES) $(PAIR_HISTORY_FLAGS)

pair-history-check:
	$(PY) scripts/check_pair_history_model.py --model-dir $(PAIR_HISTORY_OUTPUT)

pair-graph-baseline:
	$(PY) scripts/train_public_pair_baseline.py --dataset $(PAIR_CHALLENGER_DATASET) \
		--output-dir $(PAIR_GRAPH_NEW_BASELINE) --benchmark new_relationship \
		$(PAIR_GRAPH_BASELINE_FLAGS)

pair-graph-dataset:
	$(PY) scripts/build_pair_graph_dataset.py --source-dir $(PAIR_HISTORY_SOURCE) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) --output-dir $(PAIR_GRAPH_DATASET) \
		--benchmarks $(PAIR_GRAPH_BENCHMARKS) \
		--max-user-neighbors $(PAIR_GRAPH_USER_NEIGHBORS) \
		--max-resource-neighbors $(PAIR_GRAPH_RESOURCE_NEIGHBORS) \
		$(PAIR_GRAPH_DATASET_FLAGS)

pair-graph-dataset-check:
	$(PY) scripts/check_pair_graph_dataset.py --dataset $(PAIR_GRAPH_DATASET) \
		--source-dir $(PAIR_HISTORY_SOURCE) --pair-dataset $(PAIR_CHALLENGER_DATASET)

pair-graph-test:
	$(PY) -m pytest -q tests/test_pair_graph.py

pair-graph-train:
	$(PY) scripts/train_pair_graph.py --graph-dataset $(PAIR_GRAPH_DATASET) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) \
		--cold-start-baseline $(PAIR_CHALLENGER_BASELINE) \
		--new-relationship-baseline $(PAIR_GRAPH_NEW_BASELINE) \
		--output-dir $(PAIR_GRAPH_OUTPUT) --benchmarks $(PAIR_GRAPH_BENCHMARKS) \
		--epochs $(PAIR_GRAPH_EPOCHS) --batch-size $(PAIR_GRAPH_BATCH_SIZE) \
		--patience $(PAIR_GRAPH_PATIENCE) \
		--bootstrap-samples $(PAIR_GRAPH_BOOTSTRAP_SAMPLES) $(PAIR_GRAPH_FLAGS)

pair-graph-check:
	$(PY) scripts/check_pair_graph_model.py --model-dir $(PAIR_GRAPH_OUTPUT)

pair-ensemble-test:
	$(PY) -m pytest -q tests/test_pair_ensemble.py tests/test_model_ops.py

pair-ensemble-train:
	$(PY) scripts/train_pair_ensemble.py --dataset $(PAIR_CHALLENGER_DATASET) \
		--champion-dir $(PAIR_CHALLENGER_BASELINE) --output-dir $(PAIR_ENSEMBLE_OUTPUT) \
		--folds $(PAIR_ENSEMBLE_FOLDS) \
		--bootstrap-samples $(PAIR_ENSEMBLE_BOOTSTRAP_SAMPLES) $(PAIR_ENSEMBLE_FLAGS)

pair-ensemble-check:
	$(PY) scripts/check_pair_ensemble.py --model-dir $(PAIR_ENSEMBLE_OUTPUT)

model-stability-test:
	$(PY) -m pytest -q tests/test_model_stability.py

model-stability:
	$(PY) scripts/build_model_stability_report.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--output $(MODEL_REGISTRY_DIR)/stability_report.json \
		--bootstrap-samples $(MODEL_STABILITY_BOOTSTRAP_SAMPLES) \
		--random-seed $(MODEL_STABILITY_SEED) $(MODEL_STABILITY_FLAGS)

model-stability-check:
	$(PY) scripts/check_model_stability.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--report $(MODEL_REGISTRY_DIR)/stability_report.json

model-seed-stability-test:
	$(PY) -m pytest -q tests/test_seed_stability.py

model-seed-stability:
	$(PY) scripts/build_validation_seed_stability.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--output $(MODEL_REGISTRY_DIR)/validation_seed_stability.json \
		--seeds $(MODEL_SEED_STABILITY_SEEDS) $(MODEL_SEED_STABILITY_FLAGS)

model-seed-stability-check:
	$(PY) scripts/check_validation_seed_stability.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--report $(MODEL_REGISTRY_DIR)/validation_seed_stability.json

model-scenario-holdout-test:
	$(PY) -m pytest -q tests/test_scenario_holdout.py

model-scenario-holdout:
	$(PY) scripts/build_scenario_holdout_report.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--source-world $(MODEL_SCENARIO_SOURCE) \
		--output $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--lineage $(MODEL_REGISTRY_DIR)/generator_scenario_lineage.parquet \
		--bootstrap-samples $(MODEL_SCENARIO_BOOTSTRAP_SAMPLES) $(MODEL_SCENARIO_FLAGS)

model-scenario-holdout-check:
	$(PY) scripts/check_scenario_holdout_report.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--source-world $(MODEL_SCENARIO_SOURCE) \
		--report $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--lineage $(MODEL_REGISTRY_DIR)/generator_scenario_lineage.parquet

model-card-test:
	$(PY) -m pytest -q tests/test_model_card.py

model-card: model-stability-check model-seed-stability-check model-scenario-holdout-check model-drift phase12-operational model-registry model-registry-check
	$(PY) scripts/build_model_card.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--registry-dir $(MODEL_REGISTRY_DIR) \
		--stability-report $(MODEL_REGISTRY_DIR)/stability_report.json \
		--seed-stability-report $(MODEL_REGISTRY_DIR)/validation_seed_stability.json \
		--scenario-holdout-report $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--output $(MODEL_REGISTRY_DIR)/model_card.json \
		--markdown $(MODEL_REGISTRY_DIR)/model_card.md \
		--owner $(MODEL_CARD_OWNER) --review-date $(MODEL_CARD_REVIEW_DATE) $(MODEL_CARD_FLAGS)

model-card-check:
	$(PY) scripts/check_model_card.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--registry-dir $(MODEL_REGISTRY_DIR) \
		--stability-report $(MODEL_REGISTRY_DIR)/stability_report.json \
		--seed-stability-report $(MODEL_REGISTRY_DIR)/validation_seed_stability.json \
		--scenario-holdout-report $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--card $(MODEL_REGISTRY_DIR)/model_card.json \
		--markdown $(MODEL_REGISTRY_DIR)/model_card.md

phase12-model-card: model-card
	$(MAKE) model-card-check

model-drift:
	$(PY) scripts/check_model_drift.py --dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) --output-dir $(MODEL_REGISTRY_DIR)

phase12-operational:
	$(PY) scripts/run_phase12_operational_checks.py \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--output $(MODEL_REGISTRY_DIR)/operational_report.json

model-registry:
	$(PY) scripts/build_model_registry.py --champion-dir $(PAIR_CHALLENGER_BASELINE) \
		--ensemble-dir $(PAIR_ENSEMBLE_OUTPUT) --dataset-dir $(PAIR_CHALLENGER_DATASET) \
		--output-dir $(MODEL_REGISTRY_DIR) \
		--operational-report $(MODEL_REGISTRY_DIR)/operational_report.json

model-registry-test:
	$(PY) -m pytest -q tests/test_registry.py

model-registry-check:
	$(PY) scripts/check_model_registry.py --registry-dir $(MODEL_REGISTRY_DIR)

phase12-check: pair-ensemble-test pair-ensemble-check model-stability-test model-seed-stability-test model-seed-stability-check model-scenario-holdout-test model-scenario-holdout-check model-registry-test model-card-test phase12-model-card

phase12: pair-ensemble-train model-stability model-seed-stability model-scenario-holdout phase12-check

rule-evidence-test:
	$(PY) -m pytest -q tests/test_rule_evidence.py \
		tests/test_rule_evidence_warehouse.py tests/test_scoring_contracts.py
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

phase-b1-check: rule-evidence-test

pair-rules-test:
	$(PY) -m pytest -q tests/test_pair_rules_v2.py
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

phase-b2-check: rule-evidence-test pair-rules-test

stateful-rules-test:
	$(PY) -m pytest -q tests/test_stateful_pair_rules.py tests/test_pair_rules_v2.py
	cd $(FLINK_PAIR_FEATURES_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) test
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

phase-b3-check: rule-evidence-test stateful-rules-test

review-policy-test:
	$(PY) -m pytest -q tests/test_review_policy.py tests/test_scoring_contracts.py
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

phase-b4-check: phase-b3-check review-policy-test

rule-governance-test:
	$(PY) -m pytest -q tests/test_rule_evaluation.py tests/test_pair_rules_v2.py
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

rule-evaluation:
	$(PY) scripts/build_rule_evaluation_report.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--source-world $(MODEL_SCENARIO_SOURCE) \
		--scenario-report $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--lineage $(MODEL_REGISTRY_DIR)/generator_scenario_lineage.parquet \
		--output $(MODEL_REGISTRY_DIR)/rule_evaluation_report.json

rule-evaluation-check:
	$(PY) scripts/check_rule_evaluation_report.py \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--model-dir $(PAIR_CHALLENGER_BASELINE) \
		--source-world $(MODEL_SCENARIO_SOURCE) \
		--scenario-report $(MODEL_REGISTRY_DIR)/scenario_holdout_report.json \
		--lineage $(MODEL_REGISTRY_DIR)/generator_scenario_lineage.parquet \
		--output $(MODEL_REGISTRY_DIR)/rule_evaluation_report.json

phase-b5-check: phase-b4-check rule-governance-test rule-evaluation-check

rule-monitoring-test:
	$(PY) -m pytest -q tests/test_rule_monitoring.py tests/test_rule_evaluation.py
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/risk ./internal/stream

rule-monitor-window:
	$(PY) scripts/build_rule_monitoring_window.py \
		--baseline $(MODEL_REGISTRY_DIR)/rule_evaluation_report.json \
		--dataset $(PAIR_CHALLENGER_DATASET) \
		--output $(MODEL_REGISTRY_DIR)/rule_monitoring_window.json

rule-monitoring: rule-monitor-window
	$(PY) scripts/build_rule_monitoring_report.py \
		--baseline $(MODEL_REGISTRY_DIR)/rule_evaluation_report.json \
		--window $(MODEL_REGISTRY_DIR)/rule_monitoring_window.json \
		--output $(MODEL_REGISTRY_DIR)/rule_monitoring_report.json \
		--prometheus-output $(MODEL_REGISTRY_DIR)/rule_monitoring.prom

rule-monitoring-check:
	$(PY) scripts/check_rule_monitoring_report.py \
		--baseline $(MODEL_REGISTRY_DIR)/rule_evaluation_report.json \
		--window $(MODEL_REGISTRY_DIR)/rule_monitoring_window.json \
		--output $(MODEL_REGISTRY_DIR)/rule_monitoring_report.json \
		--prometheus-output $(MODEL_REGISTRY_DIR)/rule_monitoring.prom

phase-b6-check: phase-b5-check rule-monitoring-test rule-monitoring-check

go-risk-test:
	cd $(GO_RISK_DIR) && $(GO) test ./...

go-risk-race:
	cd $(GO_RISK_DIR) && $(GO) test -race ./internal/risk ./internal/stream

go-risk-benchmark:
	cd $(GO_RISK_DIR) && $(GO) test -run '^$$' -bench '^BenchmarkScoreHand$$' -benchtime=1s ./internal/risk

go-risk-check:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/risk-contract-check \
		--model-dir $(GO_RISK_MODEL_DIR)

go-risk-run:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/risk-scorer \
		--model-dir $(GO_RISK_MODEL_DIR) --triton-url $(TRITON_HTTP_URL) \
		--listen $(GO_RISK_LISTEN) $(GO_RISK_FLAGS)

go-risk-kafka:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/risk-kafka \
		--model-dir $(GO_RISK_MODEL_DIR) --triton-url $(TRITON_HTTP_URL) \
		$(GO_RISK_KAFKA_FLAGS)

go-risk-kafka-check:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/risk-kafka \
		--model-dir $(GO_RISK_MODEL_DIR) --check-kafka-only

risk-scores-check: check-kafka
	$(PY) scripts/check_risk_scores.py $(RISK_SCORE_CHECK_FLAGS)

world-replay: check-kafka
	$(PY) scripts/replay_world.py --dataset $(WORLD_DATASET_DIR) \
		--mode $(WORLD_REPLAY_MODE) --rate $(WORLD_REPLAY_RATE) $(WORLD_REPLAY_FLAGS)

world-topics: check-kafka
	$(PY) scripts/ensure_world_topics.py

enrichment-topics: check-kafka
	$(PY) scripts/ensure_enrichment_topics.py

scoring-topics: check-kafka
	$(PY) scripts/ensure_scoring_topics.py

canonical-flink-topics: check-kafka
	$(PY) scripts/ensure_canonical_flink_topics.py

world-replay-dry:
	$(PY) scripts/replay_world.py --dataset $(WORLD_DATASET_DIR) \
		--mode replay --dry-run $(WORLD_REPLAY_FLAGS)

world-verify: check-kafka
	$(PY) scripts/verify_world_replay.py --dataset $(WORLD_DATASET_DIR) \
		$(WORLD_REPLAY_FLAGS)

world-ingest: check-kafka
	$(PY) scripts/ingest_world.py --migrate $(WORLD_INGEST_FLAGS)

pair-features-check: check-kafka
	$(PY) scripts/check_pair_features.py $(PAIR_FEATURE_CHECK_FLAGS)

pair-features-ingest: check-kafka
	$(PY) scripts/ingest_pair_features.py $(PAIR_FEATURE_INGEST_FLAGS)

load-dataset: migrate
	@for split in train validation test; do \
		$(PY) scripts/load_warehouse.py \
			--jsonl $(DATASET_DIR)/$$split.events.jsonl \
			--labels $(DATASET_DIR)/$$split.labels.jsonl \
			--batch-size $(LOAD_BATCH_SIZE) || exit $$?; \
	done
	$(PY) scripts/load_warehouse.py --compute-features

generate: check-kafka
	$(PY) scripts/generate.py --hands 5000 --players 200 --pairs 30 --out kafka

replay-challenge: check-kafka
	$(PY) scripts/replay.py --events $(DATASET_DIR)/challenge.events.jsonl --rate $(REPLAY_RATE)

evaluate-challenge:
	$(PY) scripts/evaluate_challenge.py --labels $(DATASET_DIR)/challenge.labels.jsonl

consume: check-kafka
	$(PY) scripts/stream.py --max-messages 5000

realtime: check-kafka
	$(PY) scripts/realtime.py

flink-realtime: check-flink
	KAFKA_HANDS_TOPIC=$(KAFKA_TOPIC) KAFKA_ALERTS_TOPIC=$(FLINK_ALERTS_TOPIC) \
		KAFKA_PAIR_MEMORY_TOPIC=$(FLINK_PAIR_MEMORY_TOPIC) \
		$(PY) scripts/flink_realtime.py --input-topic $(KAFKA_TOPIC) \
		--alerts-topic $(FLINK_ALERTS_TOPIC) \
		--pair-memory-topic $(FLINK_PAIR_MEMORY_TOPIC) \
		--group-id $(FLINK_GROUP) $(FLINK_FLAGS)

flink-pair-memory: check-flink
	KAFKA_HANDS_TOPIC=$(KAFKA_TOPIC) KAFKA_PAIR_MEMORY_TOPIC=$(FLINK_PAIR_MEMORY_TOPIC) \
		$(PY) scripts/flink_pair_memory.py --input-topic $(KAFKA_TOPIC) \
		--pair-memory-topic $(FLINK_PAIR_MEMORY_TOPIC) \
		--group-id $(FLINK_PAIR_MEMORY_GROUP) $(FLINK_PAIR_MEMORY_FLAGS)

flink-action-patterns: check-flink
	KAFKA_HANDS_TOPIC=$(KAFKA_TOPIC) KAFKA_ACTION_PATTERNS_TOPIC=$(FLINK_ACTION_PATTERNS_TOPIC) \
		$(PY) scripts/flink_action_patterns.py --input-topic $(KAFKA_TOPIC) \
		--action-patterns-topic $(FLINK_ACTION_PATTERNS_TOPIC) \
		--group-id $(FLINK_ACTION_PATTERN_GROUP) $(FLINK_ACTION_PATTERN_FLAGS)

flink-context-build:
	cd $(FLINK_CONTEXT_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) clean package

flink-context-test:
	cd $(FLINK_CONTEXT_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) test

flink-pair-features-build:
	cd $(FLINK_PAIR_FEATURES_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) clean package

flink-pair-features-test:
	cd $(FLINK_PAIR_FEATURES_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) test

features:
	$(PY) scripts/load_warehouse.py --compute-features

train:
	$(PY) scripts/train.py --profile cpu

train-full:
	$(PY) scripts/train.py --profile full

cpu-validate: dataset load-dataset train

dl-export:
	$(PY) scripts/export_dl_dataset.py --output $(DL_DATASET)

dl-train-local:
	$(PY) scripts/train_dl.py --dataset $(DL_DATASET) --output-dir $(DL_OUTPUT_DIR) \
		--epochs $(DGX_EPOCHS) --batch-size $(DGX_BATCH_SIZE) \
		--patience $(DGX_PATIENCE)

dgx-sync:
	@test -f $(DL_DATASET) || (echo "Missing $(DL_DATASET); run 'make dl-export' first."; exit 1)
	ssh $(DGX_HOST) "mkdir -p $(DGX_PROJECT_DIR)/data/datasets/dgx-v1 $(DGX_PROJECT_DIR)/models/dgx"
	rsync -az --exclude=.env --exclude=.git --exclude=.venv --exclude=data --exclude=models \
		./ $(DGX_HOST):$(DGX_PROJECT_DIR)/
	rsync -az $(DL_DATASET) $(DL_DATASET:.npz=.manifest.json) \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/data/datasets/dgx-v1/

dgx-train-dl: dgx-sync
	ssh $(DGX_HOST) docker run --rm --gpus all --ipc=host \
		--ulimit memlock=-1 --ulimit stack=67108864 \
		-e PYTHONPATH=/workspace -e PYTHONUNBUFFERED=1 \
		-v $(DGX_PROJECT_DIR):/workspace -w /workspace $(DGX_IMAGE) \
		python scripts/train_dl.py --dataset $(DL_DATASET) --output-dir $(DL_OUTPUT_DIR) \
		--epochs $(DGX_EPOCHS) --batch-size $(DGX_BATCH_SIZE) \
		--patience $(DGX_PATIENCE) --device cuda

dgx-fetch-dl:
	mkdir -p $(DL_OUTPUT_DIR)
	rsync -az $(DGX_HOST):$(DGX_PROJECT_DIR)/$(DL_OUTPUT_DIR)/ $(DL_OUTPUT_DIR)/

dgx-pair-challengers-sync:
	@test -f $(PAIR_CHALLENGER_DATASET)/manifest.json || \
		(echo "Missing pair dataset manifest; build pair-full-v2 first."; exit 1)
	@test -f $(PAIR_CHALLENGER_BASELINE)/predictions.parquet || \
		(echo "Missing promoted CatBoost predictions."; exit 1)
	ssh $(DGX_HOST) "mkdir -p $(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE) $(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_OUTPUT)"
	rsync -az --exclude=.env --exclude=.git --exclude=.venv --exclude=data --exclude=models \
		./ $(DGX_HOST):$(DGX_PROJECT_DIR)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/manifest.json $(PAIR_CHALLENGER_DATASET)/schema.json \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/dgx/cold_start/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start/
	rsync -az $(PAIR_CHALLENGER_BASELINE)/metrics.json \
		$(PAIR_CHALLENGER_BASELINE)/predictions.parquet \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE)/

dgx-pair-challengers-train: dgx-pair-challengers-sync
	ssh $(DGX_HOST) docker run --rm --gpus all --ipc=host \
		--ulimit memlock=-1 --ulimit stack=67108864 \
		-e PYTHONPATH=/workspace -e PYTHONUNBUFFERED=1 \
		-v $(DGX_PROJECT_DIR):/workspace -w /workspace $(DGX_IMAGE) \
		python scripts/train_pair_challengers.py --dataset $(PAIR_CHALLENGER_DATASET) \
		--baseline-dir $(PAIR_CHALLENGER_BASELINE) --output-dir $(PAIR_CHALLENGER_OUTPUT) \
		--models $(PAIR_CHALLENGER_MODELS) --epochs $(PAIR_CHALLENGER_EPOCHS) \
		--batch-size $(PAIR_CHALLENGER_BATCH_SIZE) --patience $(PAIR_CHALLENGER_PATIENCE) \
		--bootstrap-samples $(PAIR_CHALLENGER_BOOTSTRAP_SAMPLES) --device cuda --overwrite \
		$(PAIR_CHALLENGER_FLAGS)

dgx-pair-challengers-fetch:
	mkdir -p $(PAIR_CHALLENGER_OUTPUT)
	rsync -az $(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_OUTPUT)/ \
		$(PAIR_CHALLENGER_OUTPUT)/

dgx-pair-history-sync:
	@test -f $(PAIR_HISTORY_DATASET)/manifest.json || \
		(echo "Missing Phase 10 history dataset; run make pair-history-dataset first."; exit 1)
	@test -f $(PAIR_CHALLENGER_BASELINE)/predictions.parquet || \
		(echo "Missing promoted CatBoost predictions."; exit 1)
	ssh $(DGX_HOST) "mkdir -p $(DGX_PROJECT_DIR)/$(PAIR_HISTORY_DATASET) \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE) $(DGX_PROJECT_DIR)/$(PAIR_HISTORY_OUTPUT)"
	rsync -az --exclude=.env --exclude=.git --exclude=.venv --exclude=data --exclude=models \
		./ $(DGX_HOST):$(DGX_PROJECT_DIR)/
	rsync -az $(PAIR_HISTORY_DATASET)/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_HISTORY_DATASET)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/manifest.json $(PAIR_CHALLENGER_DATASET)/schema.json \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/dgx/cold_start/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start/
	rsync -az $(PAIR_CHALLENGER_BASELINE)/metrics.json \
		$(PAIR_CHALLENGER_BASELINE)/predictions.parquet \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE)/

dgx-pair-history-train: dgx-pair-history-sync
	ssh $(DGX_HOST) docker run --rm --gpus all --ipc=host \
		--ulimit memlock=-1 --ulimit stack=67108864 \
		-e PYTHONPATH=/workspace -e PYTHONUNBUFFERED=1 \
		-v $(DGX_PROJECT_DIR):/workspace -w /workspace $(DGX_IMAGE) \
		python scripts/train_pair_history.py --history-dataset $(PAIR_HISTORY_DATASET) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) --baseline-dir $(PAIR_CHALLENGER_BASELINE) \
		--output-dir $(PAIR_HISTORY_OUTPUT) --pretrain-epochs $(PAIR_HISTORY_PRETRAIN_EPOCHS) \
		--epochs $(PAIR_HISTORY_EPOCHS) --pretrain-batch-size $(PAIR_HISTORY_PRETRAIN_BATCH_SIZE) \
		--batch-size $(PAIR_HISTORY_BATCH_SIZE) --patience $(PAIR_HISTORY_PATIENCE) \
		--bootstrap-samples $(PAIR_HISTORY_BOOTSTRAP_SAMPLES) --device cuda --overwrite \
		$(PAIR_HISTORY_FLAGS)

dgx-pair-history-fetch:
	mkdir -p $(PAIR_HISTORY_OUTPUT)
	rsync -az $(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_HISTORY_OUTPUT)/ \
		$(PAIR_HISTORY_OUTPUT)/

dgx-pair-graph-sync:
	@test -f $(PAIR_GRAPH_DATASET)/manifest.json || \
		(echo "Missing Phase 11 graph dataset; run make pair-graph-dataset first."; exit 1)
	@test -f $(PAIR_GRAPH_NEW_BASELINE)/predictions.parquet || \
		(echo "Missing new-relationship baseline; run make pair-graph-baseline first."; exit 1)
	ssh $(DGX_HOST) "mkdir -p $(DGX_PROJECT_DIR)/$(PAIR_GRAPH_DATASET) \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/new_relationship \
		$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE) \
		$(DGX_PROJECT_DIR)/$(PAIR_GRAPH_NEW_BASELINE) $(DGX_PROJECT_DIR)/$(PAIR_GRAPH_OUTPUT)"
	rsync -az --exclude=.env --exclude=.git --exclude=.venv --exclude=data --exclude=models \
		./ $(DGX_HOST):$(DGX_PROJECT_DIR)/
	rsync -az $(PAIR_GRAPH_DATASET)/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_GRAPH_DATASET)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/manifest.json $(PAIR_CHALLENGER_DATASET)/schema.json \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/
	rsync -az $(PAIR_CHALLENGER_DATASET)/dgx/cold_start/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/cold_start/
	rsync -az $(PAIR_CHALLENGER_DATASET)/dgx/new_relationship/ \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_DATASET)/dgx/new_relationship/
	rsync -az $(PAIR_CHALLENGER_BASELINE)/metrics.json \
		$(PAIR_CHALLENGER_BASELINE)/artifact_manifest.json \
		$(PAIR_CHALLENGER_BASELINE)/predictions.parquet \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_CHALLENGER_BASELINE)/
	rsync -az $(PAIR_GRAPH_NEW_BASELINE)/metrics.json \
		$(PAIR_GRAPH_NEW_BASELINE)/artifact_manifest.json \
		$(PAIR_GRAPH_NEW_BASELINE)/predictions.parquet \
		$(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_GRAPH_NEW_BASELINE)/

dgx-pair-graph-train: dgx-pair-graph-sync
	ssh $(DGX_HOST) docker run --rm --gpus all --ipc=host \
		--ulimit memlock=-1 --ulimit stack=67108864 \
		-e PYTHONPATH=/workspace -e PYTHONUNBUFFERED=1 \
		-v $(DGX_PROJECT_DIR):/workspace -w /workspace $(DGX_IMAGE) \
		python scripts/train_pair_graph.py --graph-dataset $(PAIR_GRAPH_DATASET) \
		--pair-dataset $(PAIR_CHALLENGER_DATASET) \
		--cold-start-baseline $(PAIR_CHALLENGER_BASELINE) \
		--new-relationship-baseline $(PAIR_GRAPH_NEW_BASELINE) \
		--output-dir $(PAIR_GRAPH_OUTPUT) --benchmarks $(PAIR_GRAPH_BENCHMARKS) \
		--epochs $(PAIR_GRAPH_EPOCHS) --batch-size $(PAIR_GRAPH_BATCH_SIZE) \
		--patience $(PAIR_GRAPH_PATIENCE) \
		--bootstrap-samples $(PAIR_GRAPH_BOOTSTRAP_SAMPLES) --device cuda --overwrite \
		$(PAIR_GRAPH_FLAGS)

dgx-pair-graph-fetch:
	mkdir -p $(PAIR_GRAPH_OUTPUT)
	rsync -az $(DGX_HOST):$(DGX_PROJECT_DIR)/$(PAIR_GRAPH_OUTPUT)/ \
		$(PAIR_GRAPH_OUTPUT)/

dgx-triton-sync:
	ssh $(DGX_HOST) "mkdir -p $(DGX_TRITON_MODEL_DIR)"
	rsync -az models/pair-catboost-full-v2/triton/ \
		$(DGX_HOST):$(DGX_TRITON_MODEL_DIR)/

dgx-triton-start: dgx-triton-sync
	ssh $(DGX_HOST) 'if docker container inspect $(DGX_TRITON_CONTAINER) >/dev/null 2>&1; then \
		docker start $(DGX_TRITON_CONTAINER); \
	else \
		docker image inspect $(DGX_TRITON_IMAGE) >/dev/null 2>&1 || docker pull $(DGX_TRITON_IMAGE); \
		docker run -d --name $(DGX_TRITON_CONTAINER) --restart unless-stopped --gpus all \
			-p 127.0.0.1:8000:8000 -p 127.0.0.1:8001:8001 -p 127.0.0.1:8002:8002 \
			-v $(DGX_TRITON_MODEL_DIR):/models:ro $(DGX_TRITON_IMAGE) \
			tritonserver --model-repository=/models --strict-model-config=true --log-verbose=0; \
	fi'

dgx-triton-status:
	ssh $(DGX_HOST) 'docker ps --filter name=^$(DGX_TRITON_CONTAINER)$$ \
		--format "{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}"; \
		curl --fail --silent --show-error http://127.0.0.1:8000/v2/health/ready; \
		curl --fail --silent --show-error http://127.0.0.1:8000/v2/models/pair_catboost/ready; \
		curl --fail --silent --show-error http://127.0.0.1:8000/v2/models/pair_catboost/stats'

dgx-triton-tunnel:
	ssh -N -L $(DGX_TRITON_LOCAL_PORT):127.0.0.1:8000 $(DGX_HOST)

seed-qdrant:
	$(PY) scripts/seed_qdrant.py

admin:
	$(STREAMLIT) run admin/Home.py

demo: services migrate generate consume features train seed-qdrant
	@echo ""
	@echo "Demo pipeline complete. Launch the admin with: make admin"

demo-realtime: check-kafka services
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9094 --create --if-not-exists --topic $(KAFKA_TOPIC)
	KAFKA_HANDS_TOPIC=$(KAFKA_TOPIC) $(PY) scripts/realtime.py \
		--max-messages $(REALTIME_HANDS) --batch-size $(REALTIME_BATCH_SIZE) \
		--threshold $(REALTIME_THRESHOLD) \
		--group-id $(REALTIME_GROUP) $(REALTIME_FLAGS) & \
		rt_pid=$$!; \
		sleep 3; \
		KAFKA_HANDS_TOPIC=$(KAFKA_TOPIC) $(PY) scripts/generate.py \
			--hands $(REALTIME_HANDS) --players 200 --pairs 30 --out kafka; \
		wait $$rt_pid

test:
	pytest -q

cdc-contract-test:
	$(PY) -m pytest -q tests/test_cdc_hand_history.py
	cd services/go && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache $(GO) test ./internal/cdc

cdc-fixture-check:
	$(PY) scripts/check_cdc_hand_fixture.py

phase-c2-readiness-check: cdc-contract-test cdc-fixture-check

go-hand-adapter-test:
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./internal/cdc ./internal/kafkaio ./internal/stream ./cmd/hand-adapter

go-hand-adapter:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/hand-adapter $(GO_HAND_ADAPTER_FLAGS)

go-hand-adapter-sim:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/hand-adapter \
		--simulation-mode --allow-simulation-codecs \
		--input-topic poker.sim.cdc-hand-outbox.v1 \
		--output-topic poker.sim.hands.raw.v1 \
		--dead-letter-topic poker.sim.pipeline.dead-letter.v1 \
		--dataset-id $(C2_ADAPTER_DATASET_ID) $(GO_HAND_ADAPTER_FLAGS)

go-hand-adapter-kafka-check:
	cd $(GO_RISK_DIR) && $(GO) run ./cmd/hand-adapter --check-kafka-only \
		$(GO_HAND_ADAPTER_FLAGS)

phase-c2-runtime-check: phase-c2-readiness-check go-hand-adapter-test

cdc-sim-config-check:
	docker compose --profile cdc-sim config --quiet
	$(PY) scripts/register_debezium_sim.py --check-only
	$(PY) scripts/simulate_postgres_cdc.py --dry-run --hands $(CDC_SIM_HANDS) \
		--rate 0 --dataset-id $(CDC_SIM_SOURCE_DATASET_ID) \
		--start-at $(CDC_SIM_START_AT)
	$(PY) scripts/simulate_postgres_cdc_faults.py --dry-run \
		--dataset-id $(CDC_SIM_FAULT_SOURCE_DATASET_ID) \
		--start-at $(CDC_SIM_START_AT)

cdc-sim-up:
	docker compose --profile cdc-sim up -d kafka postgres-cdc debezium-connect

cdc-sim-migrate: cdc-sim-up
	docker compose exec -T postgres-cdc psql -U poker_sim -d poker_sim \
		-v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/002_simulation_scenario.sql
	docker compose exec -T postgres-cdc psql -U poker_sim -d poker_sim \
		-v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/003_user_context_lookup.sql
	docker compose exec -T postgres-cdc psql -U poker_sim -d poker_sim \
		-v ON_ERROR_STOP=1 -f /docker-entrypoint-initdb.d/004_scope_user_context.sql

cdc-sim-seed-user-context: cdc-sim-migrate
	CDC_SIM_POSTGRES_DSN=$(CDC_SIM_POSTGRES_DSN) \
		$(PY) -m scripts.seed_postgres_user_context \
		--source-dataset-id $(CDC_SIM_SOURCE_DATASET_ID)

cdc-sim-topics:
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9094 --create --if-not-exists \
		--topic poker.sim.cdc-hand-outbox.v1 --partitions 1 --replication-factor 1
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9094 --create --if-not-exists \
		--topic poker.sim.hands.raw.v1 --partitions 3 --replication-factor 1
	docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
		--bootstrap-server kafka:9094 --create --if-not-exists \
		--topic poker.sim.pipeline.dead-letter.v1 --partitions 3 --replication-factor 1

cdc-sim-register:
	$(PY) scripts/register_debezium_sim.py --timeout 120

cdc-sim-status:
	docker compose --profile cdc-sim ps kafka postgres-cdc debezium-connect
	$(PY) scripts/register_debezium_sim.py --status-only --timeout 15

cdc-sim-generate:
	CDC_SIM_POSTGRES_DSN=$(CDC_SIM_POSTGRES_DSN) $(PY) scripts/simulate_postgres_cdc.py \
		--hands $(CDC_SIM_HANDS) --rate 0 \
		--dataset-id $(CDC_SIM_SOURCE_DATASET_ID) --start-at $(CDC_SIM_START_AT)

cdc-sim-verify:
	$(PY) scripts/check_postgres_cdc_simulation.py \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--source-dataset-id $(CDC_SIM_SOURCE_DATASET_ID) \
		--expected-source-rows $(CDC_SIM_HANDS) \
		--expected-canonical-records $(CDC_SIM_EXPECTED_CANONICAL)

cdc-sim-e2e: cdc-sim-up cdc-sim-migrate cdc-sim-topics cdc-sim-register
	$(MAKE) go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--bootstrap-servers localhost:9092 --security-protocol PLAINTEXT --expected-database poker_sim --max-records $(CDC_SIM_EXPECTED_CANONICAL) --group-id $(CDC_SIM_GROUP_ID) --metrics-listen=" & \
	adapter_pid=$$!; \
	$(PY) scripts/wait_kafka_consumer_group.py --group-id $(CDC_SIM_GROUP_ID) --topic poker.sim.cdc-hand-outbox.v1; \
	$(MAKE) cdc-sim-generate CDC_SIM_SOURCE_DATASET_ID=$(CDC_SIM_SOURCE_DATASET_ID) CDC_SIM_START_AT=$(CDC_SIM_START_AT); \
	wait $$adapter_pid
	$(MAKE) cdc-sim-verify CDC_SIM_SOURCE_DATASET_ID=$(CDC_SIM_SOURCE_DATASET_ID)

cdc-sim-fault-generate:
	CDC_SIM_POSTGRES_DSN=$(CDC_SIM_POSTGRES_DSN) $(PY) scripts/simulate_postgres_cdc_faults.py \
		--dataset-id $(CDC_SIM_FAULT_SOURCE_DATASET_ID) --start-at $(CDC_SIM_START_AT)

cdc-sim-fault-verify:
	$(PY) scripts/check_postgres_cdc_faults.py \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--source-dataset-id $(CDC_SIM_FAULT_SOURCE_DATASET_ID)

cdc-sim-fault-e2e: cdc-sim-up cdc-sim-migrate cdc-sim-topics cdc-sim-register
	$(MAKE) go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--bootstrap-servers localhost:9092 --security-protocol PLAINTEXT --expected-database poker_sim --max-records 5 --group-id $(CDC_SIM_FAULT_GROUP_ID) --metrics-listen=" & \
	adapter_pid=$$!; \
	$(PY) scripts/wait_kafka_consumer_group.py --group-id $(CDC_SIM_FAULT_GROUP_ID) --topic poker.sim.cdc-hand-outbox.v1; \
	$(MAKE) cdc-sim-fault-generate CDC_SIM_FAULT_SOURCE_DATASET_ID=$(CDC_SIM_FAULT_SOURCE_DATASET_ID) CDC_SIM_START_AT=$(CDC_SIM_START_AT); \
	wait $$adapter_pid
	$(MAKE) cdc-sim-fault-verify CDC_SIM_FAULT_SOURCE_DATASET_ID=$(CDC_SIM_FAULT_SOURCE_DATASET_ID)

cdc-sim-recovery-e2e: cdc-sim-up cdc-sim-migrate cdc-sim-topics cdc-sim-register
	$(MAKE) go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--bootstrap-servers localhost:9092 --security-protocol PLAINTEXT --expected-database poker_sim --max-records 1 --group-id $(CDC_SIM_RECOVERY_GROUP_ID) --metrics-listen=" & \
	adapter_pid=$$!; \
	$(PY) scripts/wait_kafka_consumer_group.py --group-id $(CDC_SIM_RECOVERY_GROUP_ID) --topic poker.sim.cdc-hand-outbox.v1; \
	$(PY) scripts/simulate_postgres_cdc.py --hands 1 --rate 0 --game-types NLH_CASH_6MAX --allowed-game-types NLH_CASH_6MAX --dataset-id $(CDC_SIM_RECOVERY_BASELINE_DATASET_ID) --start-at $(CDC_SIM_START_AT); \
	wait $$adapter_pid
	$(MAKE) go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--bootstrap-servers localhost:9092 --security-protocol PLAINTEXT --expected-database poker_sim --max-records 1 --group-id $(CDC_SIM_RECOVERY_GROUP_ID) --metrics-listen= --simulation-fail-first-commit" & \
	adapter_pid=$$!; \
	$(PY) scripts/wait_kafka_consumer_group.py --group-id $(CDC_SIM_RECOVERY_GROUP_ID) --topic poker.sim.cdc-hand-outbox.v1; \
	$(PY) scripts/simulate_postgres_cdc.py --hands 1 --rate 0 --game-types NLH_CASH_6MAX --allowed-game-types NLH_CASH_6MAX --dataset-id $(CDC_SIM_RECOVERY_SOURCE_DATASET_ID) --start-at $(CDC_SIM_START_AT); \
	if wait $$adapter_pid; then echo "expected injected commit failure" >&2; exit 1; fi
	$(PY) scripts/check_postgres_cdc_recovery.py \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--source-dataset-id $(CDC_SIM_RECOVERY_SOURCE_DATASET_ID) \
		--group-id $(CDC_SIM_RECOVERY_GROUP_ID) --phase after-failure
	$(MAKE) go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--bootstrap-servers localhost:9092 --security-protocol PLAINTEXT --expected-database poker_sim --max-records 1 --group-id $(CDC_SIM_RECOVERY_GROUP_ID) --metrics-listen="
	$(PY) scripts/check_postgres_cdc_recovery.py \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--source-dataset-id $(CDC_SIM_RECOVERY_SOURCE_DATASET_ID) \
		--group-id $(CDC_SIM_RECOVERY_GROUP_ID) --phase complete

cdc-sim-fault-replay-e2e: cdc-sim-fault-e2e cdc-sim-recovery-e2e

cdc-sim-stop:
	docker compose --profile cdc-sim stop debezium-connect postgres-cdc

phase-c2-cdc-simulation-check: phase-c2-packaging-check cdc-sim-config-check

clean:
	rm -f data/parquet/*.duckdb data/parquet/*.parquet
	rm -f data/raw/*.jsonl
	rm -f models/*.onnx models/*.pt models/*.json models/*.csv

# ---- Snowflake / Snowpark Container Services ----
SNOW_IMAGE ?= poker-pipeline:dev
SNOW_REPO_URL ?= clbsdfj-bq59861.registry.snowflakecomputing.com/poker_ml_demo/spcs/poker_ml_repo
SNOW_REMOTE_IMAGE ?= $(SNOW_REPO_URL)/poker-pipeline:dev
C1_IMAGE_TAG ?= $(shell $(PY) scripts/c1_image_tag.py 2>/dev/null || echo dev-unknown)
C1_RISK_IMAGE_TAG ?= $(C1_IMAGE_TAG)
C1_FLINK_IMAGE_TAG ?= $(C1_IMAGE_TAG)
C1_RISK_IMAGE ?= poker-risk:$(C1_RISK_IMAGE_TAG)
C1_FLINK_IMAGE ?= poker-flink:$(C1_FLINK_IMAGE_TAG)
C1_REMOTE_RISK_IMAGE ?= $(SNOW_REPO_URL)/poker-risk:$(C1_RISK_IMAGE_TAG)
C1_REMOTE_FLINK_IMAGE ?= $(SNOW_REPO_URL)/poker-flink:$(C1_FLINK_IMAGE_TAG)
C1_TRITON_SOURCE_IMAGE ?= nvcr.io/nvidia/tritonserver:25.12-py3
C1_REMOTE_TRITON_IMAGE ?= $(SNOW_REPO_URL)/tritonserver:25.12-py3
C1_MODEL_SOURCE ?= models/pair-catboost-full-v2
C1_MODEL_BUNDLE ?= build/c1/risk-runtime
C1_MODEL_RUN_ID ?= pair_7a1c58c1046b
C1_ALLOWED_TENANTS ?= demo
C1_RISK_SCORER_GROUP_ID ?= poker-go-risk-scorer-v1
C1_FLINK_CONTEXT_SAVEPOINT_PATH ?=
C1_FLINK_PAIR_SAVEPOINT_PATH ?=
C2_ADAPTER_IMAGE_TAG ?= $(C1_IMAGE_TAG)
C2_ADAPTER_IMAGE ?= poker-adapter:$(C2_ADAPTER_IMAGE_TAG)
C2_REMOTE_ADAPTER_IMAGE ?= $(SNOW_REPO_URL)/poker-adapter:$(C2_ADAPTER_IMAGE_TAG)
C2_ADAPTER_DATASET_ID ?= sim-cdc-v1
C2_ADAPTER_ALLOWED_TENANTS ?= demo
C2_ADAPTER_GROUP_ID ?= poker-go-hand-adapter-sim-v1

snow-bootstrap:
	$(PY) infra/snowflake/deploy.py bootstrap

snow-mfa-login:
	$(PY) scripts/snowflake_mfa_login.py

snow-configure-kafka:
	$(PY) infra/snowflake/deploy.py configure-kafka

snow-seed-user-context:
	WAREHOUSE_BACKEND=snowflake \
		$(PY) scripts/seed_snowflake_user_context.py

snow-validate-catalog:
	$(PY) infra/snowflake/deploy.py validate-catalog

snow-inspect-flink:
	$(PY) infra/snowflake/deploy.py inspect-services --service POKER_FLINK

snow-render:
	$(PY) infra/snowflake/deploy.py render

snow-build:
	docker buildx build --platform linux/amd64 --load \
		-f Dockerfile.spcs -t $(SNOW_IMAGE) .

snow-push:
	docker tag $(SNOW_IMAGE) $(SNOW_REMOTE_IMAGE)
	docker push $(SNOW_REMOTE_IMAGE)

snow-deploy-admin:
	$(PY) infra/snowflake/deploy.py deploy-admin

snow-suspend-admin:
	$(PY) infra/snowflake/deploy.py suspend-admin

snow-resume-admin:
	$(PY) infra/snowflake/deploy.py resume-admin

snow-deploy-realtime:
	$(PY) infra/snowflake/deploy.py deploy-realtime

snow-train:
	$(PY) infra/snowflake/deploy.py run-training-job

snow-status:
	$(PY) infra/snowflake/deploy.py status

c1-package-test:
	$(PY) -m pytest -q tests/test_snowflake_deploy.py tests/test_c1_packaging.py \
		tests/test_spcs_flink_savepoints.py

c1-take-flink-savepoints:
	$(PY) scripts/spcs_flink_savepoints.py \
		--image-path /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:$(C1_FLINK_IMAGE_TAG)

c1-risk-bundle:
	$(PY) scripts/build_risk_runtime_bundle.py \
		--source $(C1_MODEL_SOURCE) --output $(C1_MODEL_BUNDLE)

c1-render:
	SPCS_RISK_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-risk:$(C1_RISK_IMAGE_TAG) \
	SPCS_FLINK_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:$(C1_FLINK_IMAGE_TAG) \
	SPCS_TRITON_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/tritonserver:25.12-py3 \
	SPCS_BUILD_VERSION=$(C1_IMAGE_TAG) \
	SPCS_RISK_BUILD_VERSION=$(C1_RISK_IMAGE_TAG) \
	SPCS_FLINK_BUILD_VERSION=$(C1_FLINK_IMAGE_TAG) \
	SPCS_MODEL_RUN_ID=$(C1_MODEL_RUN_ID) \
	SPCS_RISK_SCORER_GROUP_ID=$(C1_RISK_SCORER_GROUP_ID) \
	SPCS_FLINK_CONTEXT_SAVEPOINT_PATH=$(C1_FLINK_CONTEXT_SAVEPOINT_PATH) \
	SPCS_FLINK_PAIR_SAVEPOINT_PATH=$(C1_FLINK_PAIR_SAVEPOINT_PATH) \
	RISK_ALLOWED_TENANTS=$(C1_ALLOWED_TENANTS) \
		$(PY) infra/snowflake/deploy.py render

c1-build-risk:
	docker buildx build --platform linux/amd64 --load \
		--build-arg BUILD_VERSION=$(C1_RISK_IMAGE_TAG) \
		-f Dockerfile.risk -t $(C1_RISK_IMAGE) .

c1-build-flink:
	docker buildx build --platform linux/amd64 --load \
		--build-arg BUILD_VERSION=$(C1_FLINK_IMAGE_TAG) \
		-f Dockerfile.flink -t $(C1_FLINK_IMAGE) .

c1-build: c1-build-risk c1-build-flink

c1-image-smoke: c1-risk-bundle
	docker run --rm --platform linux/amd64 --entrypoint /usr/local/bin/risk-kafka \
		$(C1_RISK_IMAGE) --help
	docker run --rm --platform linux/amd64 \
		-v $(abspath $(C1_MODEL_BUNDLE)):/opt/models:ro \
		--entrypoint /usr/local/bin/risk-contract-check $(C1_RISK_IMAGE) \
		--model-dir /opt/models
	docker run --rm --platform linux/amd64 --entrypoint /opt/flink/bin/check-poker-image \
		$(C1_FLINK_IMAGE)

c1-release-check:
	$(PY) scripts/check_c1_release.py --tag $(C1_IMAGE_TAG)

c1-push: c1-release-check
	docker tag $(C1_RISK_IMAGE) $(C1_REMOTE_RISK_IMAGE)
	docker push $(C1_REMOTE_RISK_IMAGE)
	docker tag $(C1_FLINK_IMAGE) $(C1_REMOTE_FLINK_IMAGE)
	docker push $(C1_REMOTE_FLINK_IMAGE)

c1-mirror-triton:
	docker pull --platform linux/amd64 $(C1_TRITON_SOURCE_IMAGE)
	docker tag $(C1_TRITON_SOURCE_IMAGE) $(C1_REMOTE_TRITON_IMAGE)
	docker push $(C1_REMOTE_TRITON_IMAGE)

c1-upload-model: c1-risk-bundle
	$(PY) infra/snowflake/deploy.py upload-risk-bundle \
		--bundle-dir $(C1_MODEL_BUNDLE)

c1-deploy-risk: c1-release-check
	$(PY) infra/snowflake/deploy.py deploy-risk

c1-deploy-flink: c1-release-check
	$(PY) infra/snowflake/deploy.py deploy-flink

c1-deploy: c1-deploy-flink c1-deploy-risk

phase-c1-check: c1-package-test c1-risk-bundle
	KAFKA_BOOTSTRAP_SERVERS=broker.c1.invalid:9092 $(MAKE) c1-render
	bash -n streaming/flink-java/docker/submit-jobs.sh
	cd services/go && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache go test ./...
	cd $(FLINK_CONTEXT_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) test package
	cd $(FLINK_PAIR_FEATURES_DIR) && $(MAVEN) -Dmaven.repo.local=$(MAVEN_REPO) test package

f5-package-test:
	$(PY) -m pytest -q tests/test_f5_context_deployment.py \
		tests/test_snowflake_deploy.py tests/test_c1_packaging.py \
		tests/test_seed_snowflake_user_context.py

phase-f5-check: f5-package-test
	KAFKA_BOOTSTRAP_SERVERS=broker.f5.invalid:9092 \
		$(PY) infra/snowflake/deploy.py render
	$(PY) infra/snowflake/deploy.py validate-catalog

c2-adapter-package-test:
	$(PY) -m pytest -q tests/test_c2_adapter_packaging.py \
		tests/test_snowflake_deploy.py tests/test_cdc_remote_simulation.py

c2-adapter-render:
	SPCS_ADAPTER_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-adapter:$(C2_ADAPTER_IMAGE_TAG) \
	SPCS_ADAPTER_BUILD_VERSION=$(C2_ADAPTER_IMAGE_TAG) \
	SPCS_ADAPTER_DATASET_ID=$(C2_ADAPTER_DATASET_ID) \
	SPCS_ADAPTER_ALLOWED_TENANTS=$(C2_ADAPTER_ALLOWED_TENANTS) \
	SPCS_ADAPTER_GROUP_ID=$(C2_ADAPTER_GROUP_ID) \
		$(PY) infra/snowflake/deploy.py render

c2-adapter-build:
	docker buildx build --platform linux/amd64 --load \
		--build-arg BUILD_VERSION=$(C2_ADAPTER_IMAGE_TAG) \
		-f Dockerfile.adapter -t $(C2_ADAPTER_IMAGE) .

c2-adapter-image-smoke:
	docker run --rm --platform linux/amd64 $(C2_ADAPTER_IMAGE) --help

c2-adapter-release-check:
	$(PY) scripts/check_c1_release.py --phase C2 --tag $(C2_ADAPTER_IMAGE_TAG)

c2-adapter-push: c2-adapter-release-check
	docker tag $(C2_ADAPTER_IMAGE) $(C2_REMOTE_ADAPTER_IMAGE)
	docker push $(C2_REMOTE_ADAPTER_IMAGE)

c2-adapter-deploy-sim: c2-adapter-release-check c2-adapter-render
	$(PY) infra/snowflake/deploy.py deploy-adapter-sim

c2-adapter-configure-kafka:
	$(PY) infra/snowflake/deploy.py configure-adapter-sim-kafka \
		$(C2_ADAPTER_KAFKA_CONFIG_FLAGS)

c2-adapter-sim-topics:
	$(PY) scripts/ensure_cdc_simulation_topics.py

c2-adapter-remote-replay:
	$(PY) scripts/replay_cdc_simulation_remote.py \
		--source-dataset-id $(C2_REMOTE_SIM_SOURCE_DATASET_ID) \
		--adapter-dataset-id $(C2_ADAPTER_DATASET_ID) \
		--adapter-group-id $(C2_ADAPTER_GROUP_ID) \
		--adapter-build-version $(C2_ADAPTER_IMAGE_TAG) \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--manifest $(C2_REMOTE_SIM_MANIFEST)

c2-adapter-remote-verify:
	$(PY) scripts/verify_cdc_simulation_remote.py \
		--manifest $(C2_REMOTE_SIM_MANIFEST)

c2-adapter-remote-e2e: cdc-sim-up cdc-sim-migrate cdc-sim-topics cdc-sim-register c2-adapter-sim-topics
	CDC_SIM_POSTGRES_DSN=$(CDC_SIM_POSTGRES_DSN) $(PY) scripts/simulate_postgres_cdc_faults.py \
		--dataset-id $(C2_REMOTE_SIM_SOURCE_DATASET_ID) \
		--start-at $(CDC_SIM_START_AT)
	$(MAKE) c2-adapter-remote-replay \
		C2_REMOTE_SIM_SOURCE_DATASET_ID=$(C2_REMOTE_SIM_SOURCE_DATASET_ID) \
		C2_REMOTE_SIM_MANIFEST=$(C2_REMOTE_SIM_MANIFEST)
	$(MAKE) c2-adapter-remote-verify \
		C2_REMOTE_SIM_MANIFEST=$(C2_REMOTE_SIM_MANIFEST)

phase-c2-packaging-check: phase-c2-runtime-check c2-adapter-package-test
	KAFKA_BOOTSTRAP_SERVERS=broker.c2.invalid:9092 $(MAKE) c2-adapter-render

shadow-sim-package-test:
	$(PY) -m pytest -q tests/test_shadow_simulation_packaging.py \
		tests/test_shadow_simulation_replay.py \
		tests/test_cdc_remote_simulation.py tests/test_snowflake_deploy.py

shadow-sim-java-test:
	docker run --rm -v $(CURDIR):/workspace -v $(MAVEN_REPO):/root/.m2 \
		-w /workspace/$(FLINK_CONTEXT_DIR) \
		maven:3.9.9-eclipse-temurin-17 mvn -q test package
	docker run --rm -v $(CURDIR):/workspace -v $(MAVEN_REPO):/root/.m2 \
		-w /workspace/$(FLINK_PAIR_FEATURES_DIR) \
		maven:3.9.9-eclipse-temurin-17 mvn -q test package

shadow-sim-topics:
	$(PY) scripts/ensure_shadow_simulation_topics.py

shadow-sim-render:
	SPCS_RISK_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-risk:$(C1_RISK_IMAGE_TAG) \
	SPCS_FLINK_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:$(C1_FLINK_IMAGE_TAG) \
	SPCS_TRITON_IMAGE_PATH=/POKER_ML_DEMO/SPCS/POKER_ML_REPO/tritonserver:25.12-py3 \
	SPCS_RISK_BUILD_VERSION=$(C1_RISK_IMAGE_TAG) \
	SPCS_FLINK_BUILD_VERSION=$(C1_FLINK_IMAGE_TAG) \
	SPCS_MODEL_RUN_ID=$(C1_MODEL_RUN_ID) \
	RISK_ALLOWED_TENANTS=$(C1_ALLOWED_TENANTS) \
		$(PY) infra/snowflake/deploy.py render

shadow-sim-deploy-flink: c1-release-check shadow-sim-render
	$(PY) infra/snowflake/deploy.py deploy-flink-sim

shadow-sim-deploy-risk: c1-release-check shadow-sim-render
	$(PY) infra/snowflake/deploy.py deploy-risk-sim

shadow-sim-deploy: shadow-sim-deploy-flink shadow-sim-deploy-risk

shadow-sim-generate:
	CDC_SIM_POSTGRES_DSN=$(CDC_SIM_POSTGRES_DSN) $(PY) scripts/simulate_postgres_cdc.py \
		--hands 1 --players 6 --tables 1 --pairs 1 --seed 9221 --rate 0 \
		--game-types NLH_CASH_6MAX --allowed-game-types NLH_CASH_6MAX \
		--dataset-id $(C2_SHADOW_SOURCE_DATASET_ID) \
		--start-at $(C2_SHADOW_START_AT)

shadow-sim-replay:
	$(PY) scripts/replay_shadow_simulation.py \
		--source-dataset-id $(C2_SHADOW_SOURCE_DATASET_ID) \
		--adapter-dataset-id $(C2_ADAPTER_DATASET_ID) \
		--adapter-group-id $(C2_ADAPTER_GROUP_ID) \
		--adapter-build-version $(C2_SHADOW_ADAPTER_BUILD_VERSION) \
		--flink-build-version $(C1_FLINK_IMAGE_TAG) \
		--risk-build-version $(C1_RISK_IMAGE_TAG) \
		--model-run-id $(C1_MODEL_RUN_ID) \
		--postgres-dsn $(CDC_SIM_POSTGRES_DSN) \
		--manifest $(C2_SHADOW_MANIFEST)

shadow-sim-verify:
	$(PY) scripts/verify_shadow_simulation.py \
		--manifest $(C2_SHADOW_MANIFEST)

shadow-sim-e2e: cdc-sim-up cdc-sim-migrate cdc-sim-topics cdc-sim-register shadow-sim-topics
	$(MAKE) shadow-sim-generate \
		C2_SHADOW_SOURCE_DATASET_ID=$(C2_SHADOW_SOURCE_DATASET_ID) \
		C2_SHADOW_START_AT=$(C2_SHADOW_START_AT)
	$(MAKE) shadow-sim-replay \
		C2_SHADOW_SOURCE_DATASET_ID=$(C2_SHADOW_SOURCE_DATASET_ID) \
		C2_SHADOW_MANIFEST=$(C2_SHADOW_MANIFEST)
	$(MAKE) shadow-sim-verify C2_SHADOW_MANIFEST=$(C2_SHADOW_MANIFEST)

phase-c2-shadow-packaging-check: shadow-sim-package-test shadow-sim-java-test
	KAFKA_BOOTSTRAP_SERVERS=broker.c2.invalid:9092 $(MAKE) shadow-sim-render
	cd $(GO_RISK_DIR) && GOCACHE=/tmp/snowflake-poker-ml-go-build-cache \
		$(GO) test ./...

# ---- AWS / Terraform ----
# Force AWS_PROFILE=default for these targets. The shell may export
# AWS_PROFILE=mds (a stale UBC SSO profile); override ensures Terraform
# always uses the active personal-account credentials. To target a different
# profile run e.g. `make tf-plan AWS_PROFILE=staging`.
override AWS_PROFILE := $(or $(filter-out mds,$(AWS_PROFILE)),default)
AWS_REGION  ?= us-west-2
TF_DIR      ?= infra/terraform
ECR_REPO    ?= $(shell cd $(TF_DIR) && AWS_PROFILE=$(AWS_PROFILE) terraform output -raw ecr_repository_url 2>/dev/null)
IMAGE_TAG   ?= latest

export AWS_PROFILE

tf-init:
	cd $(TF_DIR) && terraform init

tf-plan:
	cd $(TF_DIR) && terraform plan

tf-apply:
	cd $(TF_DIR) && terraform apply

build-byoc:
	docker buildx build --platform linux/amd64 --load \
		-f Dockerfile.byoc-gpu -t poker-ml:$(IMAGE_TAG) .

push-byoc: build-byoc
	@if [ -z "$(ECR_REPO)" ]; then \
		echo "ECR_REPO is empty — run 'make tf-apply' first to create the repo."; exit 1; \
	fi
	aws ecr get-login-password --region $(AWS_REGION) \
		| docker login --username AWS --password-stdin $(ECR_REPO)
	docker tag poker-ml:$(IMAGE_TAG) $(ECR_REPO):$(IMAGE_TAG)
	docker push $(ECR_REPO):$(IMAGE_TAG)
	@echo "Pushed $(ECR_REPO):$(IMAGE_TAG)"
