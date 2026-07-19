.PHONY: help install install-flink check-kafka check-flink services flink-services down migrate dataset load-dataset generate replay-challenge evaluate-challenge consume realtime flink-realtime flink-pair-memory flink-action-patterns features train train-full cpu-validate seed-qdrant admin demo demo-realtime test clean build-byoc push-byoc tf-init tf-plan tf-apply snow-bootstrap snow-mfa-login snow-configure-kafka snow-render snow-build snow-push snow-deploy-admin snow-suspend-admin snow-resume-admin snow-deploy-realtime snow-train snow-status

PY ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)
PIP ?= $(shell [ -x .venv/bin/pip ] && echo .venv/bin/pip || echo pip)
STREAMLIT ?= $(shell [ -x .venv/bin/streamlit ] && echo .venv/bin/streamlit || echo streamlit)
KAFKA_TOPIC ?= hands.raw
FLINK_ALERTS_TOPIC ?= alerts.out
FLINK_PAIR_MEMORY_TOPIC ?= pair.memory
FLINK_ACTION_PATTERNS_TOPIC ?= patterns.action
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
TRAIN_HANDS ?= 20000
VALIDATION_HANDS ?= 5000
TEST_HANDS ?= 5000
CHALLENGE_HANDS ?= 5000
DATASET_PLAYERS ?= 200
DATASET_PAIRS ?= 30
DATASET_SEED ?= 42
REPLAY_RATE ?= 25
LOAD_BATCH_SIZE ?= 2000

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
	@echo "  load-dataset Load labeled train/validation/test splits and compute features"
	@echo "  generate     Generate synthetic hands and publish to Kafka"
	@echo "  replay-challenge Replay the label-free frozen challenge stream to Kafka"
	@echo "  evaluate-challenge Compare persisted alerts with challenge labels"
	@echo "  consume      Consume from Kafka and write to warehouse"
	@echo "  realtime     Consume Kafka and score each batch immediately"
	@echo "  flink-realtime Consume Kafka with PyFlink and publish alert JSON"
	@echo "  flink-pair-memory Build keyed rolling pair memory with PyFlink"
	@echo "  flink-action-patterns Detect action motifs with PyFlink"
	@echo "  features     Compute FEATURES + RULE_FLAGS tables"
	@echo "  train        CPU phase: train classical ML, export ONNX, and score"
	@echo "  train-full   Later phase: also train DL, GNN, and meta models"
	@echo "  cpu-validate Build/load frozen data and run the CPU training phase"
	@echo "  seed-qdrant  Seed Qdrant collusion/normal pattern collections"
	@echo "  admin        Launch Streamlit admin on :8501"
	@echo "  demo         End-to-end: services + migrate + generate + consume + features + train + seed-qdrant"
	@echo "  demo-realtime End-to-end live path: Kafka -> realtime processor -> ALERTS"
	@echo "  test         Run pytest smoke tests"
	@echo "  clean        Remove generated data and models"
	@echo "  snow-bootstrap Provision suspended Snowpark Container Services resources"
	@echo "  snow-mfa-login Cache a Snowflake MFA token using a local TOTP prompt"
	@echo "  snow-configure-kafka Store remote Kafka network access + credentials"
	@echo "  snow-render  Render SPCS service specs"
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

features:
	$(PY) scripts/load_warehouse.py --compute-features

train:
	$(PY) scripts/train.py --profile cpu

train-full:
	$(PY) scripts/train.py --profile full

cpu-validate: dataset load-dataset train

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

clean:
	rm -f data/parquet/*.duckdb data/parquet/*.parquet
	rm -f data/raw/*.jsonl
	rm -f models/*.onnx models/*.pt models/*.json models/*.csv

# ---- Snowflake / Snowpark Container Services ----
SNOW_IMAGE ?= poker-pipeline:dev
SNOW_REPO_URL ?= clbsdfj-bq59861.registry.snowflakecomputing.com/poker_ml_demo/spcs/poker_ml_repo
SNOW_REMOTE_IMAGE ?= $(SNOW_REPO_URL)/poker-pipeline:dev

snow-bootstrap:
	$(PY) infra/snowflake/deploy.py bootstrap

snow-mfa-login:
	$(PY) scripts/snowflake_mfa_login.py

snow-configure-kafka:
	$(PY) infra/snowflake/deploy.py configure-kafka

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
