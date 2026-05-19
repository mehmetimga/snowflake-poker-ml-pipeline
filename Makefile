.PHONY: help install install-flink check-kafka check-flink services flink-services down migrate generate consume realtime flink-realtime flink-pair-memory flink-action-patterns features train seed-qdrant admin demo demo-realtime test clean build-byoc push-byoc tf-init tf-plan tf-apply

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

export PYTHONPATH := $(CURDIR):$(PYTHONPATH)

help:
	@echo "Targets:"
	@echo "  install      Install Python dependencies"
	@echo "  install-flink Install optional PyFlink dependencies"
	@echo "  services     Start Kafka + Qdrant via docker compose"
	@echo "  flink-services Start Kafka + Qdrant + local Flink cluster via docker compose"
	@echo "  down         Stop docker compose services"
	@echo "  migrate      Apply SQL migrations to warehouse (DuckDB or Snowflake)"
	@echo "  generate     Generate synthetic hands and publish to Kafka"
	@echo "  consume      Consume from Kafka and write to warehouse"
	@echo "  realtime     Consume Kafka and score each batch immediately"
	@echo "  flink-realtime Consume Kafka with PyFlink and publish alert JSON"
	@echo "  flink-pair-memory Build keyed rolling pair memory with PyFlink"
	@echo "  flink-action-patterns Detect action motifs with PyFlink"
	@echo "  features     Compute FEATURES + RULE_FLAGS tables"
	@echo "  train        Train ML/DL/GNN/meta models, export ONNX"
	@echo "  seed-qdrant  Seed Qdrant collusion/normal pattern collections"
	@echo "  admin        Launch Streamlit admin on :8501"
	@echo "  demo         End-to-end: services + migrate + generate + consume + features + train + seed-qdrant"
	@echo "  demo-realtime End-to-end live path: Kafka -> realtime processor -> ALERTS"
	@echo "  test         Run pytest smoke tests"
	@echo "  clean        Remove generated data and models"

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

generate: check-kafka
	$(PY) scripts/generate.py --hands 5000 --players 200 --pairs 30 --out kafka

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
	$(PY) scripts/train.py

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
