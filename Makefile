.PHONY: help install services down migrate generate consume features train seed-qdrant admin demo test clean build-byoc push-byoc tf-init tf-plan tf-apply

PY ?= $(shell [ -x .venv/bin/python ] && echo .venv/bin/python || echo python)
PIP ?= $(shell [ -x .venv/bin/pip ] && echo .venv/bin/pip || echo pip)
STREAMLIT ?= $(shell [ -x .venv/bin/streamlit ] && echo .venv/bin/streamlit || echo streamlit)

help:
	@echo "Targets:"
	@echo "  install      Install Python dependencies"
	@echo "  services     Start Kafka + Qdrant via docker compose"
	@echo "  down         Stop docker compose services"
	@echo "  migrate      Apply SQL migrations to warehouse (DuckDB or Snowflake)"
	@echo "  generate     Generate synthetic hands and publish to Kafka"
	@echo "  consume      Consume from Kafka and write to warehouse"
	@echo "  features     Compute FEATURES + RULE_FLAGS tables"
	@echo "  train        Train ML/DL/GNN/meta models, export ONNX"
	@echo "  seed-qdrant  Seed Qdrant collusion/normal pattern collections"
	@echo "  admin        Launch Streamlit admin on :8501"
	@echo "  demo         End-to-end: services + migrate + generate + consume + features + train + seed-qdrant"
	@echo "  test         Run pytest smoke tests"
	@echo "  clean        Remove generated data and models"

install:
	$(PIP) install -r requirements.txt
	$(PIP) install -e .

services:
	docker compose up -d kafka qdrant

down:
	docker compose down

migrate:
	$(PY) scripts/migrate.py

generate:
	$(PY) scripts/generate.py --hands 5000 --players 200 --pairs 30 --out kafka

consume:
	$(PY) scripts/stream.py --max-messages 5000

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
