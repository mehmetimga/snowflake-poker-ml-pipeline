.PHONY: help install services down migrate generate consume features train seed-qdrant admin demo test clean

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
