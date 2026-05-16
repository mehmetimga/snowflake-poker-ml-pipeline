# snowflake-poker-ml-pipeline

End-to-end demo ML pipeline for detecting collusion in synthetic No-Limit Hold'em hand data. Runs locally on a laptop or in Snowflake — same code, one env var.

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
- **Streamlit admin** — alerts, hand viewer, model metrics, graph explorer, retrain trigger, similarity search.

## Quickstart (local, no Snowflake account)

```bash
cp .env.example .env       # ships with WAREHOUSE_BACKEND=duckdb
make install
make demo                  # services + migrate + generate + consume + features + train + seed-qdrant
make admin                 # streamlit at http://localhost:8501
```

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

## Project layout

```
pipeline/                  # Library code (pip install -e .)
  warehouse/               # Snowflake + DuckDB adapters
  generator/               # Synthetic hand + collusion injection
  kafka/                   # Producer + consumer + schemas
  features/                # Feature engineering
  rules/                   # Rule engine (5 rules)
  ml/                      # XGBoost / CatBoost / LightGBM + ONNX
  dl/                      # LSTM + Transformer
  gnn/                     # VGAE + simple HGT
  qdrant/                  # Embedding + pattern store
  meta/                    # Wide-and-Deep meta-learner
  inference/               # Ensemble scorer
admin/                     # Streamlit multipage app
sql/migrations/            # 001-005 DDL (Snowflake; DuckDB-translated automatically)
scripts/                   # CLI entrypoints
tests/                     # pytest smoke tests
```

## Data flow

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
                            feature engineering
                                      │
                                      ▼
                          FEATURES + RULE_FLAGS
                                      │
                  ┌───────────────────┼───────────────────────┐
                  ▼                   ▼                       ▼
            XGB/CAT/LGBM         LSTM/Transformer       VGAE/HGT graph
                  │                   │                       │
                  └───────────────────┼───────────────────────┘
                                      ▼
                       Wide-and-Deep meta-learner ──► ALERTS
                                      │
                                      ▼
                              Streamlit admin
```

## License

Demo / educational use. No production data is included.
