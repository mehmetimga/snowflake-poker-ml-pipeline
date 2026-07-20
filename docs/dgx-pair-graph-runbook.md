# DGX temporal heterogeneous graph runbook

This runbook builds and trains the Phase 11 inductive temporal GraphSAGE model
for pair-risk scoring. It evaluates both completely unseen users (`cold_start`)
and unseen relationships among known users (`new_relationship`).

## Graph contract

Node and relation inputs are derived from:

- users and prior co-player edges;
- device and network associations;
- session associations;
- prior table participation;
- explicit account-link evidence.

Every edge used for an example has an event timestamp strictly earlier than the
hand. Hands sharing an event timestamp are snapshotted before any edge from
that timestamp is committed. Message passing receives feature-derived node
vectors, relation type, edge count, and recency; it never receives a trainable
raw user, device, network, session, table, or account-link ID embedding.

The `cold_start` split therefore evaluates feature-based embeddings for users
not seen during training. The `new_relationship` split evaluates protected pair
components while allowing graph state from earlier unlabeled public events, as
it would be available during chronological online inference.

## Build the baselines and graph data

The new-relationship baseline is trained separately because it must use the
same benchmark assignment as the graph model. This public-only baseline reads
train, validation, and test; it never opens the private challenge.

```bash
make pair-graph-test
make pair-graph-baseline
make pair-graph-dataset
make pair-graph-dataset-check
```

The default graph artifact is `data/datasets/pair-graph-full-v2`. It contains
750,000 graph-aligned pair examples and is approximately 62 MB compressed.

| Benchmark | Split | Rows | User edges | Resource edges |
|---|---|---:|---:|---:|
| Cold start | Train | 300,000 | 4,789,000 | 4,309,195 |
| Cold start | Validation | 75,000 | 1,189,000 | 1,069,480 |
| Cold start | Test | 75,000 | 1,189,000 | 1,069,950 |
| New relationship | Train | 210,015 | 3,351,790 | 3,016,020 |
| New relationship | Validation | 44,910 | 717,065 | 645,300 |
| New relationship | Test | 45,075 | 720,145 | 647,875 |

## Train on DGX

```bash
make dgx-pair-graph-train
make dgx-pair-graph-fetch
make pair-graph-check
```

Useful bounded overrides are:

```bash
make dgx-pair-graph-train \
  PAIR_GRAPH_EPOCHS=20 \
  PAIR_GRAPH_BATCH_SIZE=2048 \
  PAIR_GRAPH_PATIENCE=5
```

Each benchmark fits preprocessing on its own train split. Validation selects
the checkpoint, Platt calibration, and alert threshold. Test is evaluated once
after the best checkpoint is restored. Promotion requires improvement over the
matching CatBoost baseline on both benchmarks, including a positive paired
hand-bootstrap lower bound and operational metrics.

## Completed run

Run `pair_graph_31b1df3bbf37` trained a 490,017-parameter relation-aware model
on NVIDIA GB10.

| Benchmark/model | Test PR-AUC | Test F1 | Recall at 2% budget | Batch-15 p95 |
|---|---:|---:|---:|---:|
| Cold-start CatBoost | 0.362918 | 0.421687 | 0.706667 | — |
| Cold-start GraphSAGE | 0.247934 | 0.336634 | 0.653333 | 0.656 ms |
| New-pair CatBoost | 0.615757 | 0.133169 | 0.903226 | — |
| New-pair GraphSAGE | 0.508470 | 0.119403 | 0.758065 | 0.663 ms |

The cold-start paired PR-AUC difference interval was
`[-0.183188, -0.038979]`; the new-relationship interval was
`[-0.193118, -0.026377]`. Both are strictly negative. Stable incremental lift
was therefore not demonstrated, neither benchmark became a promotion
candidate, CatBoost remains champion, and the private challenge remains sealed.

The GNN did outperform the earlier neural tabular and sequence models on
cold-start PR-AUC, showing that relational structure contributes useful signal.
It still duplicated much of the synthetic signal already summarized by the
CatBoost features. A future retry should use real graph events—device changes,
network churn, transfers, account ownership, and session transitions—before
adding model complexity or tuning repeatedly against these public tests.
