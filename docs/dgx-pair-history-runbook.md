# DGX multi-hand history runbook

This runbook builds and trains the Phase 10 user/pair behavioral-history model.
The model combines the existing contextual pair features with Transformer
representations of the two users' previous hands and the pair's previous hands.

## Point-in-time contract

- Histories are built from immutable `context-full-v2` hand events and aligned
  one-to-one with the frozen `pair-full-v2` cold-start rows.
- Every history token has a timestamp strictly earlier than the example.
- Hands sharing a timestamp are snapshotted before any hand in that timestamp
  group updates history.
- Player populations remain disjoint across train, validation, and test.
- Sequence means and scales are fitted on valid train steps only.
- Pretraining reads train sequences only and never reads labels.
- Validation selects the fine-tuning checkpoint, Platt calibration, and alert
  threshold. Test is evaluated after the checkpoint is restored.
- Challenge files and private labels are not read, exported, or copied to DGX.

## Build and verify histories

```bash
make pair-history-test
make pair-history-dataset
make pair-history-dataset-check
```

The default artifact is `data/datasets/pair-sequences-full-v2`. It contains 16
strictly prior hands per sequence, stored as deterministic float16 NPZ files.
The 450,000-example full dataset is approximately 44 MB compressed.

The current build contains:

| Split | Pair examples | User snapshots | User-history steps | Pair-history steps |
|---|---:|---:|---:|---:|
| Train | 300,000 | 120,000 | 1,892,800 | 2,217,263 |
| Validation | 75,000 | 30,000 | 452,800 | 141,482 |
| Test | 75,000 | 30,000 | 452,800 | 141,797 |

## Pretrain and fine-tune on DGX

```bash
make dgx-pair-history-train
make dgx-pair-history-fetch
make pair-history-check
```

The self-supervised objectives are masked-step reconstruction, next-step
prediction, and contrastive window consistency. Separate user and pair encoders
are pretrained on train histories, then jointly fine-tuned with the contextual
tabular branch for pair risk.

Useful bounded overrides are:

```bash
make dgx-pair-history-train \
  PAIR_HISTORY_PRETRAIN_EPOCHS=8 \
  PAIR_HISTORY_EPOCHS=20 \
  PAIR_HISTORY_BATCH_SIZE=2048
```

## Completed run

Run `pair_history_fe49d3205cc2` trained 450,881 parameters on NVIDIA GB10.
User and pair pretraining used 113,620 and 247,190 train-only sequences,
respectively. Fine-tuning selected epoch 3 and stopped after epoch 7.

| Model | Test PR-AUC | Test F1 | Recall at 2% budget | Batch-15 p95 |
|---|---:|---:|---:|---:|
| CatBoost champion | 0.362918 | 0.421687 | 0.706667 | — |
| History Transformer | 0.181929 | 0.258993 | 0.506667 | 1.026 ms |

The paired hand-bootstrap PR-AUC difference was `-0.183826` at the median,
with a 95% interval of `[-0.290937, -0.079428]`. The sequence model therefore
does not pass the public quality gate. CatBoost remains the champion and the
private challenge remains sealed.

This is a useful boundary result: on this synthetic dataset, the aggregate
pair features already capture more of the generated signal than the learned
ordering of prior hands. Real poker-server data should add richer temporal
signals such as session transitions, changing stakes, device/network changes,
and longer behavior windows before the sequence approach is reconsidered.
