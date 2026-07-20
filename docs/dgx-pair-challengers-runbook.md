# DGX pair-challenger runbook

This runbook trains the Phase 9 Residual MLP, FT-Transformer, and DCN-V2
challengers on DGX Spark. All models use the same frozen `pair-features-v1`
cold-start rows as the CatBoost champion.

## Leakage and promotion boundary

- Training statistics and categorical vocabularies are fitted on train only.
- Validation selects early stopping, Platt calibration, and the alert threshold.
- Test is evaluated after the best checkpoint is restored.
- The isolated private-challenge partition is not copied to DGX or read by this
  phase.
- A challenger must improve test PR-AUC by at least 2%, have a positive lower
  bound in the paired hand-bootstrap interval, match CatBoost recall at the 2%
  alert budget, and match CatBoost F1.
- Passing those public gates creates a promotion candidate only. It does not
  authorize promotion until the isolated challenge is evaluated separately.

## Train on DGX

The sync target transfers source code, the frozen cold-start Parquet files,
their schema and manifest, and CatBoost's public-split metrics and predictions.
It excludes `.env`, credentials, other data, and the private challenge.

```bash
make pair-challengers-test
make dgx-pair-challengers-train
make dgx-pair-challengers-fetch
make pair-challengers-check
```

The DGX job uses `nvcr.io/nvidia/pytorch:25.12-py3` with the repository mounted
at `/workspace`. Useful bounded overrides are:

```bash
make dgx-pair-challengers-train \
  PAIR_CHALLENGER_EPOCHS=30 \
  PAIR_CHALLENGER_BATCH_SIZE=2048 \
  PAIR_CHALLENGER_PATIENCE=5
```

## Artifact contract

The default local output is `models/pair-challengers-full-v2`. It contains:

- one checkpoint and one metrics document per architecture;
- train-fitted preprocessing metadata;
- validation and test predictions with event and hand identifiers;
- a combined run summary;
- SHA-256 hashes for every artifact.

`make pair-challengers-check` rejects missing or extra artifacts, hash changes,
wrong run IDs, unexpected splits, duplicate prediction events, incorrect row
counts, private-challenge use, or an artifact that marks a model promotion
eligible.

## Completed run

Run `pair_challengers_7cd1845a955b` trained on NVIDIA GB10 with 300,000 train,
75,000 validation, and 75,000 test pair rows. CatBoost's frozen test PR-AUC is
`0.362918`, F1 is `0.421687`, and recall at the 2% alert budget is `0.706667`.

| Model | Best epoch | Test PR-AUC | Test F1 | Recall at budget | Batch-15 p95 | PR-AUC bootstrap difference (95%) | Candidate |
|---|---:|---:|---:|---:|---:|---:|---:|
| Residual MLP | 5 | 0.186673 | 0.304762 | 0.506667 | 0.216 ms | [-0.275870, -0.097172] | No |
| FT-Transformer | 3 | 0.182130 | 0.267516 | 0.613333 | 0.318 ms | [-0.280430, -0.093550] | No |
| DCN-V2 | 5 | 0.142649 | 0.257576 | 0.426667 | 0.172 ms | [-0.328287, -0.131296] | No |

All three models are fast enough for batched inference, but their quality is
materially below CatBoost and every bootstrap interval is negative. CatBoost
therefore remains the champion. The private challenge remains sealed because
no challenger passed the public promotion gate.

The next model phase should add information absent from the tabular snapshot:
multi-hand user and pair sequences with self-supervised pretraining. Repeated
hyperparameter searches on this unchanged public test partition should not be
used as a substitute for new signal.
