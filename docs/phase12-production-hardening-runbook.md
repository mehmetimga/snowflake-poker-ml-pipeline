# Phase 12: ensemble and production hardening

Phase 12 adds a leakage-safe ensemble experiment and the controls needed to
operate the current CatBoost champion. It does not change the deployed model.

## Outcome

The five-fold OOF stack combines fold-local CatBoost, deterministic rules, and
a fold-local player-history logistic baseline. It scored `0.214408` test
PR-AUC versus `0.362918` for the CatBoost champion, so the public promotion
gate rejected it. The private challenge was not read. Production remains
`pair-catboost-v1`, run `pair_7a1c58c1046b`.

The rejected result is recorded in the model registry. A future candidate can
reach production only after artifact integrity, public quality, sealed private
challenge, manual approval, and operational verification gates all pass.

## Leakage boundaries

- `StratifiedGroupKFold` groups by `hand_id`; every training row is held out
  exactly once and no hand crosses a fold.
- Every fold fits its own feature preprocessor and base estimators using only
  the other folds.
- The meta logistic model learns only from OOF base predictions.
- Validation fits Platt calibration and the alert-budget threshold.
- Test is used once for the public paired hand-bootstrap comparison.
- Challenge paths are never loaded by the Phase 12 trainer.
- Failed Phase 9–11 neural, history, and graph models are excluded from the
  stack because they lack OOF predictions and did not pass public gates.

Run or verify:

```bash
make pair-ensemble-train PAIR_ENSEMBLE_FLAGS=--overwrite
make pair-ensemble-check
```

## Registry and deployment safety

`models/registry/registry.json` stores model stage and gate results.
`deployment.json` pins the active run, artifact-manifest hash, feature version,
decision policy, Triton model, and rollback target. `audit_log.jsonl` records
tenant/product-scoped registration and deployment events. The checker detects
artifact mutation, multiple active champions, deployment identity mismatch,
and missing audit scope.

```bash
make model-registry
make model-registry-check
```

The warehouse migration `010_feedback_and_registry.sql` adds tenant-scoped
registry, deployment, monitoring, analyst-feedback, and audit tables. Analyst
feedback becomes a training label only after its `label_available_at`; an
inconclusive review never becomes a label.

## Drift monitoring

The reference uses the independent 5,000-hand validation window. This is
intentional: the 20,000-hand training generator accumulates much deeper user
and pair histories and would create artificial PSI alarms against a 5,000-hand
operational window. Test remains independent and is the simulated current
window.

The report covers numeric feature PSI, categorical total-variation distance,
and calibrated-score PSI. PSI thresholds are `0.10` warning and `0.25`
critical. A production scheduler should build the reference once per promoted
run, then evaluate fixed-duration tenant/product windows without labels.

```bash
make model-drift
```

## Go service hardening

- Hand state is keyed by tenant, dataset, split, and hand.
- HTTP and Kafka paths support explicit tenant allowlists.
- Kafka publishes score/alert output before committing input offsets.
- Restart recovery replays an uncommitted partial hand into a fresh assembler.
- The test load sends 128 concurrent complete-hand requests and the race
  detector covers scorer and stream packages.
- Prometheus exports throughput, errors, readiness, in-flight work, and an
  end-to-end request-latency histogram.
- pprof is disabled by default and rejects non-loopback listeners.
- Grafana and Prometheus definitions live in `ops/grafana` and
  `ops/prometheus`.

Production examples:

```bash
make go-risk-run GO_RISK_FLAGS="--allowed-tenants tenant-a,tenant-b --pprof-listen 127.0.0.1:6060"
make go-risk-kafka GO_RISK_KAFKA_FLAGS="--allowed-tenants tenant-a,tenant-b"
```

Run the complete acceptance suite and write its machine-readable evidence:

```bash
make phase12-operational
make phase12-check
```

The operational report must pass before the registry marks the current
deployment's operational gate as passed. The report contains the exact model
run ID and artifact-manifest SHA-256; the registry rejects a successful report
that was produced for any other artifact.
