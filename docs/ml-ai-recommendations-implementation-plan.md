# ML/AI recommendations implementation plan

This is the follow-up plan for implementing the recommendations in the
[Data science and ML/AI model development guide](data-science-model-development-guide.md).
It is the working checklist for engineering, data science, platform, and risk
operations.

## Plan control

| Field | Value |
|---|---|
| Plan version | 1 |
| Created | 2026-07-20 |
| Current production champion | `pair-catboost-v1`, run `pair_7a1c58c1046b` |
| Current feature definition | `pair-features-v1` |
| Automatic model promotion | Disabled |
| Immediate next phase | Phase A — model card, segment intervals, and validation-seed stability |

Update each phase's status and evidence links as work progresses. A checked
task means the code and tests exist; it does not by itself mean that a model is
approved for production.

Status values used below:

- `COMPLETE`: implemented and validated.
- `IN PROGRESS`: a bounded slice is implemented, but the whole phase is not complete.
- `NEXT`: ready to start.
- `BLOCKED`: needs the named prerequisite.
- `OPTIONAL`: not required for the current production path.

## 1. Objective and guardrails

The objective is to improve real-time collusion detection without losing
point-in-time correctness, reproducibility, explainability, or operational
safety.

Guardrails:

- Keep CatBoost active until another registered candidate passes every gate.
- Keep train, validation, test, and sealed challenge responsibilities separate.
- Group all pair rows from a hand together during splitting and bootstrapping.
- Never put private labels on inference topics.
- Never use future context, histories, graph edges, or analyst decisions.
- Never add a synchronous PostgreSQL or Snowflake read to the scoring path.
- Version features, rules, models, calibration, and decision policy separately.
- Require online/offline parity before shadow serving a new feature family.
- Keep hard policy decisions separate from learned probability.
- Do not repeatedly tune against the current public test or private challenge.
- Require manual approval and a tested rollback before production promotion.

## 2. Dependency and delivery order

```text
Existing frozen datasets, CatBoost, registry and drift controls
                              |
                              v
           Phase A: stability report + model card
                              |
                              v
              Phase B: versioned Rules v2
                              |
                              v
      Phase C: SPCS packaging + real-data shadow path
                              |
                +-------------+-------------+
                |                           |
                v                           v
   Phase D: graph augmentation    Phase E: richer sequence model
                |                           |
                +-------------+-------------+
                              v
          Phase F: anomaly discovery + analyst AI
```

Phases A and B can begin with current synthetic data. Phase C requires the
future poker-server/PostgreSQL/Debezium boundary and the target SPCS packaging.
Phases D and E should not seek production promotion until real graph and
temporal signals are available. Phase F is supporting capability, not primary
scoring.

## 3. Completed foundation

Status: `COMPLETE`

- [x] Deterministic PokerKit world generation.
- [x] Separate train, validation, test, and challenge datasets.
- [x] Cold-start, temporal, and new-relationship benchmark construction.
- [x] Challenge labels isolated from the public model path.
- [x] Point-in-time user and pair features.
- [x] Java/Flink context enrichment and pair-feature jobs.
- [x] Versioned 58-value Go scoring contract.
- [x] CatBoost training, validation calibration, and threshold selection.
- [x] Native CatBoost to ONNX parity checks.
- [x] CatBoost, rules-only, and player-only baselines.
- [x] Residual MLP, FT-Transformer, and DCN-V2 challengers.
- [x] Prior-only multi-hand Transformer experiment.
- [x] Prior-only temporal GraphSAGE experiment.
- [x] Leakage-safe five-fold OOF ensemble experiment.
- [x] Artifact hashes, registry, deployment snapshot, and rollback identity.
- [x] Feature and score drift report.
- [x] Go replay, restart, concurrency, race, and benchmark checks.

The neural, sequence, graph, and ensemble experiments were rejected because
they did not beat CatBoost under the public promotion policy. That rejection is
a successful safety outcome, not unfinished implementation.

## 4. Phase A — CatBoost stability report and model card

Status: `IN PROGRESS`

Purpose: quantify uncertainty around the champion and create a standard
evidence package for all future candidates without changing online scoring.

Estimated single-engineer effort: 4–6 working days.

### A1. New code and artifacts

Add:

```text
pipeline/ml/stability.py
scripts/build_model_stability_report.py
scripts/check_model_stability.py
tests/test_model_stability.py
models/registry/stability_report.json
models/registry/model_card.json
```

Implementation status:

- [x] `pipeline/ml/stability.py`
- [x] `scripts/build_model_stability_report.py`
- [x] `scripts/check_model_stability.py`
- [x] `tests/test_model_stability.py`
- [x] Local generated `models/registry/stability_report.json`
- [ ] `models/registry/model_card.json`

Add Make targets:

```text
model-stability
model-stability-check
```

- [x] Added `model-stability-test`.
- [x] Added `model-stability`.
- [x] Added `model-stability-check`.
- [x] Extended `phase12-check` with stability tests and deterministic report
  verification.

Extend `phase12-check` to run the stability checker.

### A2. Statistical work

- [x] Compute 1,000-sample paired hand-bootstrap intervals for test PR-AUC.
- [x] Compute intervals for precision, recall, F1, Brier score, alert rate, and
  false positives per 1,000 hands.
- [x] Bootstrap by `hand_id`; never sample pair rows independently.
- [x] Report recall and precision at the fixed analyst alert budget.
- [ ] Report validation robustness across at least five training seeds.
- [ ] Keep seed robustness on train/validation; do not select a seed using test.
- [ ] Add generator-seed and scenario-family holdout summaries.
- [ ] Add segment metrics with counts and confidence intervals.
- [ ] Suppress or mark segments below a configured reliability floor.
- [x] Record every stability configuration and random seed.

### A3. Model card

The machine-readable and Markdown-renderable model card must include:

- [ ] prediction unit and intended use;
- [ ] prohibited uses;
- [ ] dataset IDs, hashes, splits, and label policy;
- [ ] feature, preprocessing, model, calibration, and policy versions;
- [ ] overall and segment metrics with counts;
- [ ] synthetic-data limitation;
- [ ] known failure modes;
- [ ] drift reference and current drift status;
- [ ] inference contract and latency evidence;
- [ ] promotion and rollback identities; and
- [ ] owner and review date.

### A4. Registry generalization

The current registry builder knows the champion and one ensemble candidate.
Generalize candidate registration so future tabular, graph, sequence, or hybrid
artifacts implement one common manifest contract.

- [ ] Define a generic candidate evidence schema.
- [ ] Require dataset, feature, predictions, metrics, and artifact hashes.
- [ ] Preserve exactly one active champion per tenant/product/benchmark scope.
- [ ] Reject a candidate whose operational report belongs to another run.
- [ ] Preserve manual approval and rollback requirements.

### A5. Acceptance gate

Phase A is complete when:

- [x] existing CatBoost metrics reproduce from frozen predictions;
- [x] bootstrap output is deterministic for a fixed bootstrap seed;
- [x] private challenge datasets and labels are not opened;
- [ ] model-card facts match the artifact and registry;
- [x] mutation of any tracked source or report evidence fails validation;
- [x] all new tests pass; and
- [x] `make phase12-check` includes and passes the new controls.

Production effect: none. CatBoost run `pair_7a1c58c1046b` remains active.

## 5. Phase B — Versioned Rules v2

Status: `PENDING` after Phase A

Purpose: add explainable, governed evidence without replacing or silently
changing CatBoost probability.

Estimated single-engineer effort: 8–12 working days.

### B1. Rule-evidence contract

Add a versioned event contract with:

```text
rule_event_id
rule_id
rule_version
rule_owner
entity_type
entity_key
hand_id
severity
raw_score
evidence
effective_at
feature_definition_version
trace_id
```

Recommended Kafka topic: `poker.rule-evidence.v1`.

- [ ] Add the Python event contract and forbidden-field validation.
- [ ] Add the equivalent Go structure and validation.
- [ ] Add schema examples and round-trip fixtures.
- [ ] Use deterministic IDs so replay is idempotent.
- [ ] Reference rule evidence from score and alert events.
- [ ] Add Snowflake persistence with tenant and model-run lineage.

### B2. Port inference-safe pair rules to Go

Port the existing pair baseline first:

- [ ] one player folded while the other won;
- [ ] same device;
- [ ] same network;
- [ ] outcome asymmetry;
- [ ] A-fold/B-win rate;
- [ ] B-fold/A-win rate.

The current 58-value feature record already carries these inputs. They do not
need a database lookup or new Flink state.

- [ ] Create Python/Go golden parity fixtures.
- [ ] Emit structured evidence, not only a weighted sum.
- [ ] Keep the rules-only benchmark independently measurable.
- [ ] Store rule versions with every affected risk score.

### B3. Stateful Flink rules

Add a Flink rule only when it needs ordered state not already summarized by the
pair-feature event. Candidate stateful patterns:

- [ ] repeated fold-to-partner wins in a time window;
- [ ] persistent one-direction chip flow;
- [ ] synchronized table or session movement;
- [ ] coordinated action timing;
- [ ] changing shared device/network relationships.

For each rule:

- [ ] define key, state, watermark, lateness, TTL, and correction semantics;
- [ ] add duplicate and replay tests;
- [ ] add online/offline reference parity; and
- [ ] expose firing rate, lag, and state-size metrics.

### B4. Decision-policy separation

- [ ] Hard policy rules create mandatory review with explicit reasons.
- [ ] Soft behavioral rules attach evidence and analyst filters.
- [ ] Data-quality violations remain DLQ events, not fraud evidence.
- [ ] CatBoost calibrated probability stays unchanged in Rules v2.
- [ ] No manually weighted probability blend is introduced.

If probability fusion is proposed later, it must use OOF predictions and the
normal model promotion process.

### B5. Rule governance and evaluation

Every rule must have:

- [ ] owner and description;
- [ ] version and effective date;
- [ ] unit and replay tests;
- [ ] precision, recall, firing rate, and alert-volume report;
- [ ] segment and drift monitoring;
- [ ] rollback procedure; and
- [ ] independent-label evaluation.

Labels created solely because a rule fired must be marked so that the same rule
is not evaluated against circular truth.

### B6. Acceptance gate

Phase B is complete when:

- [ ] Python and Go rule outputs match exactly on golden fixtures;
- [ ] Kafka replay produces the same evidence IDs and revisions;
- [ ] no rule reads a database in the hot path;
- [ ] all scores retain model, feature, policy, and rule versions;
- [ ] hard versus soft behavior is explicit;
- [ ] rule dashboards and alerts exist; and
- [ ] CatBoost predictions remain bit-for-bit unchanged.

## 6. Phase C — Target runtime and real-data shadow evaluation

Status: `BLOCKED` on SPCS packaging and future CDC source integration

Purpose: measure the current pipeline on real poker-server data without
automated enforcement.

Estimated engineering effort after access and schemas exist: 2–3 weeks.
Label collection will take longer than implementation.

### C1. Target deployment

- [ ] Package Java 17/Flink as `poker-flink:<git-sha>`.
- [ ] Package Go scoring as `poker-risk:<git-sha>`.
- [ ] Deploy separate long-running `POKER_FLINK` and `POKER_RISK` SPCS services.
- [ ] Configure durable checkpoint storage and savepoint recovery.
- [ ] Store artifacts in a controlled Snowflake stage/registry URI, not a local
  absolute path.
- [ ] Add service readiness, lag, watermark, checkpoint, state, and latency
  monitoring.

### C2. Future CDC boundary

- [ ] Read immutable PostgreSQL hand history through Debezium.
- [ ] Convert CDC records into the canonical hand-event contract.
- [ ] Preserve source transaction/LSN lineage for replay and audit.
- [ ] Verify direct-publish and CDC fixtures produce equivalent canonical hands.
- [ ] Keep the poker server outside this ML repository and deployment boundary.

### C3. Shadow scoring

- [ ] Run CatBoost and Rules v2 without blocking or penalizing users.
- [ ] Persist features, raw probability, calibrated probability, threshold,
  rules, evidence, and lineage.
- [ ] Display shadow alerts to authorized analysts.
- [ ] Record `confirmed_collusion`, `false_positive`, and `inconclusive` reviews.
- [ ] Enforce `label_available_at`.
- [ ] Never convert inconclusive or unreviewed events into negatives.

### C4. Real-data evaluation boundary

- [ ] Define tenant/time-based train and validation windows.
- [ ] Create a later sealed forward-test window.
- [ ] Perform power/precision analysis for required positive counts.
- [ ] As an initial planning floor, seek a few hundred independently confirmed
  positives in each main validation and forward-test window.
- [ ] Report confidence interval width rather than relying on row count alone.
- [ ] Keep synthetic and real metrics separate.

### C5. Acceptance gate

Phase C is complete when:

- [ ] online and offline features match for real fixtures;
- [ ] checkpoint restart and Kafka replay remain idempotent;
- [ ] no synchronous database read occurs during scoring;
- [ ] tenant isolation and PII controls pass;
- [ ] delayed-label semantics pass;
- [ ] real-data drift and analyst-yield reports exist; and
- [ ] shadow results receive manual review before any enforcement proposal.

## 7. Phase D — Graph-derived CatBoost augmentation

Status: `BLOCKED` on Phase C real graph signals

Purpose: add relational information in the lowest-risk order before deploying a
real-time GNN.

Estimated effort after data readiness: 2–4 weeks.

### D1. Versioned graph scalar features

Create `pair-features-v2`; never alter the meaning or column order of v1.

Candidate prior-only features:

- [ ] common-neighbor count;
- [ ] shared device/network/session/resource counts;
- [ ] user and resource degree;
- [ ] suspicious-neighbor and two-hop counts;
- [ ] connected-component size;
- [ ] edge-type diversity;
- [ ] relationship and shared-resource recency;
- [ ] device/network churn; and
- [ ] accounts per shared resource.

### D2. Online/offline parity

- [ ] Build point-in-time graph scalars in Python.
- [ ] Build the same scalars in Flink keyed state or an asynchronous graph
  feature stream.
- [ ] Isolate equal timestamps.
- [ ] Exclude future/challenge edges.
- [ ] Add a complete online/offline parity checker.

### D3. CatBoost feature-ablation candidate

- [ ] Train CatBoost v2 with base plus graph features.
- [ ] Run base-only, graph-only, and combined ablations.
- [ ] Evaluate cold-start, temporal, and new-relationship benchmarks.
- [ ] Stop if scalar graph features show no stable incremental value.

### D4. OOF GraphSAGE augmentation

Only after D3 shows useful graph signal:

- [ ] Generate hand-grouped OOF GraphSAGE scores for training.
- [ ] Generate frozen validation/test scores.
- [ ] Test a simple calibrated fusion before embedding fusion.
- [ ] Consider graph embeddings only after graph-score lift succeeds.
- [ ] Keep the private challenge sealed until the public gate passes.

### D5. Acceptance gate

- [ ] Every graph edge precedes the scored hand.
- [ ] No raw-ID-only embeddings.
- [ ] Online/offline parity passes.
- [ ] Registered minimum relative PR-AUC gain is met.
- [ ] Paired bootstrap lower bound is positive.
- [ ] Recall at budget and F1 do not regress.
- [ ] Lift holds on both cold-start and new-relationship benchmarks.

## 8. Phase E — Richer deep sequence model

Status: `BLOCKED` on richer real temporal signals

Purpose: retest deep learning when its input contains information not already
summarized by CatBoost.

Estimated effort after data readiness: 2–3 weeks.

Do not simply retune the rejected 16-hand synthetic Transformer against the
same public test.

### E1. New sequence contract

Define `pair-sequence-features-v1` containing prior-only:

- [ ] raw action order;
- [ ] action and response timing;
- [ ] session boundaries and table movement;
- [ ] stake changes;
- [ ] device/network transitions;
- [ ] longer user and pair histories; and
- [ ] behavioral-change indicators.

### E2. Offline and online construction

- [ ] Build the training tokenizer from immutable events.
- [ ] Build the identical tokenizer in a stateful Flink operator.
- [ ] Publish sequence snapshots keyed by tenant and hand.
- [ ] Add exact token, mask, order, and normalization parity tests.
- [ ] Keep the Go scoring path free of database reads.

### E3. Training and serving

- [ ] Pretrain encoders using train histories only.
- [ ] Fine-tune on approved labels.
- [ ] Select checkpoint, calibration, and threshold on validation.
- [ ] Export ONNX or use one batched Triton request per hand.
- [ ] Add Go assembly, timeouts, readiness, and fallback behavior.
- [ ] Shadow serve before any promotion request.

### E4. Acceptance gate

- [ ] No future or equal-time sequence token.
- [ ] Online/offline sequence parity passes.
- [ ] New evaluation boundary or pre-registered hypothesis is used.
- [ ] Candidate beats the active graph-augmented champion.
- [ ] Paired improvement interval is positive.
- [ ] End-to-end p95 remains below the one-second target.

## 9. Phase F — Anomaly discovery and analyst AI

Status: `OPTIONAL` after the supervised shadow path is reliable

### F1. Anomaly discovery

- [ ] Train Isolation Forest, autoencoder, or density candidates without using
  anomaly as a synonym for collusion.
- [ ] Publish a versioned `poker.anomaly-signals.v1` event.
- [ ] Include reference window, feature version, score, and reason codes.
- [ ] Use anomaly output for candidate discovery and analyst sampling only.
- [ ] Measure independent analyst yield and incremental recall.
- [ ] Prohibit autonomous enforcement.

### F2. Analyst AI

- [ ] Generate summaries only from structured features, SHAP, rules, graph
  evidence, and approved record links.
- [ ] Require evidence IDs for every factual claim.
- [ ] Apply tenant isolation, access controls, and PII minimization.
- [ ] Prevent the LLM from changing model probabilities or thresholds.
- [ ] Store prompt/template/model version and analyst feedback.
- [ ] Keep a human decision maker in the workflow.

### F3. Acceptance gate

- [ ] Anomaly and LLM outputs are clearly marked as supporting evidence.
- [ ] No fabricated or untraceable evidence passes evaluation.
- [ ] No raw unrestricted PII reaches the LLM.
- [ ] No automated enforcement path exists.

## 10. Cross-cutting promotion gate

Every new candidate must pass all applicable gates in order:

1. Artifact integrity.
2. Dataset/split/label contract validation.
3. Leakage and point-in-time checks.
4. Public test quality.
5. Positive paired hand-bootstrap lower bound.
6. Recall-at-budget and F1 non-regression.
7. Required segment checks.
8. Sealed private challenge.
9. Online/offline parity.
10. Replay, restart, load, latency, and security checks.
11. Shadow verification on real data.
12. Manual approval.
13. Tested rollback.

The current minimum public candidate requirement is a registered 2% relative
PR-AUC improvement plus a positive paired lower confidence bound. Passing one
aggregate metric is not sufficient.

## 11. Definition of done for every phase

A phase is not complete until it has:

- [ ] reviewed contracts and versioning;
- [ ] unit tests;
- [ ] leakage tests where data is involved;
- [ ] deterministic/replay tests where streaming is involved;
- [ ] online/offline parity where features are involved;
- [ ] machine-readable metrics and evidence;
- [ ] artifact hashes;
- [ ] monitoring and alerts;
- [ ] documentation and operating commands;
- [ ] security and tenant review;
- [ ] rollback instructions; and
- [ ] an updated status in this plan.

## 12. Immediate implementation slice

Begin with Phase A only. It has no external dependency and does not alter the
production score.

First pull-request-sized slice:

1. [x] Add hand-grouped bootstrap utilities to `pipeline/ml/stability.py`.
2. [x] Test that one hand never splits across a bootstrap cluster.
3. [x] Build confidence intervals from the frozen CatBoost test predictions.
4. [x] Write `stability_report.json` with dataset/model/run identity.
5. [x] Add a checker that verifies counts, metrics, hashes, and challenge exclusion.
6. [x] Add stability build, check, and test Make targets.
7. [x] Add the targets to `phase12-check` after standalone checks pass.

Do not change the Go scorer, decision threshold, registry champion, or private
challenge during this slice.

## 13. Progress log

Add dated entries here as phases move:

| Date | Phase | Change | Evidence | Decision |
|---|---|---|---|---|
| 2026-07-20 | Planning | Created implementation follow-up plan | This document | Phase A is next |
| 2026-07-20 | A | Completed the first stability slice: exact hand-grouped bootstrap, hash-bound report, deterministic checker, tests, and Make integration | `make model-stability`; `make model-stability-check`; `make phase12-check`; PR-AUC `0.362918`, 95% CI `[0.255751, 0.494629]` from 1,000 hand draws | Phase A remains in progress; model card, segment intervals, and validation multi-seed evidence are next |
| 2026-07-20 | A validation | Targeted stability tests and the complete Phase 12 gate pass. The repository-wide `python -m pytest -q` currently stops during collection because the active base interpreter does not have the pinned `pokerkit==0.7.4` dependency installed. | `make model-stability-test`: 4 passed; `make phase12-check`: passed; `requirements.txt`: `pokerkit==0.7.4` | Dependency environment follow-up; this does not block the Phase A stability slice |
