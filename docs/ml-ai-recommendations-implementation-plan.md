# ML/AI recommendations implementation plan

This is the follow-up plan for implementing the recommendations in the
[Data science and ML/AI model development guide](data-science-model-development-guide.md).
It is the working checklist for engineering, data science, platform, and risk
operations.

## Plan control

| Field | Value |
|---|---|
| Plan version | 7 |
| Created | 2026-07-20 |
| Current production champion | `pair-catboost-v1`, run `pair_7a1c58c1046b` |
| Current feature definition | `pair-features-v1` |
| Automatic model promotion | Disabled |
| Immediate next phase | Phase B3 — first stateful Flink rule |

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

Status: `COMPLETE`

Purpose: quantify uncertainty around the champion and create a standard
evidence package for all future candidates without changing online scoring.

Estimated single-engineer effort: 4–6 working days.

### A1. New code and artifacts

Add:

```text
pipeline/ml/stability.py
pipeline/ml/model_card.py
pipeline/ml/seed_stability.py
pipeline/ml/scenario_holdout.py
scripts/build_model_stability_report.py
scripts/check_model_stability.py
scripts/build_model_card.py
scripts/check_model_card.py
scripts/build_validation_seed_stability.py
scripts/check_validation_seed_stability.py
scripts/build_scenario_holdout_report.py
scripts/check_scenario_holdout_report.py
tests/test_model_stability.py
tests/test_model_card.py
tests/test_seed_stability.py
tests/test_scenario_holdout.py
models/registry/stability_report.json
models/registry/model_card.json
models/registry/model_card.md
models/registry/validation_seed_stability.json
models/registry/scenario_holdout_report.json
models/registry/generator_scenario_lineage.parquet
```

Implementation status:

- [x] `pipeline/ml/stability.py`
- [x] `scripts/build_model_stability_report.py`
- [x] `scripts/check_model_stability.py`
- [x] `tests/test_model_stability.py`
- [x] Local generated `models/registry/stability_report.json`
- [x] `pipeline/ml/model_card.py`
- [x] `scripts/build_model_card.py`
- [x] `scripts/check_model_card.py`
- [x] `tests/test_model_card.py`
- [x] Local generated `models/registry/model_card.json`
- [x] Local generated `models/registry/model_card.md`
- [x] `pipeline/ml/seed_stability.py`
- [x] `scripts/build_validation_seed_stability.py`
- [x] `scripts/check_validation_seed_stability.py`
- [x] `tests/test_seed_stability.py`
- [x] Local generated `models/registry/validation_seed_stability.json`
- [x] `pipeline/ml/scenario_holdout.py`
- [x] `scripts/build_scenario_holdout_report.py`
- [x] `scripts/check_scenario_holdout_report.py`
- [x] `tests/test_scenario_holdout.py`
- [x] Local generated `models/registry/scenario_holdout_report.json`
- [x] Local generated private evaluation-only
  `models/registry/generator_scenario_lineage.parquet`

Add Make targets:

```text
model-stability
model-stability-check
model-card
model-card-check
model-seed-stability
model-seed-stability-check
model-scenario-holdout
model-scenario-holdout-check
```

- [x] Added `model-stability-test`.
- [x] Added `model-stability`.
- [x] Added `model-stability-check`.
- [x] Added `model-card-test`.
- [x] Added `model-card`.
- [x] Added `model-card-check`.
- [x] Added `model-seed-stability-test`.
- [x] Added `model-seed-stability`.
- [x] Added `model-seed-stability-check`.
- [x] Added `model-scenario-holdout-test`.
- [x] Added `model-scenario-holdout`.
- [x] Added `model-scenario-holdout-check`.
- [x] Extended `phase12-check` with stability tests and deterministic report
  verification.

Extend `phase12-check` to run the stability checker.

### A2. Statistical work

- [x] Compute 1,000-sample paired hand-bootstrap intervals for test PR-AUC.
- [x] Compute intervals for precision, recall, F1, Brier score, alert rate, and
  false positives per 1,000 hands.
- [x] Bootstrap by `hand_id`; never sample pair rows independently.
- [x] Report recall and precision at the fixed analyst alert budget.
- [x] Report validation robustness across at least five training seeds.
- [x] Keep seed robustness on train/validation; do not select a seed using test.
- [x] Add generator-seed and scenario-family holdout summaries.
- [x] Add segment metrics with counts and confidence intervals.
- [x] Suppress or mark segments below a configured reliability floor.
- [x] Record every stability configuration and random seed.

### A3. Model card

The machine-readable and Markdown-renderable model card must include:

- [x] prediction unit and intended use;
- [x] prohibited uses;
- [x] dataset IDs, hashes, splits, and label policy;
- [x] feature, preprocessing, model, calibration, and policy versions;
- [x] overall and segment metrics with counts;
- [x] synthetic-data limitation;
- [x] known failure modes;
- [x] drift reference and current drift status;
- [x] inference contract and latency evidence;
- [x] promotion and rollback identities; and
- [x] owner and review date.

### A4. Registry generalization

The current registry builder knows the champion and one ensemble candidate.
Generalize candidate registration so future tabular, graph, sequence, or hybrid
artifacts implement one common manifest contract.

- [x] Define a generic candidate evidence schema.
- [x] Require dataset, feature, predictions, metrics, and artifact hashes.
- [x] Preserve exactly one active champion per tenant/product/benchmark scope.
- [x] Reject a candidate whose operational report belongs to another run.
- [x] Preserve manual approval and rollback requirements.

The language-neutral contract is
[`schemas/model_candidate_evidence.schema.json`](../schemas/model_candidate_evidence.schema.json).
`pipeline/ops/candidate.py` validates the schema's cross-file identities and
hashes, while `pipeline/ops/registry.py` consumes only candidate evidence and
does not parse a model family's private metrics layout. The current CatBoost
and OOF-stack layouts are adapted at the script boundary.

### A5. Acceptance gate

Phase A is complete when:

- [x] existing CatBoost metrics reproduce from frozen predictions;
- [x] bootstrap output is deterministic for a fixed bootstrap seed;
- [x] private challenge datasets and labels are not opened;
- [x] model-card facts match the artifact and registry;
- [x] validation seed evidence opens only train/validation and does not select a
  best-looking seed;
- [x] material validation seed sensitivity is surfaced as a warning in the
  model card;
- [x] scenario lineage is private evaluation evidence and is never included in
  a model feature matrix;
- [x] every leave-one-family-out model removes the held-out family from both
  training and validation calibration; and
- [x] generator/scenario evidence is hash-bound, deterministically reproducible,
  challenge-free, and visible in the model card;
- [x] mutation of any tracked source or report evidence fails validation;
- [x] all new tests pass; and
- [x] `make phase12-check` includes and passes the new controls.

Production effect: none. CatBoost run `pair_7a1c58c1046b` remains active.

## 5. Phase B — Versioned Rules v2

Status: `IN PROGRESS`

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
observation_revision
severity
raw_score
evidence
effective_at
feature_definition_version
trace_id
```

Recommended Kafka topic: `poker.rule-evidence.v1`.

- [x] Add the Python event contract and forbidden-field validation.
- [x] Add the equivalent Go structure and validation.
- [x] Add schema examples and round-trip fixtures.
- [x] Use deterministic IDs so replay is idempotent.
- [x] Reference rule evidence from score and alert events.
- [x] Add Snowflake persistence with tenant and model-run lineage.

The contract, replay algorithm, safety boundary, Kafka routing, and warehouse
lineage are documented in
[`docs/rule-evidence-v1.md`](rule-evidence-v1.md). B1 established the contract;
B2 now populates the evidence-reference array from inference-safe pair
features without altering probability.

### B2. Port inference-safe pair rules to Go

Port the existing pair baseline first:

- [x] one player folded while the other won;
- [x] same device;
- [x] same network;
- [x] outcome asymmetry;
- [x] A-fold/B-win rate;
- [x] B-fold/A-win rate.

The current 58-value feature record already carries these inputs. They do not
need a database lookup or new Flink state.

- [x] Create Python/Go golden parity fixtures.
- [x] Emit structured evidence, not only a weighted sum.
- [x] Keep the rules-only benchmark independently measurable.
- [x] Store rule versions with every affected risk score.

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
- [x] CatBoost calibrated probability stays unchanged in Rules v2.
- [x] No manually weighted probability blend is introduced.

If probability fusion is proposed later, it must use OOF predictions and the
normal model promotion process.

### B5. Rule governance and evaluation

Every rule must have:

- [x] owner and description;
- [x] version and effective date;
- [x] unit and replay tests;
- [ ] precision, recall, firing rate, and alert-volume report;
- [ ] segment and drift monitoring;
- [ ] rollback procedure; and
- [ ] independent-label evaluation.

Labels created solely because a rule fired must be marked so that the same rule
is not evaluated against circular truth.

### B6. Acceptance gate

Phase B is complete when:

- [x] Python and Go rule outputs match exactly on golden fixtures;
- [x] Kafka replay produces the same evidence IDs and revisions;
- [x] no rule reads a database in the hot path;
- [x] all scores retain model, feature, policy, and rule versions;
- [ ] hard versus soft behavior is explicit;
- [ ] rule dashboards and alerts exist; and
- [x] CatBoost predictions remain bit-for-bit unchanged.

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

## 12. Execution slices and immediate next step

Phase A was implemented without altering the production score. The completed
slices are retained here as the engineering record.

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

Second pull-request-sized slice:

1. [x] Add versioned production-style segment definitions.
2. [x] Compute point metrics and hand-grouped intervals for reliable segments.
3. [x] Publish counts but suppress metrics below configured hand/class floors.
4. [x] Generate a machine-readable model card from governed evidence.
5. [x] Render Markdown from the same JSON card and validate exact parity.
6. [x] Bind model-card facts to dataset, model, registry, deployment, drift,
   operational, and stability artifacts.
7. [x] Add model-card tests and include the build/check sequence in
   `phase12-check`.

Do not interpret suppressed segment metrics as zero performance. The next data
generation work should increase independent positive coverage for those
segments before they are used for promotion decisions.

Third pull-request-sized slice:

1. [x] Add a separate allowlisted CatBoost trainer that opens only frozen train
   and validation parquet files.
2. [x] Refit the exact champion hyperparameters for five fixed unique seeds.
3. [x] Refit calibration and the alert-budget threshold independently on
   validation for each seed.
4. [x] Record per-seed prediction digests, metrics, best iteration, calibration,
   threshold, and aggregate statistics.
5. [x] Prohibit test/challenge reads and seed selection in the evidence contract.
6. [x] Add integrity, source-hash, deterministic recomputation, and mutation
   checks.
7. [x] Bind seed stability into the JSON/Markdown model card and Phase 12 gate.
8. [x] Scope Go's operational-test build cache to a writable temporary path so
   managed-workspace cache trimming cannot create false failures.

The result is a `warning`, not a passing stability claim: validation PR-AUC
varies from `0.145587` to `0.225296`, a `0.416905` relative spread versus the
configured `0.25` limit. Do not choose the best seed or replace the champion
from this result. Treat it as evidence that the rare-positive synthetic
validation window is sensitive to stochastic training.

Fourth pull-request-sized slice:

1. [x] Bind the pair dataset to its exact source-world manifest and generator
   seeds.
2. [x] Derive a private scenario-lineage sidecar from the generator's fixed
   round-robin pair-pattern assignment; do not add it to model features.
3. [x] Report champion metrics and hand-grouped intervals on independent
   validation seed `10042` and public-test seed `20042`.
4. [x] Train four leave-one-scenario-family-out CatBoost models with the
   champion hyperparameters and seed.
5. [x] Remove every held-out-family hand from both training and calibration,
   then evaluate only on normal plus held-out-family public-test hands.
6. [x] Compare unseen-family models with champion references and write
   hand-grouped confidence intervals.
7. [x] Add semantic/file hashes, mutation checks, deterministic retraining,
   model-card binding, tests, and Phase 12 integration.

Unseen-family PR-AUC ranges from `0.078057` for `soft_play` to `0.429689`
for `fold_benefit`. The four family test slices contain only 15–23 positives,
so their intervals remain wide. Treat these as scenario-generalization
evidence, not as independently promotable models.

Fifth pull-request-sized slice:

1. [x] Publish a language-neutral candidate evidence JSON Schema.
2. [x] Bind dataset, feature, metrics, predictions, model artifacts, and their
   hashes into each candidate document.
3. [x] Make registry schema v2 consume the common contract instead of
   model-family-specific metric layouts.
4. [x] Enforce one active champion per tenant/product/benchmark scope.
5. [x] Reject mismatched operational model, run, artifact, or report hashes.
6. [x] Retain manual approval, disabled automatic promotion, and exact rollback
   identity.
7. [x] Adapt the current CatBoost and OOF-stack artifacts and add the registry
   tests to `phase12-check`.

Completed slice — Phase B1:

1. [x] Define `poker.rule-evidence.v1` in Python and Go.
2. [x] Specify tenant/product scope, deterministic event IDs, rule ownership,
   version/effective time, entity and hand identity, severity, raw score,
   structured evidence, feature version, and trace lineage.
3. [x] Reject labels, private challenge fields, final model probability, and
   decision-policy output from the rule-evidence payload.
4. [x] Add shared JSON fixtures and Python/Go round-trip and replay-idempotency
   tests.
5. [x] Reference rule-evidence IDs from risk-score and alert contracts without
   changing CatBoost probability.
6. [x] Add the Snowflake table and tenant/model-run lineage migration only
   after the event contract passes locally.

Completed slice — Phase B2:

1. [x] Freeze rule IDs, owners, versions, thresholds, severities, and effective
   dates for the six inference-safe pair rules.
2. [x] Implement a pure Python reference evaluator using only the current-hand,
   context, and prior pair-history feature groups.
3. [x] Implement the matching Go evaluator before model inference.
4. [x] Produce one structured `RuleEvidenceEvent` per fired rule and attach its
   deterministic ID to the hand's score and optional alert.
5. [x] Publish rule evidence in the same acknowledged output batch as the score
   so offsets cannot commit partial audit evidence.
6. [x] Add Python/Go golden parity fixtures, replay tests, output-order tests,
   and probability-invariance tests.
7. [x] Keep a separately measurable rules-only benchmark; do not create a
   manually weighted probability blend.

Immediate next slice — Phase B3:

1. [ ] Select one stateful pattern for the first vertical slice; start with
   repeated fold-to-partner wins in a bounded event-time window.
2. [ ] Freeze its rule identity, semantics, key, window, watermark, lateness,
   TTL, correction behavior, severity, and rollout date.
3. [ ] Add an offline Python reference evaluator over ordered pair events.
4. [ ] Add matching keyed Java/Flink state and emit the existing
   `poker.rule-evidence.v1` contract.
5. [ ] Prove duplicate, restart, late-event, correction, and Python/Flink
   replay parity before enabling the rule in a deployed job.
6. [ ] Add firing-rate, event-time lag, state-size, and checkpoint metrics.

## 13. Progress log

Add dated entries here as phases move:

| Date | Phase | Change | Evidence | Decision |
|---|---|---|---|---|
| 2026-07-20 | Planning | Created implementation follow-up plan | This document | Phase A is next |
| 2026-07-20 | A | Completed the first stability slice: exact hand-grouped bootstrap, hash-bound report, deterministic checker, tests, and Make integration | `make model-stability`; `make model-stability-check`; `make phase12-check`; PR-AUC `0.362918`, 95% CI `[0.255751, 0.494629]` from 1,000 hand draws | Phase A remains in progress; model card, segment intervals, and validation multi-seed evidence are next |
| 2026-07-20 | A validation | Targeted stability tests and the complete Phase 12 gate pass. The repository-wide `python -m pytest -q` currently stops during collection because the active base interpreter does not have the pinned `pokerkit==0.7.4` dependency installed. | `make model-stability-test`: 4 passed; `make phase12-check`: passed; `requirements.txt`: `pokerkit==0.7.4` | Dependency environment follow-up; this does not block the Phase A stability slice |
| 2026-07-20 | A | Added versioned segment definitions, configured reliability floors, whole-hand confidence intervals, and suppression for statistically thin slices. Added a hash-bound JSON model card and generated Markdown view tied to dataset, model, stability, registry, deployment, drift, and operational evidence. | `make phase12-check`: passed; 6/11 segments reliable, 5/11 suppressed; model-card hashes, identities, and Markdown parity passed; stability tests: 5 passed; model-card tests: 1 passed | Phase A remains in progress; validation multi-seed evidence, generator/scenario holdouts, and generic registry schema are next |
| 2026-07-21 | A | Added validation-only five-seed CatBoost robustness evidence and bound its warning into the governed model card. The loader allowlists only train/validation; deterministic recomputation passed without test, challenge, or stored prediction reads. Also moved the Go operational build cache to a writable temporary directory for managed-workspace execution. | Seeds `11,23,42,67,101`; validation PR-AUC `[0.145587, 0.225296]`; relative spread `0.416905`; status `warning`; seed tests: 2 passed; `--recompute`: passed; `make phase12-check`: passed | Do not select the best seed or change production. Phase A remains in progress; generator/scenario holdouts and generic registry schema are next |
| 2026-07-21 | A | Added independent generator-seed summaries and four true leave-one-scenario-family-out CatBoost evaluations. Scenario lineage is stored only in a private evaluation sidecar; held-out hands are absent from training/calibration and challenge is never loaded. Bound results into the governed model card. | Generator seeds: train `42`, validation `10042`, test `20042`; unseen-family PR-AUC: `soft_play=0.078057`, `chip_dump=0.318533`, `squeeze_collude=0.230247`, `fold_benefit=0.429689`; 300 hand bootstraps; scenario tests: 2 passed; deterministic `--recompute`: passed; `make phase12-check`: passed | No scenario model selected and no production change. Phase A remains in progress; generic candidate registry schema is next |
| 2026-07-21 | A | Completed registry generalization with a JSON Schema and hash-bound model-family-neutral candidate evidence. Registry schema v2 enforces one champion per tenant/product/benchmark scope, exact operational run binding, manual approval, immutable candidate evidence, and rollback identity. Current model-specific metric paths exist only in the legacy adapter. | Registry tests: 6 passed; real registry build/check: passed; complete `make phase12-check`: passed; production remains `pair-catboost-v1:pair_7a1c58c1046b`; OOF stack remains `rejected` | Phase A complete with no production change. Phase B rule-evidence contract is next |
| 2026-07-21 | B1 | Added `poker.rule-evidence.v1` in Python and Go, a shared cross-language UUIDv5 fixture, recursive label/model/policy leakage rejection, score/alert evidence references, managed Kafka topic configuration, and idempotent warehouse event plus score/model-run lineage persistence. | `make phase-b1-check`: 14 Python tests and Go risk/stream suites passed; `go test ./...`: passed; `make phase12-check`: passed; shared revision-aware ID `8bcfb4e4-2113-52c3-85c2-a6ca4cb19823`; migration `011_rule_evidence.sql` exercised through DuckDB | B1 complete. No rules are evaluated and CatBoost probability is unchanged; B2 Go pair-rule parity is next |
| 2026-07-21 | B2 | Froze six pair-rule definitions, added a pure Python reference and matching pre-inference Go evaluator, retained the exact rules-only formula, emitted deterministic evidence with score/alert references, and published evidence before its score and alert in one acknowledged batch. | `make phase-b2-check`: 18 Python tests plus Go risk/stream suites passed; cross-language golden score `0.8475`; replay IDs matched and higher snapshot revisions produced distinct IDs; enabled/disabled Go scorer probabilities and decisions matched; `go test ./...`: passed; `make phase12-check`: passed | B2 complete with no probability blend and no production deployment change. Phase B remains in progress; the first stateful Java/Flink rule is next |
