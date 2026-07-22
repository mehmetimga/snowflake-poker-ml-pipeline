# ML/AI recommendations implementation plan

This is the follow-up plan for implementing the recommendations in the
[Data science and ML/AI model development guide](data-science-model-development-guide.md).
It is the working checklist for engineering, data science, platform, and risk
operations.

## Plan control

| Field | Value |
|---|---|
| Plan version | 19 |
| Created | 2026-07-20 |
| Current production champion | `pair-catboost-v1`, run `pair_7a1c58c1046b` |
| Current feature definition | `pair-features-v1` |
| Automatic model promotion | Disabled |
| Immediate next phase | Authenticate Snowflake, release `poker-adapter:8e7475782fe4`, deploy `POKER_ADAPTER_SIM`, and run the prepared offset-bounded Confluent replay |

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

Status: `COMPLETE`

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

- [x] repeated fold-to-partner wins in a time window;
- [ ] persistent one-direction chip flow;
- [ ] synchronized table or session movement;
- [ ] coordinated action timing;
- [ ] changing shared device/network relationships.

The first vertical slice is complete. Additional candidates remain deferred
until shadow data shows they add useful, non-duplicative evidence. For the
implemented repeated-fold rule:

- [x] define key, state, watermark, lateness, TTL, and correction semantics;
- [x] add duplicate, stale-revision, correction, late-event, and restart tests;
- [x] add Python-offline/Java-online golden parity; and
- [x] expose evaluation, firing, duplicate, correction, stale, late-event,
  event-time-lag, and state-size metrics.

The rule is soft evidence only. Flink embeds its complete governed evidence in
the pair-feature event; Go validates it and publishes evidence before the score
and alert in the existing acknowledged output batch. It does not alter the
58-value CatBoost vector or calibrated probability.

### B4. Decision-policy separation

- [x] Hard policy rules create mandatory review with explicit reasons.
- [x] Soft behavioral rules attach evidence and analyst filters.
- [x] Data-quality violations remain DLQ events, not fraud evidence.
- [x] CatBoost calibrated probability stays unchanged in Rules v2.
- [x] No manually weighted probability blend is introduced.

If probability fusion is proposed later, it must use OOF predictions and the
normal model promotion process.

### B5. Rule governance and evaluation

Every rule must have:

- [x] owner and description;
- [x] version and effective date;
- [x] unit and replay tests;
- [x] precision, recall, firing rate, and alert-volume report;
- [x] segment and drift monitoring;
- [x] rollback procedure; and
- [x] independent-label evaluation.

Labels created solely because a rule fired must be marked so that the same rule
is not evaluated against circular truth.

The frozen inputs, metrics, reliability rules, monitoring thresholds, observed
results, and executable Go/Flink rollback are documented in
[`docs/rule-governance-evaluation.md`](rule-governance-evaluation.md).

### B6. Acceptance gate

Phase B is complete when:

- [x] Python and Go rule outputs match exactly on golden fixtures;
- [x] Kafka replay produces the same evidence IDs and revisions;
- [x] no rule reads a database in the hot path;
- [x] all scores retain model, feature, policy, and rule versions;
- [x] hard versus soft behavior is explicit;
- [x] rule dashboards and alerts exist; and
- [x] CatBoost predictions remain bit-for-bit unchanged.

The runtime metrics, delayed-label window contract, deterministic alerts,
Prometheus rules, Grafana dashboard, and Streamlit page are documented in
[`docs/rule-monitoring-and-alerting.md`](rule-monitoring-and-alerting.md).

## 6. Phase C — Target runtime and real-data shadow evaluation

Status: `COMPLETE` for C1 and C2 local contract/runtime/packaging plus
PostgreSQL/Debezium simulation. Immutable images,
governed model artifacts, live SPCS services, periodic checkpoints, two-job
savepoint restore, post-restore online/offline parity, scorer recovery, and a
bounded end-to-end replay are verified. The proposed outbox/envelope contract,
Python and Go mapping, binary codec seam, lineage, parity fixtures, and the Go
publish-or-DLQ-before-commit runtime, isolated simulation topics, adapter
image, private SPCS specification, local logical-WAL source, database filter,
Debezium connector, deterministic Protobuf codec, and bounded Kafka replay are
also verified. Remote Confluent topics and replay tooling are ready; the image
push, SPCS deployment, and bounded live replay are `NEXT`. Real poker-server
ingestion is outside the current project scope;
real-data C3–C5 remain deferred until independently reviewed data and labels
exist.

Purpose: measure the current pipeline on real poker-server data without
automated enforcement.

Estimated engineering effort after access and schemas exist: 2–3 weeks.
Label collection will take longer than implementation.

### C1. Target deployment

- [x] Package Java 17/Flink as `poker-flink:<git-sha>`.
- [x] Package Go scoring as `poker-risk:<git-sha>`.
- [x] Deploy separate long-running `POKER_FLINK` and `POKER_RISK` SPCS services.
- [x] Configure durable checkpoint storage and verify both live jobs checkpoint.
- [x] Complete a controlled savepoint restore for both Flink jobs.
- [x] Store artifacts in a controlled Snowflake stage/registry URI, not a local
  absolute path.
- [x] Add service readiness, lag, watermark, checkpoint, state, and latency
  monitoring.

The image contents, minimal runtime model bundle, SPCS service specs, block
storage boundary, clean-commit release guard, deployment sequence, and local
evidence are documented in
[`docs/spcs-c1-deployment.md`](spcs-c1-deployment.md). The restore drill used
two explicit savepoints, restored both Kafka sources and keyed state, completed
new checkpoints, and passed a collision-aware post-restore replay.

### C2. CDC-shaped simulation boundary

- [x] Freeze a proposed insert-only PostgreSQL hand-completed outbox and raw
  Debezium PostgreSQL envelope without applying it to the external database.
- [x] Convert CDC fixtures into the canonical hand-event contract in Python and
  Go with identical event/trace IDs, topic, key, payload, and canonical headers.
- [x] Preserve source transaction/LSN plus Kafka position as `cdc_*` headers and
  in `RAW_EVENT_ENVELOPES.source_lineage` for replay and audit.
- [x] Verify direct-publish, retry, Kafka Connect wrapper, and snapshot fixtures
  produce equivalent canonical hands.
- [x] Keep the poker server, PostgreSQL, and Debezium outside this ML repository
  and SPCS deployment boundary.
- [ ] Deferred/out of current scope: obtain a real poker-server schema, binary
  codec, connector, and independently reviewed golden fixtures.
- [x] Complete the Go Kafka consume/publish/DLQ loop with sanitized deterministic
  failures, Prometheus metrics, explicit fixture-codec opt-in, and
  publish-before-commit recovery tests.
- [x] Package the adapter in its own immutable Docker image and private
  simulation-only SPCS service specification.
- [x] Build a deterministic PokerKit writer backed by a real local PostgreSQL
  logical-WAL source, transactional game-type-filtered outbox, and Debezium
  connector.
- [x] Define `poker-hand-protobuf-v1`, generate Python/Go bindings, remove
  private truth before encoding, and verify one shared binary fixture in both
  languages.
- [x] Prove locally that 8 source rows become exactly 4 allowlisted CDC records,
  4 acknowledged canonical records, 0 DLQ records, and 4 pre-Kafka exclusions.
- [x] Add exact checksum, malformed-Protobuf, game-mismatch, unknown-codec,
  pre-Kafka filter, sanitized-DLQ, and live commit-recovery scenarios locally.
- [ ] Repeat the accepted manifest through isolated Confluent topics and
  `POKER_ADAPTER_SIM`.

The frozen mapping, ownership boundary, operation policy, connector settings,
and remaining simulation gates are documented in
[`docs/debezium-hand-history-ingress.md`](debezium-hand-history-ingress.md).

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

Completed slice — Phase B3:

1. [x] Select one stateful pattern for the first vertical slice; start with
   repeated fold-to-partner wins in a bounded event-time window.
2. [x] Freeze its rule identity, semantics, key, window, watermark, lateness,
   TTL, correction behavior, severity, and rollout date.
3. [x] Add an offline Python reference evaluator over ordered pair events.
4. [x] Add matching checkpointed keyed Java/Flink state and carry the existing
   `poker.rule-evidence.v1` contract to the Go transactional output boundary.
5. [x] Prove duplicate, restart, late-event, correction, and Python/Flink
   replay parity before enabling the rule in a deployed job.
6. [x] Add evaluation/firing counters, event-time lag, keyed state size, and
   checkpointed state coverage.

Completed slice — Phase B4:

1. [x] Define a versioned decision-policy contract separate from model scores
   and rule evidence.
2. [x] Classify every current B2/B3 rule as soft evidence; configure no hard
   enforcement rule until risk owners approve one.
3. [x] Define mandatory-review semantics for any future hard policy rule,
   including explicit reason codes and independent audit records.
4. [x] Keep malformed/late data on quality and DLQ paths rather than turning
   it into fraud evidence.
5. [x] Add Go policy evaluation, deterministic decision IDs, replay tests,
   rule-version lineage, and probability-invariance tests.
6. [x] Add policy firing-rate and review-volume gates before any shadow
   deployment.

Completed slice — Phase B5:

1. [x] Build a deterministic rule-evaluation dataset from frozen public-test
   features and independently available labels.
2. [x] Report per-rule support, firing rate, precision, recall, and alert
   volume with whole-hand confidence intervals.
3. [x] Mark label provenance and exclude labels caused solely by the evaluated
   rule from its own quality report.
4. [x] Add tenant/context/scenario segment summaries and reliability floors.
5. [x] Define rule disable/rollback configuration and prove replay after a
   rollback does not change stored model probability.
6. [x] Emit machine-readable monitoring thresholds for firing-rate and rule
   drift dashboards.

Completed slice — Phase B6:

1. [x] Export runtime firing, evidence-volume, label-yield, lateness, and state
   metrics to the target monitoring system.
2. [x] Build rule and segment dashboards from the B5 baseline contract.
3. [x] Alert on eligible-window threshold violations while marking thin
   windows `insufficient_data`.
4. [x] Run alert tests with synthetic drift and verify links to the exact rule,
   rollout, model, tenant, and evaluation versions.
5. [x] Keep all current rules in shadow evidence mode; dashboards do not grant
   enforcement authority.

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
| 2026-07-21 | B3 | Implemented the first stateful rule, `pair.repeated-fold-to-partner-wins`, as a 24-hour event-time window with scoped keyed state, deterministic corrections, lateness handling, 72-hour TTL, metrics, and Python/Java replay parity. Flink carries complete evidence to Go, which validates and publishes it atomically with the corresponding score/alert batch. | `make phase-b3-check`: 22 Python tests, 7 Java tests, and Go risk/stream suites passed; shared golden evidence IDs matched across Python and Java; checkpoint restore, duplicate, correction, stale, late-event, transport-validation, and probability-invariance paths passed | First B3 vertical slice complete with no deployed-job or probability change. Other stateful candidates are deferred pending shadow evidence; B4 decision-policy separation is next |
| 2026-07-21 | B4 | Added `poker.review-routing:v1` and an independent `poker.review-decision.recorded` audit contract. All seven current rules are explicitly soft, the hard list is empty, hypothetical hard rules require review with deterministic reasons, and quality failures remain DLQ-only. Go now publishes evidence, score, review decision, and optional policy-linked alert in one acknowledged batch. | `make phase-b4-check`: 32 Python tests, 7 Java tests, and Go risk/stream suites passed; Python/Go golden decision ID `23300633-c2ac-5284-b110-039fe2850d03`; Kafka replay, output order, hard-below-threshold routing, soft-rule non-action, DLQ isolation, rollout metrics, and probability invariance passed | B4 complete in local shadow configuration. No hard rule, automatic enforcement, probability change, or deployment change. B5 governed rule evaluation is next |
| 2026-07-21 | B5 | Added a hash-bound public-test report for all seven rules, independent-label provenance controls, whole-hand intervals, reliable tenant/context/scenario/history slices, machine-readable monitoring baselines, and executable per-rule rollback in Go and Flink. | 75,000 rows/5,000 hands; 500 hand bootstraps per rule; 7/7 overall reports reliable; 51 reliable and 26 suppressed rule/segment slices; all 75,000 labels independently synthetic; all-rule rollback probability delta `0.0`; `make phase-b5-check` passed with 8 Java tests and all Python/Go suites; `make phase12-check` passed | B5 complete locally. Results confirm rules remain soft shadow evidence; no champion, threshold, hard rule, or deployed service changed. B6 dashboards and alerts are next |
| 2026-07-21 | B6 | Completed operational Rules v2 monitoring with acknowledged Go runtime counters, existing Flink state/lateness signals, a hash-bound delayed-label window and report, deterministic lineage-rich alerts, Prometheus rules, a Grafana dashboard, and a Streamlit admin page. Thin windows are explicitly `insufficient_data`; circular or unknown labels are quarantined; no alert can disable a rule or enforce an action. | Stable frozen replay: 5,000 hands, 75,000 pair rows, 7/7 rules `ok`, 0 alerts; synthetic thin, drift, bad-label, integrity, and admin-loader tests passed; `make phase-b6-check` passed with 8 Java tests and all focused Python/Go suites; `make phase12-check` passed | Phase B complete locally with all rules still in soft shadow mode and model probability unchanged. Phase C1 SPCS packaging is next; real-data C2–C5 remain blocked on CDC and independently reviewed labels |
| 2026-07-21 | C1 packaging | Added separate multi-stage amd64 `poker-risk` and `poker-flink` images, a hash-verified seven-file serving bundle, pinned localhost Triton sidecar contract, private readiness/metrics endpoints, three-container Flink session service, 20 GiB checkpoint/savepoint block volume, retained deletion snapshots, governed Kafka secrets, stage mounts, and a clean-commit release guard. | `make phase-c1-check`: 15 deployment tests, all Go packages, 6 context-enrichment Java tests, 8 pair-feature Java tests, and both shaded packages passed; `make c1-build` produced amd64 images; `make c1-image-smoke` loaded the 58-feature model bundle and both Flink jobs with Prometheus/RocksDB modules; `make snow-status` confirmed only legacy `poker-pipeline:dev`, `POKER_ADMIN`, and `POKER_REALTIME` are remote | C1 packaging complete locally. No C1 image was pushed, model uploaded, `POKER_FLINK`/`POKER_RISK` service deployed, or production behavior changed. The existing pool was already active because legacy services are running. Clean commit, registry release, live deployment, and savepoint/replay drill remain |
| 2026-07-21 | C1 deployment | Released risk image `21ebb31c01d6`, Flink hotfix `603ff5dbd89f`, and pinned Triton `25.12-py3`; uploaded the hash-verified run bundle; deployed separate `POKER_FLINK` and `POKER_RISK` services; created the missing governed rule-evidence and review-decision topics; added validated scorer-group cutover and live PokerKit time anchors. | All five service containers ready; both Flink jobs `RUNNING`; post-replay checkpoint 5 completed for each job; 140/140 canonical target-plus-watermark events acknowledged; target output was exactly 150 pair rows for 10 complete hands, 10 unique 15-pair/6-player scores, and 10 valid review decisions with zero broken references; current scorer run had zero stream-stop events | Live C1 deployment and bounded replay accepted. Excessive-disorder and future-clock negative runs were isolated and not used as acceptance evidence. C1 remains in progress only for the controlled two-job savepoint restore drill; C2–C5 remain blocked |
| 2026-07-21 | C1 restore | Added a private SPCS savepoint controller, took one non-cancelling savepoint per live Flink job, deployed Flink `42fe62acc2d1`, and restored both Kafka split offsets and keyed RocksDB state. | Context savepoint `savepoint-b9cfc0-0055eed2478b`; pair savepoint `savepoint-3afb63-00d800620756`; restored jobs `5aa19a1c1043c6078bb5725cf15c3ff3` and `4a461ee41f659f4da98734b8eaf5f8a8`; both jobs completed new checkpoints; `make phase-c1-check` passed with 30 packaging/controller tests, all Go packages, 6 context tests, and 9 pair tests | Controlled two-job restore gate complete; post-restore stream invariants still required before C1 closure |
| 2026-07-21 | C1 acceptance | Found and stopped an orphan local Flink smoke process that was also writing the governed enrichment topic; added same-ID/different-payload collision checks, exact-retry reporting, causal pair validation, and causal logical score time. Released risk `8807659415f7` without changing the model, threshold, or rule rollout. | Final dataset: 29/29 canonical inputs acknowledged; enrichment `24` raw/`24` unique/`0` duplicates; pair output `60` rows/4 hands with offline parity passed; 4 scores, 4 decisions, 22 evidence records, 0 broken references; all scores contain 15 pairs and 6 players; scorer lag is zero on all 6 partitions; risk and Triton containers ready | C1 complete. The first `ede123f11f5a` risk rollout was an immutable unsuccessful intermediate and was replaced, not overwritten. C2 live CDC and C3–C5 real-data evaluation remain blocked; contract/fixture readiness can proceed |
| 2026-07-21 | C2 readiness | Froze a proposed immutable hand-completed outbox, raw Debezium PostgreSQL envelope, base64/checksum boundary, exact codec registry, deterministic canonical mapping, source headers, and warehouse audit lineage. Added shared fixtures and matching Python/Go adapters; widened canonical v1 `generator` to accept `poker-server` without changing existing PokerKit records. | `make phase-c2-readiness-check`: 12 Python tests, 5 Go tests, and deterministic fixture report passed; direct and CDC event ID `f00d27af-a72b-58bd-8180-14d6e38d3040`, trace ID `e6dae691-09f7-523b-aece-0fa0a67d3609`, LSN `270113177`, tx `9001`; snapshot/replay, plugin codec, mutation, tombstone, checksum, schema drift, private truth, and DuckDB audit paths passed | Offline C2 contract readiness complete. No PostgreSQL, Debezium, connector, Kafka record, Docker image, SPCS service, model, threshold, or rule changed. Go service loop/DLQ is next; real codec and live deploy remain blocked on poker-platform inputs |
| 2026-07-22 | C2 runtime | Added the Go hand-adapter polling command and processor, canonical plus lineage-header transport, deterministic hash-only DLQ contract, publish-or-DLQ-before-commit ordering, fail-closed codec startup, bounded execution, graceful shutdown, and Prometheus health/metrics. | `make phase-c2-runtime-check` passed: 12 Python contract/audit tests, 11 Go CDC mapping/runtime tests, Kafka header bridge tests, existing stream tests, command compilation, and deterministic parity report; canonical/DLQ publish failures never committed, and commit-failure replay was byte-identical | Offline C2 runtime readiness complete. No Kafka record, Docker image, SPCS service, model, threshold, or rule changed. Adapter image/spec packaging is next; the real decoder and live integration remain blocked on poker-platform inputs |
| 2026-07-22 | C2 packaging | Scoped C2 to synthetic simulation, added strict production/simulation topic isolation, paired fixture-codec guards, a non-root digest-pinned `poker-adapter` image, private `POKER_ADAPTER_SIM` spec, dedicated Snowflake-secret injection, render/build/smoke/release targets, and a clean-commit deployment guard. | `make phase-c2-packaging-check` passed the contract/runtime gate plus 17 deployment tests; local image `poker-adapter:dev-eebb871d5c45` built as `linux/amd64`, image ID `sha256:79f95e55cfe5ce0525e70fae2186debfe74c900709a3e9e057dca116e91218b1`, user `65532:65532`, and its embedded command passed smoke testing. Simulation requires exact `poker.sim.*` topics and a `sim-*` dataset before Kafka is opened. | Offline packaging complete. No image was pushed and no SPCS/Kafka/Snowflake service changed. The deterministic real-time simulator and isolated live replay are next; real poker-server ingestion is outside current scope. |
| 2026-07-22 | C2 local CDC simulation | Added a real local PostgreSQL 17.5 logical-WAL source, transactional outbox trigger with a database-owned game-type allowlist, Debezium 3.6 connector, deterministic PokerKit writer, `poker-hand-protobuf-v1` Python/Go codecs, run-scoped verifier, explicit topic creation, and reusable `make cdc-sim-*` operations. Filtering uses trusted columns before Kafka; checksum verification and binary parsing stay in the Go adapter after Kafka. | `make cdc-sim-e2e` passed twice without deleting prior data. Accepted run: 8 source rows across 4 game types, 4 outbox/CDC rows (`NLH_CASH_6MAX` and `NLH_TOURNAMENT_6MAX`), 4 filtered rows, 4 canonical outputs, 0 DLQ; Go metrics reported `InputRecords=4`, `CanonicalPublished=4`, `CommittedRecords=4`. All 223 Python tests, all Go packages, Compose validation, shared Protobuf SHA-256 `bc2eef1b6c3571e178c8c50e13663a82e1687de7c40b0ddbeb54b28c3be7b7a4`, and C2 package/render gates passed. Rebuilt image `poker-adapter:cce0f33294ec` is `linux/amd64`, non-root `65532:65532`, and image ID `sha256:41d006c7973d721858785dea364b5fa682303e24b6ed26366c6e10d16b5b98eb`; entrypoint smoke passed. | Local C2 simulation complete. Containers remain local; no Confluent record, Snowflake object, image push, SPCS deployment, model, threshold, rule, or production topic changed. Fault manifests and isolated Confluent/SPCS shadow replay are next; the real poker-server codec remains deferred. |
| 2026-07-22 | C2 fault and recovery | Added a six-row deterministic fault manifest, run-scoped canonical/DLQ verifier, retained-volume schema migration, stable-consumer readiness barrier, and a simulation-only fail-first-commit wrapper that production mode rejects. One scenario is canonical, one is filtered before Kafka, and four poison records exercise checksum, malformed Protobuf, row/binary game mismatch, and unknown codec. | Live fault run: 6 source, 1 filtered, 5 CDC inputs, 1 canonical, 4 committed sanitized DLQs with the four exact codes, 0 raw-value/hand-ID leaks; adapter metrics were `InputRecords=5`, `CanonicalPublished=1`, `DeadLetters=4`, `CommittedRecords=5`. Recovery run published once, failed before committing source offset `19`, proved committed offset remained `19`, restarted the same group, published a byte-identical retry with one stable event ID, and committed offset `20`. The initial sleep-based consumer race was reproduced and removed with an explicit stable-assignment barrier. All 224 Python tests, all Go packages, Compose/config validation, and the complete C2 gate passed. Local image `poker-adapter:dev-653be3a18da1` is `linux/amd64`, non-root `65532:65532`, image ID `sha256:4e5b089d42d5ab98ccd4404cb900d92eafd23bee9f60aea8d212a264362ba8a7`, and passed entrypoint smoke. | Local fault/replay gate complete. Failure injection is impossible outside explicit simulation mode. The development image is not releaseable from a dirty tree. No image was pushed and no Confluent, Snowflake, SPCS, model, threshold, rule, or production topic changed. Isolated Confluent/SPCS replay is next. |
| 2026-07-22 | C2 remote preparation | Added managed definitions for the exact three `poker.sim.*` topics, a simulation-only Snowflake Secret/network-rule/EAI path, actual-local-Debezium to Confluent replay, a six-scenario run manifest, consumer-group commit wait, and output-offset-bounded canonical/DLQ verification. Forced commit failure remains local-only and cannot be enabled by the SPCS spec. | Created the three isolated Confluent topics with source/output/DLQ partitions `1/3/3` and seven-day retention; discovered and locally configured the bootstrap plus 12 advertised broker endpoints; 22 focused Python tests and all Go adapter packages passed. Built `poker-adapter:8e7475782fe4` as `linux/amd64`, non-root `65532:65532`, image ID `sha256:49457879a9c01c679b3f8763af397001557192d8623aabd093b1f03d25ec5b75`, and smoke-tested its embedded build identity. | Remote preparation is in progress. Snowflake rejected the expired cached MFA token, so no image was pushed, Secret/EAI/service created, or remote CDC record published. Run MFA login, commit/push this orchestration slice, then perform the guarded release/deploy/replay. No production topic changed. |
