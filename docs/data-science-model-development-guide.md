# Data science and ML/AI model development guide

This document explains how a data scientist should develop, evaluate, and
operate models in the poker collusion pipeline. It covers dataset splits,
leakage, metrics, the current CatBoost champion, deterministic rules, deep
learning, graph learning, ensembles, anomaly detection, analyst feedback, and
model promotion.

It is both an onboarding guide and an implementation plan. Statements marked
**implemented** describe code or artifacts already in this repository.
Statements marked **next** are recommended additions; they are not claims about
the current production path.

## 1. Current decision in one page

The prediction unit is an unordered pair of players in one six-player hand:

```text
P(pair is coordinating | current hand, prior behavior, user context)
```

A six-player hand produces 15 pair examples. The current real-time model input
for each pair is a versioned 58-value vector. The Go scorer assembles all 15
pairs into a `[15, 58]` request and produces pair, player, and hand risk.

The current decision is:

| Item | Status |
|---|---|
| Production champion | CatBoost `pair-catboost-v1`, run `pair_7a1c58c1046b` |
| Public cold-start test PR-AUC | `0.362918` |
| Private challenge PR-AUC | `0.374749` |
| Real-time format | CatBoost exported to ONNX; inference in Go or optional Triton |
| Rules | Implemented as deterministic baselines; useful as evidence but not a model replacement |
| Tabular deep learning | Tested; all three candidates failed promotion |
| Multi-hand Transformer | Tested; failed promotion |
| Temporal GraphSAGE | Tested; useful relational signal but failed promotion |
| OOF ensemble | Tested; failed promotion |
| Production data claim | None yet; results are from a frozen synthetic benchmark |

CatBoost remains the champion because it has the strongest measured quality,
not because tree models are permanently preferred. Every future method must
earn promotion using the same untouched data and operational gates.

## 2. End-to-end ML/AI placement

The online and offline paths have different responsibilities.

```text
ONLINE: score without labels

PokerKit now; poker server + Debezium later
                    |
                    v
            Confluent Cloud Kafka
                    |
                    v
         Java/Flink feature pipeline
       - event-time context join
       - prior-only user/pair state
       - six players -> 15 pairs
                    |
                    v
          poker.pair-features.v1
                    |
          +---------+----------+
          |                    |
          v                    v
   deterministic rules    Go model scorer
   and evidence signals   CatBoost ONNX today
          |               future promoted models
          +---------+----------+
                    |
                    v
        versioned decision policy
       - hard-policy overrides
       - calibrated threshold
       - pair -> player -> hand
                    |
          +---------+----------+
          v                    v
     risk scores          risk alerts
          |                    |
          +---------+----------+
                    v
       Snowflake + admin review UI


OFFLINE: learn only after labels are available

Immutable events + point-in-time features + delayed labels
                    |
                    v
         frozen dataset and manifest
                    |
       +------------+-------------+
       |            |             |
       v            v             v
   CatBoost    DL/sequence     graph models
       |            |             |
       +------------+-------------+
                    |
     validation calibration/threshold
                    |
       untouched test + bootstrap gate
                    |
       sealed challenge if gate passes
                    |
       registry + approval + deployment
```

Flink computes deterministic stateful features; it does not train a model or
contain model weights. Go validates the feature contract, creates the tensor,
runs the promoted model, calibrates probabilities, and applies the decision
policy. Snowflake stores immutable history, labels, experiments, predictions,
feedback, and audit records.

See also:

- [How the Flink feature pipeline works](flink-realtime-feature-pipeline.md)
- [Exact real-time model input contract](realtime-model-input-contract.md)
- [Data generation and storage plan](data-generation-and-pipeline-plan.md)

## 3. Data products and prediction labels

### 3.1 Public inference data

Public input includes hand actions, outcomes, point-in-time user context,
prior-only user statistics, and prior-only pair statistics. It must never
contain private truth such as:

- `target`;
- `is_collusive`;
- collusion group or scenario identity;
- a future analyst decision; or
- an outcome not yet known at scoring time.

The online Flink and Go contracts reject forbidden label fields.

### 3.2 Restricted label data

Labels are a separate restricted data product. Each label requires:

- the labeled pair or entity;
- the source and confidence of the label;
- `label_available_at`;
- reviewer or synthetic-generator lineage; and
- an explicit state: confirmed positive, confirmed negative, or unresolved.

An example is eligible for training only after `label_available_at`. An
unresolved analyst review must not silently become a negative label.

### 3.3 One hand is one grouping boundary

All 15 rows from one hand must stay in the same split or cross-validation fold.
Splitting pair rows randomly would allow 14 nearly identical views of a hand in
training and the final view in validation, producing an unrealistically high
score.

## 4. Why there are four splits

```text
train ----------> fit preprocessing and model parameters
validation -----> choose checkpoint, calibration, and threshold
test -----------> one public comparison after all choices are frozen
challenge ------> sealed end-to-end evaluation after public gates pass
```

| Split | Full-v2 hands | Pair rows | Positives | Permitted use |
|---|---:|---:|---:|---|
| Train | 20,000 | 300,000 | 352 | Fit preprocessing and models |
| Validation | 5,000 | 75,000 | 74 | Early stopping, Platt calibration, alert threshold |
| Test | 5,000 | 75,000 | 75 | One registered public comparison |
| Challenge | 5,000 | 75,000 | 106 | Sealed replay and final evaluation |

The challenge is not a second validation set. If people repeatedly inspect it
and tune against it, it stops being a challenge and a new sealed population is
required.

### 4.1 Cold-start benchmark

Train, validation, test, and challenge use disjoint player populations. This
answers:

> Can the model score users it has never seen before?

Raw player IDs must not be features. The current production champion is
registered on this benchmark.

### 4.2 Temporal benchmark

The same user population is divided chronologically:

```text
oldest 70% -> train
next   15% -> validation
latest 15% -> test
```

This answers:

> Can the model predict future behavior for previously observed users?

All features for an example must use only data strictly earlier than the hand.

### 4.3 New-relationship benchmark

Users may have appeared during training, but protected positive pair identities
in validation and test are absent from training. This answers:

> Can the model identify a newly harmful relationship instead of memorizing a
> known pair?

### 4.4 Why all three matter

A model can perform well on known users yet fail for new users. It can also
memorize suspicious pairs without learning transferable coordination patterns.
A company-wide model should therefore report cold-start, temporal, and
new-relationship results separately rather than hiding them in one average.

## 5. Leakage prevention checklist

Before accepting an experiment, verify all of the following:

- Split assignment occurs before entity and event generation.
- Every hand belongs to one split and one fold.
- Cold-start user populations are disjoint.
- Protected new-relationship pairs do not cross the required boundary.
- Challenge labels are not copied to DGX or loaded by public trainers.
- Imputation values, scales, vocabularies, feature selection, and sampling
  policies are fitted on train only.
- Validation alone chooses early stopping, calibration, and thresholds.
- Test is not used for iterative tuning.
- Context joins use `effective_at <= played_at`.
- Rolling user, pair, sequence, and graph features contain only prior events.
- Equal-timestamp hands are snapshotted before any of them update history.
- Graph neighborhoods have no future edge and no label-derived edge.
- Raw identifiers are lineage fields, not learned lookup shortcuts.
- Analyst decisions become labels only after their availability timestamp.
- Every artifact records dataset hash, feature version, configuration, seed,
  dependencies, predictions, and source run ID.

Automated leakage checks are release gates, not optional diagnostic tests.

## 6. Metrics: why accuracy is misleading

Only 75 of 75,000 pair rows in the cold-start test are positive. A classifier
that predicts “negative” for every row has:

```text
accuracy = 74,925 / 75,000 = 99.9%
recall   = 0 / 75          = 0%
```

It detects no collusion but appears highly accurate. Accuracy is therefore not
a primary metric for this project.

### 6.1 Primary metrics

| Metric | Meaning in this project |
|---|---|
| PR-AUC / average precision | Ranking quality for the rare positive class across thresholds |
| Precision | Of pairs alerted, how many are truly positive? |
| Recall | Of positive pairs, how many are found? |
| Recall at alert budget | How many positives analysts find when review capacity is fixed? |
| Precision at top K | Expected usefulness of the highest-ranked 100 or 1,000 cases |
| False positives per 1,000 hands | Direct operational review cost |
| F1 | One threshold-specific balance of precision and recall |
| Brier score | Squared error of predicted probabilities; useful for calibration |
| Calibration curve | Whether a predicted probability corresponds to observed frequency |
| Paired hand bootstrap interval | Uncertainty in a candidate's improvement over the champion |

ROC-AUC remains useful as a secondary ranking metric, but the very large number
of negatives can make it look excellent while alert precision remains poor.

### 6.2 The current CatBoost confusion matrix

At the frozen validation-selected threshold `0.984192` on the public test:

|  | Predicted alert | Predicted no alert |
|---|---:|---:|
| Actually positive | 35 | 40 |
| Actually negative | 56 | 74,869 |

This produces:

- precision `35 / 91 = 38.46%`;
- recall `35 / 75 = 46.67%`;
- F1 `42.17%`;
- 11.2 false positives per 1,000 hands; and
- pair-row accuracy `99.872%`, lower than the useless all-negative accuracy.

At a wider 2% ranking budget, it recovers 70.67% of positives, but precision at
that budget is only 3.53%. The operating threshold must therefore be selected
from analyst capacity and the relative cost of missed collusion versus false
review, not from accuracy.

### 6.3 Required reporting slices

Every registered candidate should report metrics for:

- cold-start, temporal, and new-relationship benchmarks;
- stake and game type;
- account age and skill segment;
- context present versus missing/late/corrected;
- region and acquisition channel where policy permits;
- new versus established pair histories;
- label source and label confidence; and
- a fixed operational alert budget.

Small segments must include counts and uncertainty; a high percentage based on
two positive examples is not evidence of stability.

## 7. Is the CatBoost data and model stable?

“Stable” has four different meanings.

### 7.1 Reproducible data: yes

**Implemented.** Dataset manifests record seeds, split policies, row counts,
source hashes, feature definition, and every artifact SHA-256. Rebuilding with
the same inputs is checked for deterministic event and label hashes.

### 7.2 Reproducible scoring: yes

**Implemented.** The active registry entry pins:

- model run `pair_7a1c58c1046b`;
- dataset `context-full-v2`;
- feature definition `pair-features-v1`;
- the 58-column preprocessing contract;
- calibration and decision-policy JSON;
- CatBoost and ONNX artifacts; and
- an artifact-manifest hash.

CatBoost and ONNX probabilities differed by at most approximately `6.4e-8` in
the artifact test. Go also verifies hashes and contract identity before it
becomes ready.

### 7.3 Statistically stable quality: promising, not proven

Test PR-AUC `0.362918` and private challenge PR-AUC `0.374749` are close. That
is encouraging. However, validation PR-AUC was `0.198198`, and the three public
holdouts contain only 74, 75, and 106 positive rows. The measured quality can
still vary substantially with population and generator settings.

**Next:** add a standard stability report containing:

- 1,000-sample paired hand-bootstrap confidence intervals for champion metrics;
- at least five training seeds for candidate robustness;
- generator-seed and scenario-family holdouts;
- confidence intervals for segment metrics; and
- sensitivity to class weight, history length, and alert budget.

Retraining need not be bit-identical across every CPU/GPU implementation. The
frozen artifact and its measured predictions are the deployment authority.

### 7.4 Production stability: not known yet

The current benchmark is synthetic. It proves pipeline mechanics and controlled
experimentation, not production fraud-detection accuracy. The current drift
report is `warning`: model-score PSI is healthy, while one account-age feature
has PSI `0.163`, above the `0.10` warning threshold and below the `0.25` critical
threshold.

Production claims require a shadow period with real poker-server events,
delayed investigator labels, tenant-specific segment reports, and monitored
label drift.

## 8. CatBoost: current champion and default model

### Why it fits the current input

The v1 input is medium-sized tabular data containing nonlinear ratios,
interactions, missing values, context crosses, and rolling aggregates. CatBoost
handles this type of input efficiently and is fast enough to score all 15 pairs
in one hand on CPU.

### Training contract

**Implemented:**

1. Fit numeric fill values and categorical vocabularies on train.
2. Train a class-weighted CatBoost model with a fixed configuration and seed.
3. Use validation for early stopping.
4. Fit Platt calibration on validation predictions.
5. Select a validation decision threshold under the alert budget.
6. Freeze preprocessing, model, calibration, policy, predictions, metrics,
   feature importance, SHAP summary, and hashes.
7. Evaluate test once.
8. Open the private challenge only after public promotion gates pass.
9. Export ONNX and verify parity with the native model.

### How to use it online

CatBoost remains the sole numerical champion. Rules provide evidence and policy
overrides, but they do not modify the probability unless a future leakage-safe
ensemble proves a stable improvement. Store raw probability, calibrated
probability, threshold, model version, feature version, rule evidence, and
lineage for every score.

## 9. Rule engines: evidence, policy, and model features

Rules and ML solve different problems:

- A model ranks statistical risk learned from examples.
- A rule represents explicit domain or operational knowledge.
- A decision policy decides what action follows from model and rule evidence.

### 9.1 What is implemented now

The legacy per-player rule engine contains:

| Rule | Meaning |
|---|---|
| `PRE_MW` | Multiple preflop raises in a multiway pot |
| `PRE_OFOLD_COMMIT` | Large preflop stack commitment followed by a preflop fold |
| `POST_OFOLD_COMMIT` | Large total commitment followed by a postflop fold |
| `HIGH_INVEST_FOLD` | At least 30% stack investment followed by a fold |
| `POSITION_ANOMALY` | Passive preflop and aggressive postflop behavior from late position |

The pair-model benchmark also has a deterministic score using fold/win
interaction, shared device/network, outcome asymmetry, and pair fold-benefit
rates. On the frozen test, rules-only PR-AUC is `0.039510`, so rules are not a
replacement for CatBoost.

### 9.2 Recommended rule categories

#### Hard policy rules

Use only for evidence that requires action independent of statistical risk—for
example a confirmed account-control link or an impossible security event.
These rules route to review; they should not rewrite the model probability.

#### Soft behavioral rules

Use for suspicious but non-conclusive behavior such as repeated fold-to-partner
wins, unusual chip-flow asymmetry, coordinated timing, synchronized table
movement, or persistent shared infrastructure. Soft rules appear as evidence
and may later become model features.

#### Data-quality rules

Missing fields, invalid state transitions, conflicting context versions, and
impossible values belong in the DLQ or data-quality monitoring. They are not
fraud evidence.

### 9.3 Where each rule should run

| Rule type | Runtime | Reason |
|---|---|---|
| Current-hand, stateless | Go scorer | Lowest latency and same hand context as the decision |
| Rolling/windowed pattern | Flink keyed state | Needs ordered prior events, timers, or pair memory |
| Long retrospective investigation | Snowflake task/query | Complex historical analysis outside the hot path |
| Hard security/account policy | Dedicated policy service or Go policy layer | Clear ownership, audit, and controlled overrides |

### 9.4 Rule output contract — next

Publish structured evidence rather than one unexplained sum:

```json
{
  "rule_id": "PAIR_FOLD_BENEFIT",
  "rule_version": 2,
  "entity_type": "player_pair",
  "entity_key": "player-a:player-b",
  "hand_id": "...",
  "severity": "medium",
  "raw_score": 0.81,
  "evidence": {
    "hands_together": 42,
    "fold_benefit_rate": 0.73
  },
  "effective_at": "...",
  "trace_id": "..."
}
```

Every rule needs an owner, version, effective date, description, unit tests,
separate precision/recall report, firing-rate monitor, and rollback path.

### 9.5 Combining rules and ML

Use this initial policy:

```text
if hard_policy_rule:
    create mandatory review with explicit reason
else:
    use calibrated CatBoost probability for ranking
    attach soft rules as evidence and analyst filters
```

Do not hand-tune a weighted average of rules and CatBoost. If rule outputs are
to influence probability, generate out-of-fold rule/model predictions, train a
small calibrated stacker, and require it to pass the normal promotion gate.
The implemented OOF stack scored `0.214408` PR-AUC and was rejected, proving
that adding components does not automatically improve a model.

Avoid circular evaluation: if a historical label was created solely because a
rule fired, that label cannot fairly prove that the same rule is accurate
without independent review.

## 10. Deep learning methods

Deep learning is useful when it receives information or structure that the
tabular CatBoost snapshot does not already summarize. Replacing CatBoost with a
larger network on the same inputs is not, by itself, a benefit.

### 10.1 Tabular neural networks

**Implemented and evaluated:**

| Model | Test PR-AUC | Decision |
|---|---:|---|
| Residual MLP | `0.186673` | Rejected |
| FT-Transformer | `0.182130` | Rejected |
| DCN-V2 | `0.142649` | Rejected |
| CatBoost comparison | `0.362918` | Champion |

These networks were fast enough, but all paired bootstrap intervals were
strictly negative relative to CatBoost. Do not deploy them or repeatedly tune
against the same public test.

Retry tabular deep learning only after one of these changes:

- substantially more independent real labels;
- richer high-cardinality context with a legitimate embedding benefit;
- a new dataset population; or
- a new architecture hypothesis registered before opening a fresh test.

### 10.2 Multi-hand sequence models

A sequence model should consume ordered information that aggregates cannot
fully represent:

- the last N action sequences;
- behavior before and after session/table changes;
- stake movement;
- timing gaps and action latency;
- device/network transitions;
- repeated interactions with the same players; and
- changes in aggression or chip-flow direction.

**Implemented:** a 16-hand prior-only history Transformer with train-only
self-supervised pretraining and pair-risk fine-tuning. It reached test PR-AUC
`0.181929` versus CatBoost `0.362918` and was rejected.

This result says the current synthetic sequence does not add sufficient signal.
It does not prove sequence models are useless. Real poker-server data may
contain richer temporal changes that are absent from the generator.

**Next online integration if a future sequence model passes:**

1. Define a versioned token schema.
2. Build the exact prior-only sequence in a stateful Flink operator.
3. Publish `poker.pair-sequence-features.v1` keyed by tenant and hand.
4. Make Go assemble the tabular and sequence inputs without database reads.
5. Export the network to ONNX or serve one batch per hand through Triton.
6. Add online/offline token-parity and replay tests before shadow deployment.

Do not deploy a model whose online sequence cannot exactly match its training
sequence.

## 11. Graph ML methods

Collusion is naturally relational. Relevant nodes can include users, devices,
networks, sessions, tables, accounts, payment instruments, and organizations.
Edges can represent playing together, shared infrastructure, account links,
transfers, sessions, and rule evidence.

### 11.1 Requirements for a safe graph

- Every edge timestamp must be strictly earlier than the scored hand.
- Equal-time events must not see one another as history.
- Challenge and future edges must remain unavailable.
- Raw user/device IDs must not be the only embeddings.
- The model must handle a completely unseen user inductively.
- Each graph result must be evaluated on both cold-start and new-relationship
  benchmarks.

### 11.2 What is implemented

The relation-aware temporal GraphSAGE model uses feature-derived node
initialization and zero raw-ID embeddings.

| Benchmark | GraphSAGE PR-AUC | Matching CatBoost | Decision |
|---|---:|---:|---|
| Cold start | `0.247934` | `0.362918` | Rejected |
| New relationship | `0.508470` | `0.615757` | Rejected |

The graph model beat the earlier neural models, showing useful relational
signal, but did not add stable lift over the corresponding CatBoost baseline.

### 11.3 Recommended graph progression — next

Use increasing complexity only when the previous step demonstrates lift:

1. **Graph-derived scalar features:** common neighbors, shared-resource counts,
   resource degree, relationship recency, two-hop suspicious-neighbor counts,
   component size, and edge-type diversity. Add them to CatBoost first.
2. **Graph score as a feature:** generate strictly out-of-fold GraphSAGE scores
   for training and frozen validation/test scores, then let a small model test
   whether the graph adds incremental information.
3. **Embedding fusion:** concatenate a frozen inductive graph embedding with the
   tabular branch only after step 2 succeeds.
4. **Temporal Graph Network:** consider event-memory architectures when real
   device, session, transfer, and network-churn events exist at sufficient
   scale.

For online graph inference, prefer an asynchronously updated graph-feature
topic or dedicated graph service. Do not add a synchronous Snowflake query to
the scoring path. Version graph snapshots and require point-in-time parity with
offline training.

## 12. Anomaly detection and weak supervision

### 12.1 Anomaly detection

Isolation Forest, autoencoders, or density models can surface behavior outside
known training scenarios. They are useful for candidate discovery and analyst
queues, especially before many confirmed labels exist.

They should not automatically block users because “unusual” is not equivalent
to “collusive.” Publish an anomaly score with its feature version, reference
window, and reason codes. Evaluate its contribution through analyst yield and
incremental recall, not by assuming unlabeled events are negative.

### 12.2 Weak labels

Rules may produce weak labels for research, but weak labels must remain marked
with their source and confidence. Train/evaluate against independently reviewed
labels whenever possible. Do not use rule-generated labels to claim that the
same rules or a model trained on them have independent accuracy.

## 13. Ensembles and model fusion

An ensemble is justified only when components make useful, different errors.

### Safe procedure

1. Group cross-validation folds by `hand_id`.
2. Fit preprocessing and base learners separately inside every fold.
3. Create one out-of-fold prediction per training row.
4. Train a simple logistic stacker on out-of-fold predictions only.
5. Fit calibration and threshold on validation.
6. Compare against the champion on untouched test with a paired hand bootstrap.
7. Open challenge only if the public lower confidence bound and operational
   metrics pass.

**Implemented:** the five-fold OOF CatBoost + rules + player-logistic stack
followed this procedure. Test PR-AUC was `0.214408`, so it was rejected and the
challenge stayed sealed.

Do not add a complex meta-learner until a simple stacker demonstrates stable
incremental lift.

## 14. Analyst AI and LLMs

An LLM may help investigators by converting structured evidence into a concise
case summary:

- which pair and hands caused the alert;
- important SHAP features;
- triggered rules and their evidence;
- historical relationship and context changes; and
- links to supporting records.

The LLM must not be the primary numerical risk scorer, invent evidence, change
the frozen threshold, or receive unrestricted raw PII. Its output should cite
structured evidence IDs and remain a review aid. Human outcomes return as
delayed, versioned feedback—not as immediate training truth.

## 15. Champion–challenger experiment procedure

Every new candidate should follow the same sequence.

### Step 1: register the hypothesis

Record what new information or inductive bias should improve the champion. “A
larger model may be better” is not a sufficient hypothesis.

### Step 2: freeze the data contract

Pin dataset ID and hash, prediction unit, feature version, label cutoff, split
policy, forbidden fields, evaluation benchmarks, and expected row counts.

### Step 3: build train-only preprocessing

Fit every scale, vocabulary, sampler, graph constructor, embedding pretrainer,
and feature selector without validation/test/challenge information.

### Step 4: tune on train and validation

Use grouped folds within train when needed. Validation selects the final
checkpoint, calibration, and decision policy. Save all tried configurations to
make repeated search visible.

### Step 5: freeze and evaluate test once

Write predictions for every row with `event_id` and `hand_id`. Report required
metrics, segments, latency, and a paired hand-bootstrap difference from the
champion.

### Step 6: enforce public promotion gates

A candidate must:

- improve test PR-AUC by at least the registered margin, currently 2%;
- have a positive lower bound for the paired improvement interval;
- match or improve recall at the fixed alert budget;
- match or improve validation-threshold test F1;
- pass leakage, artifact, contract, and latency checks; and
- avoid material degradation in required population segments.

### Step 7: sealed challenge

Only a public-gate candidate may be evaluated on challenge. A challenge failure
rejects the candidate. Do not return to tuning on that challenge.

### Step 8: shadow and approve

For real data, run the candidate without changing production decisions. Compare
analyst yield, latency, drift, stability, and segment behavior. Promotion
requires artifact integrity, public quality, private challenge, manual approval,
and operational verification. Automatic production promotion is disabled.

## 16. Monitoring and retraining

### Without labels, monitor daily or per fixed window

- feature PSI and missingness;
- categorical total-variation distance and unknown-category rate;
- calibrated-score distribution;
- alert and rule firing rates;
- context join status and corrections;
- model errors, latency, Kafka lag, and incomplete hands; and
- input/model/feature/policy version mismatches.

### When delayed labels arrive, monitor

- PR-AUC, precision, recall, and Brier score;
- analyst-confirmed precision at the active budget;
- false positives per 1,000 hands;
- label delay and unresolved-review rate;
- segment performance; and
- disagreement between rules, champion, and challengers.

### Retrain when evidence supports it

Retrain for a materially larger labeled window, feature/schema change, verified
concept drift, or a registered new model hypothesis. Do not retrain merely on a
calendar and automatically deploy the result. Every retrain creates a new run
and must pass the same gates.

## 17. Transition from synthetic to production data

The synthetic benchmark remains valuable for determinism, leakage tests,
replay, rare scenario coverage, and regression testing. It cannot estimate real
prevalence or production investigator yield.

Recommended production transition:

1. Keep PokerKit datasets as permanent engineering tests.
2. Add poker-server hand history through PostgreSQL and Debezium CDC.
3. Run the production feature pipeline in shadow mode.
4. Verify online/offline feature parity and event-time correctness.
5. Collect delayed investigator labels with clear unresolved states.
6. Create tenant/time-based real-data train, validation, and forward test
   windows.
7. Train a new candidate; do not call the synthetic model production-accurate.
8. Compare synthetic and real segment behavior.
9. Shadow the candidate and require the full registry gates before actioning
   scores.

Synthetic and real data should be reported separately. Synthetic examples may
augment rare scenarios, but final production quality must be measured on real,
time-forward labels.

## 18. Implementation roadmap from here

Most model families in the original roadmap are already implemented. The next
work should improve evidence and integration rather than add another model name.

### Milestone A: CatBoost stability report — next

- Add paired bootstrap intervals for the champion's absolute metrics.
- Run multi-seed candidate sensitivity without reopening challenge.
- Produce standard segment reports with counts and intervals.
- Add generator-scenario holdouts.
- Write a machine-readable model card tied to the registry entry.

Acceptance: one command produces a hash-tracked stability report and fails on
split leakage, inadequate counts, or an unapproved challenge read.

### Milestone B: versioned rules v2 — next

- Define the structured rule-evidence event.
- Port current-hand pair rules to Go.
- Add rolling pair-pattern rules to Flink where state is required.
- Store rule version/evidence with every score and alert.
- Add separate rule precision, firing-rate, and drift dashboards.
- Keep hard policy overrides separate from soft model evidence.

Acceptance: deterministic replay produces identical rule evidence; every rule
has tests, versioning, ownership, metrics, and rollback.

### Milestone C: real-data shadow evaluation — after CDC integration

- Verify source and feature parity.
- Run CatBoost and rules without enforcement.
- Measure label availability and review capacity.
- Establish real temporal splits and a sealed forward test.
- Recalibrate probability and threshold using real validation only.

Acceptance: no production accuracy claim or enforcement before sufficient
time-forward labels and manual approval.

### Milestone D: graph-derived CatBoost augmentation — after real graph events

- Add prior-only scalar graph features first.
- Evaluate them through the normal CatBoost pipeline.
- Generate OOF GraphSAGE scores only if scalar features add value.
- Test both cold-start and new-relationship lift.

Acceptance: positive paired lower bound and no operational/segment regression.

### Milestone E: richer sequence retry — after richer event collection

- Add action timing, session changes, stake changes, device/network changes,
  and longer histories.
- Register the hypothesis and fresh evaluation boundary before training.
- Implement online token parity before any shadow serving.

Acceptance: beat CatBoost and the graph-augmented champion under the same gates.

### Milestone F: anomaly discovery and analyst AI — optional

- Use anomaly scores only for candidate discovery.
- Measure independent analyst yield.
- Add evidence-grounded LLM summaries after access and PII controls exist.

Acceptance: no autonomous enforcement and complete evidence lineage.

## 19. Code and artifact map

| Purpose | Location |
|---|---|
| Pair dataset construction | `pipeline/ml/pair_dataset.py` |
| CatBoost training | `pipeline/ml/pair_train.py`, `pipeline/ml/pair_model.py` |
| Evaluation and thresholds | `pipeline/ml/evaluation.py`, `pipeline/ml/pair_model.py` |
| OOF ensemble | `pipeline/ml/ensemble.py` |
| Legacy rules | `pipeline/rules/` |
| Tabular DL | `pipeline/dl/pair_challengers.py`, `pipeline/dl/tabular_models.py` |
| Multi-hand model | `pipeline/dl/history_dataset.py`, `history_models.py`, `history_train.py` |
| Graph model | `pipeline/dl/graph_dataset.py`, `graph_models.py`, `graph_train.py` |
| Champion artifacts | [`models/pair-catboost-full-v2`](../models/pair-catboost-full-v2) |
| Model registry and drift | [`models/registry`](../models/registry) |
| Production hardening | [Phase 12 runbook](phase12-production-hardening-runbook.md) |
| Tabular DL evidence | [DGX challenger runbook](dgx-pair-challengers-runbook.md) |
| Sequence evidence | [DGX history runbook](dgx-pair-history-runbook.md) |
| Graph evidence | [DGX graph runbook](dgx-pair-graph-runbook.md) |

## 20. Further reading

Recommended reading order:

1. [Scikit-learn model evaluation](https://scikit-learn.org/stable/modules/model_evaluation.html) — precision, recall, PR curves, calibration, and classification metrics.
2. [CatBoost training parameters](https://catboost.ai/docs/en/references/training-parameters/) — validation, seeds, class weights, and model controls.
3. [Why tree-based models still outperform deep learning on typical tabular data](https://arxiv.org/abs/2207.08815) — evidence for retaining strong tree baselines.
4. [Revisiting Deep Learning Models for Tabular Data](https://arxiv.org/abs/2106.11959) — the FT-Transformer benchmark and architecture.
5. [GraphSAGE: Inductive Representation Learning on Large Graphs](https://arxiv.org/abs/1706.02216) — feature-based embeddings for unseen nodes.
6. [Temporal Graph Networks](https://arxiv.org/abs/2006.10637) — learning over evolving, timestamped graph events.
7. [SHAP: A Unified Approach to Interpreting Model Predictions](https://arxiv.org/abs/1705.07874) — feature-attribution foundations.
8. [Snorkel: Rapid Training Data Creation with Weak Supervision](https://arxiv.org/abs/1711.10160) — principles and risks of programmatic weak labels.

The repository's measured artifacts remain the authority for this project.
External papers provide methods and context; they do not override our frozen
evaluation and promotion gates.
