# 100-table test data, alert, and dataset plan

Status: D1–D5 implemented; D6 alert-acceptance pack next

Last reviewed: 2026-07-23

Implementation evidence:

- The versioned smoke profile is
  [`config/generator/multitable-smoke-v1.json`](../config/generator/multitable-smoke-v1.json).
- The versioned scenario plan is
  [`config/generator/multitable-scenarios-v1.json`](../config/generator/multitable-scenarios-v1.json).
- The first full smoke generated 6,000 hands across 100 tables and four
  population-disjoint splits.
- Every active roster contained exactly 20 four-player, 30 five-player, and 50
  six-player tables: 530 concurrent seats.
- The D4 smoke completed all 56 planned cases: 22 positive-pair cases, four
  three-account rings, and 30 difficult-negative cases.
- It produced 336 scenario hands, 336 positive player labels, 204 positive pair
  labels, and 40,000 inference-safe context rows in approximately 88 MB.
- Household negatives share a device and network. Shared-network negatives
  retain different devices. Their case identity remains private.
- D5 produced four independently manifested, label-free assignment products:
  cold start, chronological temporal, protected new relationship, and sealed
  challenge.
- The full smoke assigned 6,000 cold-start hands, 3,000 temporal hands at
  2,100/450/450, 3,000 new-relationship hands at 2,100/450/450, and 1,000
  isolated challenge hands.
- The leakage audit reported zero cross-split cold-start players, pairs, and
  public groups; zero protected-pair or case crossings in the
  new-relationship product; strict temporal ordering; complete hand coverage;
  and no challenge-label read or copy.
- All 34 focused generator, contract, frozen-world, pair-dataset, and D5
  benchmark tests passed after the refactor.
- The complete 267-test Python suite also passed; its only output was existing
  third-party SciPy deprecation warnings.

## 1. Outcome

Build a deterministic PokerKit world that behaves like a busy poker system:

- 100 concurrently active tables;
- 4, 5, or 6 seated players per table;
- players may play at several tables at the same logical time;
- normal, difficult-negative, and collusive behavior appears in controlled
  proportions;
- public events remain free of labels and synthetic scenario shortcuts;
- train, validation, test, and sealed challenge datasets are reproducible;
- a separate alert-acceptance replay reliably produces visible rule evidence
  and model-driven review alerts; and
- only hand events enter Kafka while `POKER_FLINK` lazily resolves active-player
  context from Snowflake.

The frozen benchmark measures ML quality. The alert-acceptance dataset proves
that the deployed Kafka, Flink, Go scoring, review-policy, sink, and admin path
work end to end. They must remain separate because a case selected specifically
to cross the current model threshold is not an unbiased test example.

This plan extends the
[data generation and pipeline plan](data-generation-and-pipeline-plan.md) and
uses the split and evaluation rules in the
[data science guide](data-science-model-development-guide.md).

## 2. Current implementation and required change

The repository already provides:

- deterministic legal six-player NLHE hands through PokerKit;
- four frozen splits with hash manifests;
- separate public events and delayed private player/pair labels;
- correlated user context, sessions, devices, networks, and account links;
- four collusion behaviors: soft play, chip dumping, squeeze collusion, and
  fold-for-partner benefit;
- one hand expanded into all unordered pair examples;
- CatBoost, Flink stateful features, six Go rules, one Flink stateful rule, and
  the review-policy path; and
- local, Confluent, Snowflake, and SPCS replay paths.

The current hand generator is not a multi-table world scheduler:

- it independently samples exactly six players for every hand;
- it chooses a table only after generating the hand;
- it advances one global clock by exactly 30 seconds per hand;
- it has no persistent seats, table sessions, joins, leaves, or concurrent
  player sessions; and
- every split assumes 15 pair rows per hand.

The next generator version must schedule players, tables, and event time before
asking PokerKit to execute each hand.

## 3. Target simulation profile

### 3.1 Table and seat distribution

Use this first production-shaped profile:

| Setting | Default |
|---|---:|
| Active tables | 100 |
| Four-player tables | 20 |
| Five-player tables | 30 |
| Six-player tables | 50 |
| Concurrent seat assignments | 530 |
| Mean players per hand | 5.3 |
| Mean unordered pair rows per hand | 11.7 |
| Mean completed hands per table-hour | 60 |
| Registered synthetic users per population | 10,000 |
| Daily active users | 2,000 |
| Target peak concurrent unique players | approximately 400 |
| Maximum simultaneous tables per player | 5 |

The table-size mix produces:

```text
20 * 4 + 30 * 5 + 50 * 6 = 530 concurrent seats

0.20 * C(4,2) + 0.30 * C(5,2) + 0.50 * C(6,2)
= 0.20 * 6 + 0.30 * 10 + 0.50 * 15
= 11.7 pair rows per hand
```

The exact table-size counts should be maintained at each scheduler rebalance,
but individual tables may change between 4, 5, and 6 players over time. A
player cannot occupy two seats at the same table.

### 3.2 Multi-table distribution

At a busy snapshot, target this distribution among concurrently active users:

| Simultaneous tables | Share of active users |
|---:|---:|
| 1 | 75.0% |
| 2 | 18.0% |
| 3 | 5.0% |
| 4 | 1.5% |
| 5 | 0.5% |

This averages approximately 1.345 table seats per active player. About 400
active users therefore occupy approximately 538 seats, giving the scheduler
enough candidates to fill 530 seats without inventing users or exceeding the
configured maximum.

This distribution is a configurable target, not a hard-coded label. The
manifest must record requested and observed concurrency histograms.

### 3.3 Time and session behavior

Use one logical UTC simulation clock and one independent next-hand clock per
table.

- Table hand intervals vary around the configured 60 hands/hour instead of
  advancing every table in lockstep.
- A table session normally lasts 30–120 minutes before a seat rebalance.
- A user session normally lasts 20–240 minutes.
- A multi-table user has one user session with several overlapping table-seat
  intervals.
- New users join and existing users leave throughout the simulated day.
- Table hand order is strictly increasing.
- Hands from different tables may overlap in event time.
- Hand delivery may be delayed or reordered only by the replay layer; the
  canonical business schedule does not change.

PokerKit still owns legal betting, cards, pot settlement, and winners. The
world scheduler owns table size, seating, event time, user concurrency, and
scenario assignment.

## 4. Dataset products

Do not use one dataset for every purpose. Generate four independently
manifested products.

### 4.1 Cold-start benchmark

Train, validation, test, and challenge have disjoint players and collusion
groups. This remains the primary model-promotion benchmark and answers whether
the model generalizes to unseen users.

Suggested dataset ID:

```text
multitable-cold-v1
```

### 4.2 Temporal benchmark

One stable population is split by time. Rolling state flows forward, but every
feature remains prior-only at the hand time.

```text
days 01-14 -> train
days 15-17 -> validation
days 18-20 -> test
days 21-23 -> challenge
```

Suggested dataset ID:

```text
multitable-temporal-v1
```

This benchmark answers whether the model predicts future behavior for known
users. The start of validation, test, and challenge may consume approved
prior-history warm-up rows, but warm-up rows cannot be scored as examples in
the later split. A real multi-hand investigation case may continue across a
time boundary; that is expected temporal behavior, not split leakage. Its
features at each scored hand may use only strictly earlier events. Forcing
complete cases into one temporal split would move time boundaries and distort
the intended 70/15/15 evaluation.

### 4.3 New-relationship benchmark

Users may exist in train, but protected validation/test colluding pairs must
not appear together in training hands. This detects pair-identity
memorization.

Suggested dataset ID:

```text
multitable-new-relationship-v1
```

### 4.4 Alert-acceptance and demo replay

Generate a small high-intensity scenario pack solely for end-to-end
verification.

Suggested dataset ID:

```text
multitable-alert-acceptance-v1
```

It contains:

- deterministic expected rule-evidence cases;
- explicit must-not-fire controls where a rule has a precise negative
  precondition;
- a recorded expected-evidence oracle;
- examples scored by the frozen champion;
- at least ten examples verified to exceed that champion's registered
  threshold; and
- the expected score, decision, alert, and admin row identities for the exact
  model and policy versions.

Threshold-positive examples may be selected only inside this acceptance
dataset. It is forbidden as model training, validation, test, calibration, or
promotion data.

## 5. Generation sizes

Implement the profiles in order. Do not begin with the largest benchmark.

| Profile | Train hands | Validation | Test | Challenge | Total hands | Approx. pair rows |
|---|---:|---:|---:|---:|---:|---:|
| Smoke | 3,000 | 1,000 | 1,000 | 1,000 | 6,000 | 70,200 |
| Development | 48,000 | 12,000 | 12,000 | 12,000 | 84,000 | 982,800 |
| Benchmark | 672,000 | 144,000 | 144,000 | 144,000 | 1,104,000 | 12,916,800 |

The benchmark profile represents:

```text
100 tables * 60 hands/hour * 8 simulated hours/day = 48,000 hands/day
```

It uses 14 train days and 3 days for each later split. At a mean of 5.3
players, the full benchmark also produces approximately 5,851,200 player-hand
rows.

Actual counts, table hours, player rows, pair rows, and distributions belong in
the manifest. The calculations above are capacity targets, not substitutes for
measured counts.

## 6. Realistic scenario catalogue

The generator should schedule multi-hand cases, not independently flip a
collusion flag on each hand.

### 6.1 Background and difficult negatives

These cases are labeled non-collusive and are essential to prevent synthetic
shortcuts:

| Case | Purpose |
|---|---|
| Normal one-table player | Ordinary negative population |
| Legitimate high-volume multi-tabler | Multi-table play alone must not imply risk |
| Household/shared device | Same-device evidence can be innocent |
| Shared office, carrier NAT, or public network | Same-network evidence can be innocent |
| Strong professional player | High winnings and outcome asymmetry can be legitimate |
| Friends who often choose the same tables | Co-occurrence alone is not collusion |
| New account moving to higher stakes | Context anomaly without coordinated play |
| Short sessions and table hopping | Avoid making table churn a direct label |

Shared device and network cases should include both positives and negatives.
Multi-table counts, table IDs, device IDs, network IDs, and session IDs must
never become raw identity memorization features.

### 6.2 Positive behavior cases

| Scenario | Generated behavior | Current observable path |
|---|---|---|
| Soft play | Pair avoids aggression against each other and checks/calls down | CatBoost and pair-history features |
| Chip dumping | One member calls or loses disproportionately to the partner | CatBoost and outcome-asymmetry evidence |
| Squeeze collusion | Partner re-raises to isolate another player | CatBoost/current-hand behavior |
| Fold benefit | One member folds in situations benefiting the other | Current-hand and directional-fold features |
| Repeated fold to partner wins | At least 3 directional cases among at least 5 shared hands in 24 hours, rate at least 0.60 | Flink `pair.repeated-fold-to-partner-wins` |
| Coordinated rendezvous | Pair repeatedly meets across changing tables | Future temporal/graph feature candidate |
| Multi-account ring | Three or more accounts rotate benefits within a group | Future graph/GNN candidate |
| Cross-table coordination | A group overlaps on several tables in the same session | Future temporal/graph feature candidate |

The first implementation should retain the four existing PokerKit strategies
and add a scenario planner that controls co-seating, activation windows,
direction, intensity, and minimum supporting hands. Ring and graph scenarios
are generated and labeled for later models even if the current CatBoost does
not detect them well.

### 6.3 Scenario prevalence

Start development data near:

- 95% ordinary background hands;
- 3% difficult-negative hands; and
- 2% positive scenario-affected hands.

Prevalence must be configurable by scenario and measured after generation.
Validation, test, and challenge should have realistic intensities and overlaps.
The alert-acceptance pack deliberately uses stronger cases.

Do not rebalance the public benchmark to 50/50. Training may apply
train-only row weighting or negative sampling, but evaluation must retain the
generated prevalence.

## 7. Labels and alert oracles

### 7.1 Label levels

Maintain separate restricted sidecars:

| Sidecar | Grain | Important fields |
|---|---|---|
| Player labels | hand/player | target, group, availability time, provenance |
| Pair labels | hand/unordered pair | target, group, scenario family, availability time |
| Case labels | scenario episode | case start/end, members, intensity, expected affected hands |
| Group labels | collusion ring | group members and active intervals |
| Alert oracle | expected evidence/decision | versioned rule/model/policy expectations |

Public Kafka hand events, context tables used by online inference, and pair
feature events must not contain case ID, scenario name, target, collusion group,
or expected alert.

### 7.2 Deterministic rule-evidence cases

Create at least these acceptance cases:

1. Six shared hands where A folds and B wins at least four times within 24
   hours. Expect `pair.repeated-fold-to-partner-wins` by the fifth qualifying
   hand.
2. A suspicious pair sharing one device. Expect `pair.same-device` evidence on
   each shared hand.
3. A suspicious pair sharing a network but not a device. Expect
   `pair.same-network` and no same-device evidence.
4. A fold-benefit hand. Expect `pair.one-folded-other-won` when the precise
   current-hand condition is true.
5. A clean high-volume multi-tabler with no configured relationship. Expect no
   evidence merely because the player is active at several tables.
6. An innocent household pair. Same-device or same-network evidence may fire,
   but the label remains negative so precision and policy behavior are tested.

The oracle records rule ID, rule version, pair key, qualifying hand, minimum
and maximum expected firing counts, and reason codes.

### 7.3 Evidence is not automatically a final alert

The current review policy treats all seven rules as soft evidence. A final
`review_recommended` outcome is produced when the model crosses its registered
threshold; there are currently no hard rules.

Therefore:

- rule cases can guarantee evidence but cannot falsely claim a model alert;
- the frozen champion must score the alert-acceptance pack;
- threshold-positive acceptance rows are recorded against the exact model,
  decision-policy, and review-policy hashes; and
- a future model change requires regenerating the score oracle, not changing
  the underlying hand truth.

If a realistic strong positive case does not cross the model threshold, report
that as a model miss. Do not silently modify its test label or inject a
forbidden feature.

## 8. Leakage-safe split rules

Apply all of these gates:

1. Assign the dataset, benchmark, and split before generating entities.
2. Keep every row from one hand in one split and one cross-validation fold.
3. For cold start, require zero player and pair overlap between splits.
4. For new relationship, require zero protected positive-pair overlap even
   when individual users overlap.
5. For temporal data, compare features only with events strictly earlier than
   the scored hand.
6. Fit encoders, sampling policies, imputers, feature selection, calibration,
   and thresholds on train/validation only.
7. Open public test once after choices are frozen.
8. Keep challenge labels outside the scoring identity until replay completes.
9. Exclude raw user, table, session, device, network, case, and collusion-group
   identifiers from model tensors.
10. Store `label_available_at`; never train on a label before it was available.
11. Keep the alert-acceptance product out of every model-quality computation.
12. Hash every event, label, configuration, split-assignment, and oracle file.

Scenario intensities and prevalence may be tuned using train and validation.
They must be frozen before test and challenge generation.

## 9. Target data flow

### 9.1 Frozen generation

```text
root seed + profile
        |
        v
player population and point-in-time context
        |
        v
session and multi-table scheduler
        |
        v
table seats + independent table clocks
        |
        v
scenario planner
        |
        v
PokerKit legal hand execution
        |
        +----> public hand events
        +----> private labels and case oracle
        +----> context snapshot/history seed
        +----> manifest, statistics, and hashes
```

### 9.2 Online replay

The current production-shaped boundary remains:

```text
context snapshot/history ------> Snowflake USER_CONTEXT history/current
                                      ^
                                      | lazy point-in-time lookup
                                      |
hand events -> Confluent Kafka -> POKER_FLINK -> POKER_RISK
                                                   |
                         evidence / scores / decisions / alerts
                                                   |
                                                   v
                                          Snowflake + admin
```

Only hands are replayed into the canonical input Kafka path. Snowflake contains
the 10,000-user context population, but Flink loads context only for players
who appear in hands and retains it under the existing 36-hour inactivity TTL
and 60-minute refresh policy. A full context-table bootstrap into Flink is not
allowed.

Private labels and alert oracles are loaded only after scoring, using the
restricted evaluation path.

## 10. Generator design

Add these responsibilities without moving poker mechanics out of PokerKit:

```text
pipeline/generator/
  population.py             registered, daily-active, and context population
  simulation_clock.py       deterministic event and per-table clocks
  session_scheduler.py      user sessions and simultaneous-table limits
  table_scheduler.py        100 tables, 4-6 seats, joins, and leaves
  scenario_planner.py       multi-hand case activation and co-seating
  alert_oracle.py           private expected evidence and score lineage
  multitable_world.py       orchestration and frozen artifact writing
```

Extend the hand generator so that it accepts:

- an explicit table ID;
- an explicit ordered seat list;
- 4, 5, or 6 players;
- an explicit hand start/completion time;
- an optional planned scenario; and
- stable per-table hand sequence numbers.

Use position maps appropriate to each table size and pass the actual player
count and stack tuple to PokerKit. Remove the assumptions that every hand has
six players and every hand creates 15 pair rows.

Configuration should live in a versioned JSON file rather than command-line
flags alone. The resolved configuration is copied into the dataset directory
and hashed.

## 11. Implementation phases

### D1 — Configuration and contracts

- [x] Define a versioned multi-table profile schema.
- [x] Separate registered users, daily active users, peak concurrent users,
  tables, and hands.
- [x] Define table-seat interval, scenario-case, group-label, and scenario-hand
  private contracts. The model/rule alert oracle remains intentionally deferred
  to D6.
- [x] Add manifest fields for occupancy, concurrency, and event-time
  distributions.

Exit gate: invalid capacity and impossible concurrency configurations fail
before generation.

### D2 — Variable-size PokerKit hands

- [x] Refactor hand execution to accept explicit 4–6 player seats and table
  time.
- [x] Add 4-, 5-, and 6-player position maps.
- [x] Preserve deterministic legal actions and balanced settlement.
- [x] Expand each hand into `C(num_players, 2)` pair rows.

Exit gate: golden hands for all three sizes are deterministic, legal, and
balanced.

### D3 — Sessions, seats, and 100 table clocks

- [x] Implement daily-active selection from the registered population.
- [x] Assign user session intervals and target simultaneous-table counts.
- [x] Fill exactly 100 tables with the configured 4/5/6 mix.
- [x] Maintain seat continuity, joins, leaves, and independent table clocks.
- [x] Record observed occupancy and multi-table histograms.

Exit gate: the smoke profile has 100 active tables, no duplicate same-table
seat, no user above the table limit, and reproducible schedules.

### D4 — Multi-hand scenario planner

- [x] Convert the existing four collusion strategies into scheduled cases.
- [x] Guarantee required co-seating and minimum supporting-hand counts.
- [x] Add difficult negatives and configurable positive prevalence.
- [x] Add group/ring labels for future graph evaluation.
- [x] Keep scenario metadata only in private sidecars.

Exit gate: requested scenario counts and constraints equal the generated
manifest, with no private field in public schemas.

### D5 — Split builders and leakage audit

- [x] Build cold-start, temporal, and new-relationship products.
- [x] Keep complete hands grouped.
- [x] Add player, pair, group, and time-overlap audits.
- [x] Add train-only preprocessing and validation-only threshold checks.
- [x] Preserve the sealed challenge workflow.

Exit gate: a machine-readable leakage report passes all rules in section 8.

Implementation:

- `pipeline/generator/multitable_benchmarks.py` builds deterministic,
  source-level hand-assignment indexes over the immutable D4 world.
- `scripts/build_multitable_benchmarks.py` and
  `scripts/check_multitable_benchmarks.py` are the build and independent
  verification entry points.
- Assignment and audit contracts are versioned under `schemas/generator/`.
- The assignment product contains no labels, features, or copied events.
  Downstream feature builders must join by `hand_id` and preserve the recorded
  preprocessing, threshold-selection, warm-up, and test-access policies.
- Source integrity verification deliberately skips challenge private-label
  artifacts, and a regression guard fails if code attempts to open them.

### D6 — Alert-acceptance pack

- [ ] Generate deterministic rule-positive and rule-negative cases.
- [ ] Replay features through the Java/Flink rule path.
- [ ] Score through the frozen Go/CatBoost path.
- [ ] Select and seal at least ten model-threshold-positive demo rows.
- [ ] Record exact evidence, decision, alert, sink, and admin expectations.
- [ ] Prove that this dataset is rejected by training commands.

Exit gate: replay produces the expected evidence and at least ten final alerts
for the registered model/policy versions.

### D7 — Snowflake, Confluent, and SPCS smoke

- [ ] Seed the internal Snowflake context projection.
- [ ] Publish only hand events to the canonical Confluent input topic.
- [ ] Confirm lazy context loads for observed players only.
- [ ] Verify player, pair, score, evidence, decision, and alert row counts.
- [ ] Confirm alerts are visible in admin with source hand and evidence lineage.

Exit gate: manifest counts reconcile through Kafka and Snowflake, and the admin
view displays the sealed alert-acceptance cases.

### D8 — Model benchmark and promotion report

- [ ] Build the development dataset and retrain the CatBoost baseline.
- [ ] Evaluate cold-start, temporal, new-relationship, and scenario segments.
- [ ] Run the full benchmark only after development gates pass.
- [ ] Compare CatBoost with future sequence and graph candidates using the same
  frozen splits.
- [ ] Register a model only through the existing promotion gates.

Exit gate: reproducible metrics, confidence intervals, calibration, threshold,
alert volume, and manifest lineage are published without opening challenge
labels early.

## 12. Test and acceptance matrix

### Generator correctness

- Repeating a seed produces identical schedules, events, labels, and hashes.
- Exactly 100 tables are active during the configured peak window.
- Every table has 4–6 unique players.
- A user has no more than five simultaneous table seats.
- The observed concurrency histogram is within configured tolerance.
- Every action actor is seated in that hand.
- Every hand settles correctly and chip totals balance.
- Each hand produces exactly `C(num_players, 2)` canonical pair labels.
- Event time increases per table and may interleave across tables.

### Data and leakage

- Public records contain no target, scenario, group, or alert-oracle fields.
- Cold-start populations are disjoint.
- Protected new relationships do not appear in train.
- Temporal features use prior events only.
- Context selected for a hand is effective at or before its event time.
- Challenge labels cannot be read by online service identities.
- Alert-acceptance IDs are absent from benchmark manifests.

### Rules and alerts

- Every oracle rule case matches exact versioned evidence.
- Repeated-fold evidence respects 5-hand, 3-directional-count, 0.60-rate, and
  24-hour constraints.
- Household and network hard negatives remain negative labels even when soft
  evidence fires.
- Model score and alert expectations are bound to model/policy hashes.
- Admin rows link back to hand, pair, score, evidence, and decision IDs.

### Operational load

- Kafka acknowledgements match the manifest.
- Duplicate replay does not duplicate warehouse rows.
- Table-key partitioning preserves per-table hand order.
- Flink context cache contains only observed active users.
- Cache miss, hit, refresh, expiry, and late-hand paths are measured.
- Kafka lag drains after the peak burst.
- SPCS services remain healthy through the development replay.

## 13. Evaluation report

Report at least:

- pair-level PR-AUC, ROC-AUC, precision, recall, F1, and calibration;
- case-level detection rate: whether any pair/hand in a scenario was detected;
- time and number of hands to first evidence and first final alert;
- recall by scenario family and intensity;
- false-alert rate for clean multi-tablers, households, shared networks, strong
  players, and table hoppers;
- evidence and final alerts per 1,000 hands;
- review rate against the existing 2% rollout gate;
- results by 4-, 5-, and 6-player table;
- results by one through five simultaneous tables;
- cold-start, temporal, and new-relationship performance;
- cache miss/hit/refresh rates and lookup latency; and
- end-to-end hand-to-alert latency.

Use complete hands or complete scenario cases as bootstrap sampling units.
Never bootstrap individual pair rows as though the rows from one hand were
independent.

## 14. Stable entry points

D1–D5 currently expose:

```bash
make multitable-data-test
make multitable-data-smoke
make multitable-benchmarks-test
make multitable-benchmarks
make multitable-benchmarks-check
```

D6–D8 should add:

```bash
make multitable-alert-acceptance
make multitable-alert-replay-local
make multitable-alert-replay-spcs
make multitable-model-benchmark
```

Each command writes a run report with the resolved configuration, source commit,
artifact hashes, counts, duration, and pass/fail gates.

## 15. Recommended next implementation slice

D1–D5 are complete: configuration and capacity checks, PokerKit 4–6 player
hands, the deterministic 100-table scheduler, point-in-time context, scheduled
positive and difficult-negative cases, group truth, benchmark assignments, and
machine-readable leakage gates are implemented.

The next implementation slice is D6: build a training-excluded
alert-acceptance pack, replay its deterministic rule cases through Flink, score
it through the frozen Go/CatBoost path, and seal the exact evidence, decision,
alert, sink, and admin expectations.
