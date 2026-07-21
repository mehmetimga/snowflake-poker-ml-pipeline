# Realtime Model Input Contract

## Purpose

This document defines the exact input to the production realtime poker-risk
model. It distinguishes source events from derived Flink features, identifies
fields that are carried only for routing and audit, and defines the data that is
forbidden at inference time.

The current promoted contract is:

| Item | Value |
|---|---|
| Model | Pair-level CatBoost classifier |
| Model artifact | `models/pair-catboost-full-v2` |
| Model name | `pair-catboost-v1` |
| Feature definition | `pair-features-v1` |
| Prediction unit | One unordered player pair in one completed hand |
| Pairs per six-player hand | 15 |
| Model input row | 58 ordered `float32` values |
| Realtime model batch | `[15, 58]` for one complete six-player hand |
| Calibration | Validation-fitted Platt calibration |
| Player aggregation | Maximum calibrated probability among the player's pairs |
| Hand aggregation | Maximum calibrated pair probability |

The artifact files, rather than this prose, are authoritative for a deployed
model run:

- [`scoring_contract.json`](../models/pair-catboost-full-v2/scoring_contract.json)
  fixes the input tensor name, type, shape, and feature order.
- [`preprocessing.json`](../models/pair-catboost-full-v2/preprocessing.json)
  fixes numeric fill values, categorical values, and output columns.
- [`calibration.json`](../models/pair-catboost-full-v2/calibration.json) fixes the
  calibration parameters.
- [`decision_policy.json`](../models/pair-catboost-full-v2/decision_policy.json)
  fixes pair count, aggregation, threshold, and policy version.

Never infer the production input order from a Python dictionary, Kafka JSON
field order, or this document. The scorer must read and validate the immutable
artifact contract.

## End-to-end input flow

```text
completed hand event ────────────────┐
                                     │
point-in-time user context ──────────┼─> Java/Flink keyed state
                                     │      • temporal as-of join
prior user and pair events ──────────┘      • prior-only rolling state
                                            • six players -> 15 pairs
                                            • pair-features-v1 snapshots
                                                       │
                                                       v
                                         poker.pair-features.v1
                                                       │
                                                       v
                                            Go POKER_RISK service
                                      validate -> flatten -> preprocess
                                                       │
                                                       v
                                           one [15, 58] CatBoost batch
                                                       │
                                                       v
                                   calibrate -> aggregate -> threshold -> alert
```

The realtime scorer consumes Kafka feature events. It does not query
PostgreSQL or Snowflake for each hand. PostgreSQL is the future operational
source, Kafka is the live transport, Flink state provides online history and
point-in-time context, and Snowflake remains the durable analytical store.

## Source events

### Completed hand

`poker.hands.raw.v1` supplies a completed-hand envelope containing:

- `hand_id`, `table_id`, and `played_at`.
- Blinds, pot size, board, and player count.
- Players, positions, starting stacks, cards, and won amounts.
- The ordered action timeline: street, player, action type, and amount.

The v1 model uses only derived position, action, pot, and outcome values. It
does not directly use names, raw player IDs, starting stacks, cards, board
cards, blind values, or raw timestamps.

This is post-hand detection. Final pot and won amounts are valid current-hand
inputs because scoring starts after settlement. A future in-hand intervention
model would require a different event and feature contract that excludes
future actions and settlement.

### User context

`poker.user-context.v1` carries versioned context with an `effective_at` time:

- Account creation time.
- Country bucket and timezone.
- Acquisition channel.
- KYC level and account status.
- Bankroll and preferred-stake buckets.
- Skill rating.
- Tokenized device ID and network-cluster ID.

Flink joins the newest context version satisfying
`effective_at <= hand.played_at`. It never silently substitutes the current
database row for missing historical context.

The v1 feature contract uses account age, country/timezone/acquisition equality,
bankroll/stake distance, skill, device equality, and network equality. KYC level
and account status are carried by the source contract but are not direct v1
model inputs.

### Session and account-link events

`poker.session-context.v1` and `poker.account-links.v1` are canonical platform
inputs for session, shared-device, shared-network, and household relationships.
They support state, investigation, and future feature versions. The exact
`pair-features-v1` CatBoost tensor does not directly contain session IDs,
link types, confidence buckets, or account-link IDs.

Adding session-overlap or explicit relationship-graph features requires a new
feature definition and a newly trained model. Do not append them to v1 in
place.

## Pair-feature event

Flink emits one `poker.pair-features.computed` event for each canonical pair
where `player_a < player_b`. A feature event has five groups:

```json
{
  "current_hand": {},
  "context": {},
  "user_history_a": {},
  "user_history_b": {},
  "pair_history": {}
}
```

It also carries tenant, product, dataset, hand, table, player, event, context
version, source revision, snapshot revision, timestamp, and trace fields. These
fields validate ownership, completeness, ordering, lineage, corrections, and
deduplication. They are not direct CatBoost input columns.

The event may also carry `upstream_rule_evidence`, a bounded list of complete
`poker.rule-evidence.v1` records produced by stateful Flink rules. The Go scorer
validates that each record has the same tenant, product, dataset, split, trace,
hand, pair, revision, effective time, and feature version as its feature event.
This is transport and audit metadata only: it is never flattened into the
model vector and cannot change CatBoost probability.

For a six-player hand, the Go scorer waits for all 15 unique pair snapshots for
the same tenant and hand. A higher `snapshot_revision` can replace a prior
snapshot and trigger deterministic re-scoring during the correction window.

The current `poker.pair-features.v1` topic is keyed by `pair_key`, so the first
deployment must run one scorer consumer replica. Before horizontal scaling,
Flink must publish or repartition to a topic keyed by `tenant_id + hand_id` so
all 15 pairs for a hand are assigned to one consumer-group member.

## Exact 58-feature model vector

The first 54 columns are numeric or boolean values. Booleans become `0.0` or
`1.0`. The last four columns are the frozen one-hot representation of the two
context-join status fields.

### Current-hand pair features: columns 1–18

| # | Ordered feature | Meaning |
|---:|---|---|
| 1 | `current_position_index_a` | Player A position: `UTG=0`, `MP=1`, `CO=2`, `BTN=3`, `SB=4`, `BB=5` |
| 2 | `current_position_index_b` | Player B position using the same mapping |
| 3 | `current_position_gap` | Absolute difference between the two position indexes |
| 4 | `current_invested_amount_a` | Sum of Player A's action amounts in the hand |
| 5 | `current_invested_amount_b` | Sum of Player B's action amounts in the hand |
| 6 | `current_invested_pot_ratio_a` | A invested amount divided by final pot size |
| 7 | `current_invested_pot_ratio_b` | B invested amount divided by final pot size |
| 8 | `current_invested_abs_diff_ratio` | Absolute invested-amount difference divided by pot size |
| 9 | `current_won_amount_a` | Amount awarded to A at settlement |
| 10 | `current_won_amount_b` | Amount awarded to B at settlement |
| 11 | `current_outcome_abs_diff_ratio` | Absolute won-amount difference divided by pot size |
| 12 | `current_aggressive_actions_a` | Count of A's `bet` and `raise` actions |
| 13 | `current_aggressive_actions_b` | Count of B's `bet` and `raise` actions |
| 14 | `current_fold_actions_a` | Count of A's fold actions |
| 15 | `current_fold_actions_b` | Count of B's fold actions |
| 16 | `current_both_saw_flop` | Both players produced an action on the flop |
| 17 | `current_both_saw_river` | Both players produced an action on the river |
| 18 | `current_one_folded_other_won` | One member folded and the other received a positive won amount |

### Point-in-time context features: columns 19–33

| # | Ordered feature | Meaning |
|---:|---|---|
| 19 | `context_context_missing_a` | No valid context version was available for A at hand time |
| 20 | `context_context_missing_b` | No valid context version was available for B at hand time |
| 21 | `context_skill_rating_a` | A's point-in-time skill rating |
| 22 | `context_skill_rating_b` | B's point-in-time skill rating |
| 23 | `context_skill_rating_abs_diff` | Absolute skill-rating difference |
| 24 | `context_account_age_days_a` | A account age at `played_at` |
| 25 | `context_account_age_days_b` | B account age at `played_at` |
| 26 | `context_account_age_abs_diff_days` | Absolute account-age difference |
| 27 | `context_same_country` | Both point-in-time country buckets match |
| 28 | `context_same_timezone` | Both timezones match |
| 29 | `context_same_acquisition_channel` | Both acquisition channels match |
| 30 | `context_same_device` | Both tokenized device IDs match |
| 31 | `context_same_network` | Both network-cluster IDs match |
| 32 | `context_bankroll_bucket_distance` | Ordinal distance between low, medium, and high bankroll buckets |
| 33 | `context_preferred_stake_bucket_distance` | Ordinal distance between micro, low, medium, and high stake buckets |

Raw device IDs, network identifiers, countries, timezones, and acquisition
channels do not enter CatBoost. V1 uses equality or distance features so the
model cannot memorize those identifiers or individual categories.

### Prior user history: columns 34–45

All history values are snapshotted strictly before applying the current hand.

| # | Ordered feature | Meaning |
|---:|---|---|
| 34 | `user_a_hands_seen` | Previous hands observed for A |
| 35 | `user_a_total_won_amount` | A's cumulative prior won amount |
| 36 | `user_a_mean_won_amount` | A's mean prior won amount |
| 37 | `user_a_fold_rate` | Fraction of A's prior hands containing a fold |
| 38 | `user_a_raise_rate` | Fraction of A's prior hands containing a raise |
| 39 | `user_a_saw_flop_rate` | Fraction of A's prior hands reaching the flop |
| 40 | `user_b_hands_seen` | Previous hands observed for B |
| 41 | `user_b_total_won_amount` | B's cumulative prior won amount |
| 42 | `user_b_mean_won_amount` | B's mean prior won amount |
| 43 | `user_b_fold_rate` | Fraction of B's prior hands containing a fold |
| 44 | `user_b_raise_rate` | Fraction of B's prior hands containing a raise |
| 45 | `user_b_saw_flop_rate` | Fraction of B's prior hands reaching the flop |

### Prior shared-pair history: columns 46–54

These values also exclude the current hand.

| # | Ordered feature | Meaning |
|---:|---|---|
| 46 | `pair_hands_together` | Previous hands containing both A and B |
| 47 | `pair_total_won_amount_a` | A's cumulative won amount in prior shared hands |
| 48 | `pair_total_won_amount_b` | B's cumulative won amount in prior shared hands |
| 49 | `pair_outcome_asymmetry` | Absolute cumulative won-amount imbalance normalized by their combined amount |
| 50 | `pair_a_fold_b_win_rate` | Prior shared-hand rate where A folded and B won |
| 51 | `pair_b_fold_a_win_rate` | Prior shared-hand rate where B folded and A won |
| 52 | `pair_both_saw_flop_rate` | Prior shared-hand rate where both reached the flop |
| 53 | `pair_same_table_rate` | Fraction of prior shared hands played at the current table |
| 54 | `pair_last_seen_age_seconds` | Seconds since A and B were last observed together |

`table_id` therefore influences the aggregate same-table rate but is not itself
a model column. Similarly, timestamps produce account-age and recency values but
raw timestamp values do not enter the tensor.

### Encoded context status: columns 55–58

| # | Ordered feature | Meaning |
|---:|---|---|
| 55 | `context_status_a==matched` | A received an on-time matched context version |
| 56 | `context_status_a==__UNKNOWN__` | A status is outside the fitted `matched` category |
| 57 | `context_status_b==matched` | B received an on-time matched context version |
| 58 | `context_status_b==__UNKNOWN__` | B status is outside the fitted `matched` category |

The event contract can carry `matched`, `matched_late`, `missing`, or
`corrected`. The current preprocessing artifact was fitted with `matched` plus
the reserved `__UNKNOWN__` category, so `matched_late`, `missing`, and
`corrected` encode as unknown. The explicit missing indicators in columns 19
and 20 preserve the distinction between unavailable context and other join
statuses.

## Missing and invalid values

- Missing context is explicit; it is never replaced with a newer database row.
- Numeric fill values are frozen in `preprocessing.json` from the training
  artifact. They must not be recomputed in the realtime service.
- Missing, non-numeric, `NaN`, and infinite numeric inputs use the contract's
  frozen fill value.
- Unrecognized context-status categories map to `__UNKNOWN__`.
- Feature rows with the wrong schema version, wrong pair identity, incomplete
  source lineage, or a non-six-player hand are rejected before inference.
- A hand is not scored until all 15 canonical pairs are assembled.

The scorer must emit metrics for missing context, unknown categories, invalid
features, incomplete/expired hand assemblies, corrections, and DLQ writes.
Unexpected changes in those rates are data-quality incidents even when the
preprocessor can technically fill the value.

## Inputs excluded from inference

### Labels and private synthetic truth

The following fields are forbidden anywhere inside an inference feature group:

- `collusion_group_id`
- `collusion_pair_id`
- `collusion_scenario`
- `is_collusive`
- `is_suspicious`
- `label`
- `label_available_at`
- `target`

The Go scorer recursively rejects these names before flattening features.
Labels live in restricted sidecars or Snowflake label tables and become
available only to authorized offline training and evaluation jobs.

### Identifiers and routing metadata

The following values are required for processing but not included in the
58-feature tensor:

- Tenant, product, dataset, split, event, trace, hand, table, player, pair, and
  source-event identifiers.
- Context and snapshot revision numbers.
- Raw event timestamps; only derived age and recency values are used.
- Kafka partition, offset, producer, and ingestion metadata.
- Validated upstream rule-evidence records and their deterministic IDs.

Keeping identity outside the model prevents user, pair, table, tenant, or split
memorization. These fields still enforce isolation, deterministic replay,
idempotency, and auditability.

### Raw or unused source attributes

V1 does not use:

- Player names or other direct PII.
- Raw IP addresses or un-tokenized device identifiers.
- Hole cards or board cards.
- Starting stack or blind values as direct columns.
- KYC level or account status.
- Session ID, explicit account-link type, or link confidence.
- Future hands, future context versions, future graph edges, or investigation
  outcomes that were unavailable at scoring time.

These exclusions are contract decisions, not an assertion that the information
can never be useful. Each addition must have a legitimate risk purpose, privacy
and fairness review, online/offline parity, and measurable out-of-sample lift.

## Scoring and decision policy

The 58 inputs produce one raw positive-class probability per pair. For a
six-player hand:

1. The Go service creates the ordered `[15, 58]` `float32` batch.
2. CatBoost ONNX scores all 15 rows in one call, embedded in Go or through the
   optional local Triton sidecar.
3. The service applies the frozen Platt calibration parameters.
4. Each player's risk is the maximum calibrated probability among pairs that
   include the player.
5. Hand risk is the maximum calibrated probability among all 15 pairs.
6. The frozen decision threshold produces the alert decision.

The current artifact threshold is approximately `0.984192`, selected on
validation data with a maximum 2% alert-rate budget. This value is specific to
the current model run and must be loaded from `decision_policy.json`, never
hard-coded into the service or topic schema.

## Versioning and change policy

`pair-features-v1` is immutable. Adding, removing, renaming, reordering, or
changing the meaning of any feature requires:

1. A new feature definition such as `pair-features-v2`.
2. Updated canonical event and Schema Registry contracts.
3. Matching Java/Flink and Python offline-oracle implementations.
4. Cross-language parity fixtures for identical event histories.
5. A new frozen training dataset and preprocessing artifact.
6. Retraining, calibration, threshold selection, and full promotion gates.
7. A new scoring contract and immutable artifact hashes.
8. Go scorer compatibility plus shadow-mode comparison.
9. Controlled model rollout and a tested rollback artifact.

Likely future candidates include session overlap, behavioral timing, device and
network churn, table-selection distributions, card-equity/action-EV features,
and prior-only relationship-graph embeddings. None belongs in production v1
until it demonstrates incremental value without leakage or unacceptable proxy
risk.

## Validation checklist

Before deploying a scorer or feature-job change, verify:

- Every six-player hand yields exactly 15 canonical pair events.
- Online Flink features equal the Python offline oracle for the same event
  history.
- Rolling user and pair snapshots exclude the current hand.
- Corrected context changes only the affected pair snapshots and increments the
  snapshot revision.
- The scorer verifies `pair-features-v1`, artifact hashes, model run, and
  decision-policy version.
- Preprocessing emits exactly 58 finite `float32` values in contract order.
- Forbidden label fields are rejected and written to the versioned DLQ.
- Kafka output acknowledgement occurs before input offsets become committable.
- Score and alert IDs remain deterministic under replay.
- Missing-context, DLQ, correction, latency, and incomplete-hand metrics are
  observable.

Relevant checks include:

```bash
make flink-pair-features-test
make pair-features-check
make go-risk-test
make go-risk-check
```

## Implementation references

- Canonical Python contracts:
  [`pipeline/events/contracts.py`](../pipeline/events/contracts.py)
- Deterministic Python feature oracle:
  [`pipeline/features/pair_features.py`](../pipeline/features/pair_features.py)
- Java/Flink production feature job:
  [`streaming/flink-java/pair-features`](../streaming/flink-java/pair-features)
- Go validation and preprocessing:
  [`services/go/internal/risk/features.go`](../services/go/internal/risk/features.go)
- Go hand assembly and scoring:
  [`services/go/internal/risk/assembler.go`](../services/go/internal/risk/assembler.go)
  and [`services/go/internal/risk/scorer.go`](../services/go/internal/risk/scorer.go)
- Architecture and model roadmap:
  [`realtime-context-ml-implementation-plan.md`](realtime-context-ml-implementation-plan.md)
- Production component diagram:
  [`poker-ml-production-components.excalidraw`](poker-ml-production-components.excalidraw)
