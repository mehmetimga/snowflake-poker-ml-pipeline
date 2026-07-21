# Rule evidence v1

`poker.rule-evidence.v1` carries explainable observations from governed rules.
It does not carry labels, model probabilities, or final policy decisions, and
its `raw_score` does not modify the CatBoost probability.

The contract is implemented in Python and Go and has one shared fixture:

- JSON Schema: [`schemas/events/poker.rule-evidence.v1.schema.json`](../schemas/events/poker.rule-evidence.v1.schema.json)
- Shared example: [`schemas/examples/poker.rule-evidence.v1.json`](../schemas/examples/poker.rule-evidence.v1.json)
- Python: `pipeline/events/contracts.py`
- Go: `services/go/internal/risk/rule_evidence.go`

## Event flow

```text
pair-features.v1
       |
       +--> governed Rules v2 evaluator --> rule-evidence.v1
       |                                      |
       +--> CatBoost scorer ------------------+--> risk-scores.v1
                                                       |
                                                       +--> risk-alerts.v1
```

Risk-score and alert payloads contain `rule_evidence_event_ids`. The rule
observations remain separate events so a rule can be versioned, disabled, or
audited without changing model calibration. B2 evaluates the six governed
pair rules and populates these references in Go scoring.

## Pair rules v1

The frozen definitions are in
[`schemas/rules/pair-rules-v1.json`](../schemas/rules/pair-rules-v1.json). They
observe `one_folded_other_won`, `same_device`, `same_network`,
`outcome_asymmetry`, and both directional fold/win rates. The continuous rules
fire for a non-zero value; the boolean rules fire when true. `raw_score` is the
observed rule strength on a 0–100 scale, not a probability.

The independently measurable rules-only benchmark remains exactly:

```text
0.20 * one_folded_other_won
+ 0.20 * same_device
+ 0.20 * same_network
+ 0.15 * outcome_asymmetry
+ 0.25 * max(a_fold_b_win_rate, b_fold_a_win_rate)
```

The two directional evidence events therefore describe separate observations,
while the legacy benchmark continues to use their maximum. Evidence scores are
not summed into either the benchmark or CatBoost probability.

## Payload

| Field | Meaning |
|---|---|
| `rule_event_id` | Deterministic replay identity; also the envelope `event_id` |
| `rule_id`, `rule_version` | Stable rule identity and immutable version |
| `rule_owner` | Team accountable for definition and review |
| `entity_type`, `entity_key` | Governed hand, pair, player, session, or account |
| `hand_id` | Hand that produced the observation |
| `observation_revision` | Source snapshot revision; corrections get a new identity |
| `severity` | `info`, `low`, `medium`, `high`, or `critical` |
| `raw_score` | Rule-local value from 0–100, never a model probability |
| `evidence` | Structured inference-safe observations supporting the rule |
| `effective_at` | Event time at which the observation became effective |
| `feature_definition_version` | Feature contract used by the evaluator |

The envelope supplies tenant, product, dataset/split, event time, emitted time,
and trace identity. Kafka records are keyed by `entity_type:entity_key` to keep
one entity's evidence ordered.

## Replay identity

Python and Go compute the same UUIDv5 using the URL namespace. The name is the
unit-separator-delimited sequence:

```text
tenant_id, product_id, dataset_id, dataset_split,
rule_id, rule_version, entity_type, entity_key, hand_id, observation_revision,
effective_at as UTC YYYY-MM-DDTHH:MM:SS.ffffffZ,
feature_definition_version
```

Replaying the same semantic observation and revision produces the same ID. A
higher source snapshot revision produces a different ID, so a correction never
collides with immutable evidence from the prior revision. Reusing an ID with
changed evidence is rejected by the idempotent warehouse loader.

## Safety boundary

Python and Go recursively reject private truth and decision leakage inside
`evidence`, including:

- labels, targets, synthetic collusion identities, and challenge fields;
- raw, calibrated, hand, risk, or final model probabilities; and
- alerts, decision-policy versions or thresholds, review requirements, and
  policy actions.

Rule thresholds and measured behavioral values are allowed when named as rule
observations, for example `rule_threshold` and `directional_fold_win_rate`.

## Kafka and warehouse

`make scoring-topics` now manages `poker.rule-evidence.v1` with 30-day
retention alongside scores and alerts.

Migration `011_rule_evidence.sql` creates:

- `RULE_EVIDENCE_EVENTS`: the immutable event and Kafka lineage;
- `RISK_SCORE_RULE_EVIDENCE`: score/model-run to rule-event references; and
- `RULE_EVIDENCE_WITH_MODEL_LINEAGE`: the joined audit view.

`pipeline.warehouse.rule_evidence` provides idempotent loaders for both event
and reference records. Replaying a score with a changed reference set replaces
its prior associations.

Run the complete B1 contract gate with:

```bash
make phase-b1-check
```

Run the B2 Python/Go definition, golden-fixture, replay, output-order, and
probability-invariance gate with:

```bash
make phase-b2-check
```
