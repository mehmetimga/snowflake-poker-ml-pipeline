# Review decision policy v1

Phase B4 separates three concepts that previously met at the alert boolean:

- the CatBoost score estimates statistical risk;
- rules publish governed evidence; and
- the review policy decides whether an analyst-review action is appropriate.

The model probability is never rewritten by the policy. The model artifact's
`decision_policy.json` still owns calibration aggregation and the validation
threshold. The separate review-routing policy is frozen in
[`schemas/policies/review-policy-v1.json`](../schemas/policies/review-policy-v1.json).

## Current shadow behavior

The current policy is `poker.review-routing:v1` in `shadow` mode:

| Input | Outcome | Action |
|---|---|---|
| Model threshold not exceeded; zero or more soft rules | `no_review` | `none` |
| Model threshold exceeded; zero or more soft rules | `review_recommended` | `analyst_review` |
| Any explicitly configured hard rule | `mandatory_review` | `analyst_review` |

All six B2 rules and the first B3 stateful rule are explicitly classified as
`soft`. The configured hard-rule list is empty. Therefore no current rule can
mandate review or override the model threshold.

Soft evidence remains attached to the decision so an analyst can filter and
explain it. A soft rule firing by itself produces `no_review`.

## Event contract

Every complete-hand score produces one `poker.review-decision.recorded` event
on `poker.review-decisions.v1`. Its deterministic UUIDv5 identity binds:

```text
tenant + product + dataset + split
+ review policy ID and version
+ risk-score event ID
```

The payload contains the score reference, hand identity, policy identity,
mode, outcome, action, explicit reason codes, threshold-exceeded flag, and the
category of every rule-evidence reference. It intentionally does not copy or
blend model probability.

The Python contract and offline oracle live in
[`pipeline/events/contracts.py`](../pipeline/events/contracts.py) and
[`pipeline/policy/review.py`](../pipeline/policy/review.py). The matching Go
runtime is
[`services/go/internal/risk/review_policy.go`](../services/go/internal/risk/review_policy.go).
The static event schema is
[`poker.review-decision.v1.schema.json`](../schemas/events/poker.review-decision.v1.schema.json).
Both runtimes consume the same
[`review-policy-v1.golden.json`](../schemas/examples/review-policy-v1.golden.json)
fixture.

## Kafka acknowledgement boundary

For a completed hand, the Go streaming adapter publishes one acknowledged
batch in this order:

```text
0..N rule-evidence events
1 risk-score event
1 review-decision event
0..1 risk-alert event
```

Only after the entire batch is acknowledged are the input offsets eligible to
commit. A risk alert references both the risk score and review decision.
Replay produces the same score, evidence, decision, and alert IDs.

Invalid JSON, incompatible contracts, unauthorized tenants, invalid partition
keys, and rejected hand assemblies still publish only a
`poker.pipeline.dead-lettered` event. They never become rule evidence or a
review decision.

## Future hard rules

A hard rule must be explicitly added to a new governed policy version. Its
decision contains a reason such as:

```text
hard-rule.security.confirmed-account-control.v1
```

Hard rules may require review even when the model score is below its threshold,
but they still cannot change the stored probability. Before adding one, risk
operations must approve its owner, reason code, independent audit evidence,
rollback, expected review volume, and false-positive evaluation.

## Shadow rollout gates

The policy file freezes these initial gates:

- maximum total review rate: `0.02`;
- maximum mandatory-review rate: `0.0` while no hard rule is approved; and
- minimum sample before evaluating the gates: `1,000` decisions.

The Go processor counts total decisions, recommendations, mandatory reviews,
and both rates. A rate limit is an observability/promotion gate; it does not
silently discard an audit event.

Run the complete local gate with:

```bash
make phase-b4-check
```

Production promotion still requires dashboards, alert-volume review, durable
warehouse retention, and a shadow replay against real poker-server traffic.
