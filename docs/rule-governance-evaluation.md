# Rules v2 governance, evaluation, monitoring, and rollback

This document describes how the seven Rules v2 signals are evaluated and how
they can be disabled safely. The rules remain shadow evidence. They do not
change the CatBoost probability, threshold, or production champion.

## What is evaluated

The governed set contains six stateless rules evaluated in Go and one
event-time rule evaluated in Java/Flink:

| Rule | Runtime | Input scope |
|---|---|---|
| `pair.one-folded-other-won` | Go risk scorer | Current hand |
| `pair.same-device` | Go risk scorer | Event-time context |
| `pair.same-network` | Go risk scorer | Event-time context |
| `pair.outcome-asymmetry` | Go risk scorer | Prior pair history |
| `pair.a-fold-b-win-rate` | Go risk scorer | Prior pair history |
| `pair.b-fold-a-win-rate` | Go risk scorer | Prior pair history |
| `pair.repeated-fold-to-partner-wins` | Java/Flink | 24-hour ordered pair state |

The source definitions are
[`pair-rules-v1.json`](../schemas/rules/pair-rules-v1.json) and
[`stateful-pair-rules-v1.json`](../schemas/rules/stateful-pair-rules-v1.json).
The evaluation policy is
[`rule-evaluation-v1.json`](../schemas/rules/rule-evaluation-v1.json).

## Data and label boundary

The report reads the frozen `cold_start/test` partition from
`data/datasets/pair-full-v2`. It verifies the dataset manifest hashes for the
feature and label Parquet files and confirms that:

- all 75,000 rows belong to 5,000 complete hands;
- the public DGX target equals the separately stored pair label;
- all labels have `provenance=synthetic` and a valid `label_available_at`;
- the pair dataset is bound to the source PokerKit world manifest;
- the source hand supplies tenant and product lineage;
- the evaluation-only scenario sidecar agrees with event, hand, and label
  identity; and
- no challenge feature or private challenge-label file is opened.

Rule-derived provenances such as `rule_derived` are excluded. Unknown
provenance fails the report instead of silently becoming truth. Scenario
lineage is used only to summarize results; it is not a model feature or rule
input.

This is synthetic evidence, not an estimate of production analyst yield. Real
shadow labels must remain separate in Phase C.

## Metrics and uncertainty

Each rule is treated as its own binary detector. A firing produces one pair
evidence event, so alert volume can exceed 1,000 per 1,000 hands when many
pairs fire in one hand. The report records support, firing count and rate,
precision, recall, and evidence volume. It also records false-positive volume.

Confidence intervals use 500 deterministic percentile-bootstrap draws. The
sampling unit is the complete `hand_id`; all 15 correlated pair rows receive
the same bootstrap multiplicity. Pair rows are never sampled independently.

Current public-test point results are:

| Rule | Firings | Precision | Recall | Evidence / 1,000 hands |
|---|---:|---:|---:|---:|
| One folded, other won | 11,361 | 0.00097 | 0.1467 | 2,272.2 |
| Same device | 73 | 0.23288 | 0.2267 | 14.6 |
| Same network | 2,918 | 0.01748 | 0.6800 | 583.6 |
| Outcome asymmetry | 32,834 | 0.00094 | 0.4133 | 6,566.8 |
| A-fold/B-win history | 9,753 | 0.00103 | 0.1333 | 1,950.6 |
| B-fold/A-win history | 9,503 | 0.00137 | 0.1733 | 1,900.6 |
| Repeated fold-to-partner wins | 29 | 0.00000 | 0.0000 | 5.8 |

The result supports the current design: these are analyst evidence and
filters, not standalone enforcement decisions. `same-device` is the most
precise synthetic signal, `same-network` has the highest recall, and the first
stateful rule found no synthetic positives in this test window. The latter
must remain shadow-only until real data shows value.

## Segment reliability

Results are broken down by tenant, context availability, whole-hand generator
scenario, and prior-pair-history bucket. Scenario membership is propagated to
all pair rows in the hand, retaining negative comparison rows. Counts remain
visible for every slice, while metrics are suppressed unless the slice has at
least:

- 10 hands;
- 5 positives;
- 20 negatives; and
- 5 rule firings.

The current report has 51 reliable rule/segment combinations and suppresses
26. The test world contains only tenant `demo` and complete context, so it
cannot measure cross-tenant behavior or missing-context quality. Those gaps
must be filled by Phase C shadow data.

## Monitoring contract

The report emits a machine-readable baseline for every rule. A production
window is eligible for comparison only after 250 labeled hands, 20 positive
labels, and 20 rule firings. The initial warning thresholds are:

- firing-rate movement greater than the larger of 0.02 absolute or 50%
  relative;
- labeled precision below 50% of its baseline;
- evidence volume above 2 times its baseline;
- any circular or unknown label row; or
- any non-zero model-probability delta during rollback.

These are operational warning thresholds, not promotion approval. Phase B6
will connect them to dashboards and alerts. Thin windows remain
`insufficient_data` rather than passing or failing.

## Rollback

[`rule-rollout-v1.json`](../schemas/rules/rule-rollout-v1.json) is the source
of truth for per-rule enablement. Go validates that it exactly covers all
seven governed rule identities. Disable a Go rule by setting its `enabled`
field to `false`, then restart the scorer with `--rule-rollout` or
`RISK_RULE_ROLLOUT_PATH` pointing at the approved file.

For the stateful Flink rule:

1. take and retain a savepoint;
2. set `--stateful-rule-enabled false` or
   `FLINK_STATEFUL_RULE_ENABLED=false`;
3. redeploy from committed Kafka offsets; and
4. retain old evidence and the savepoint for audit.

Disabled Flink evaluation passes the pair-feature event through without
mutating rule state. Rollback never deletes historical evidence. The public
test replay disabled all rules, reduced evidence firings to zero, and retained
the identical SHA-256 over all 75,000 stored calibrated probabilities with a
maximum absolute delta of `0.0`. Go integration tests also score the same hand
with the rollout enabled and disabled and compare every model probability and
threshold decision.

## Reproduce and verify

```bash
make rule-governance-test
make rule-evaluation
make rule-evaluation-check
```

The generated local report is
`models/registry/rule_evaluation_report.json`. It binds all source files by
SHA-256, carries its own canonical payload digest, and is deterministically
recomputed by the check command. The `models/` tree is intentionally ignored
by Git; publish the report through the controlled model-registry artifact path
when this workflow is deployed.
