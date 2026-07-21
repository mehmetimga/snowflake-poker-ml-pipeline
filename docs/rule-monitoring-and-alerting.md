# Rules v2 monitoring and alerting

Phase B6 turns the B5 public-test baseline into a delayed-label monitoring
workflow. It combines live runtime counters with a scheduled quality report,
Prometheus alerts, Grafana panels, and a Streamlit view. All current rules
remain shadow evidence. Monitoring cannot block, penalize, or automatically
disable a user or rule.

## Monitoring flow

```text
Go / Flink runtime
  rule firings, enablement, lateness, lag, state size
                         |
                         v
                 Prometheus / Grafana

independent delayed labels + persisted rule evidence
                         |
                         v
scheduled rule-monitor job ----> hash-bound JSON report
                         |         deterministic alerts
                         +-------> Prometheus textfile
                         +-------> Streamlit Rules v2 page
```

The local synthetic replay uses the frozen B5 report and public test data.
Phase C will replace the replay window with warehouse aggregates over real
shadow evidence and independently confirmed analyst labels.

## Runtime metrics

Both Go scoring entry points export acknowledged counters:

- `risk_scorer_scope_hands_scored_total`;
- `risk_scorer_scope_pairs_scored_total`;
- `risk_scorer_rule_evidence_total`; and
- `risk_scorer_rule_enabled`.

Labels bind tenant, product, model name/run, rollout ID, rule ID/version, and
runtime. The Kafka adapter increments firing counters only after the complete
evidence, score, decision, and optional alert batch is acknowledged. It serves
`/metrics` at `RISK_METRICS_LISTEN`, defaulting to
`127.0.0.1:9091`. The HTTP scorer includes the same lineage on its existing
metrics endpoint.

Flink already exports the stateful rule's evaluation, firing, duplicate,
correction, stale, late-event, event-time-lag, and keyed-state-size metrics.
The B6 dashboard adds the firing, lateness, lag, and state views.

## Delayed-label window

[`poker.rule-monitoring-window.v1.schema.json`](../schemas/events/poker.rule-monitoring-window.v1.schema.json)
defines the input to the scheduled job. Every window carries:

- tenant and product;
- event interval and label cutoff;
- dataset/benchmark identity;
- model name and run;
- rollout identity;
- B5 evaluation ID and payload hash;
- independent, circular, and unknown label counts; and
- firing and true-positive counts for every versioned rule.

The window is hash-bound. A changed tenant, count, rule identity, or source
artifact fails deterministic validation.

## Eligibility and statuses

The B5 contract requires at least 250 labeled hands, 20 positive labels, and
20 firings for each rule. Status semantics are:

| Status | Meaning | Alert |
|---|---|---|
| `ok` | Eligible and within every B5 threshold | No |
| `warning` | Eligible and one threshold violated | Yes |
| `critical` | Multiple violations or bad label provenance | Yes |
| `insufficient_data` | One or more reliability floors are not met | No quality alert |
| `disabled` | Governed rollout disables the rule | Informational runtime alert |

`insufficient_data` is not a pass. It remains visible on dashboards and can
raise an informational alert after 24 hours, but precision and drift are not
interpreted until the floor is reached.

Eligible rules are checked for:

- firing rate outside the B5 absolute/relative band;
- precision below the configured ratio to baseline;
- evidence volume above the configured ratio to baseline; and
- any circular or unknown labels.

## Deterministic alerts

Alerts conform to
[`poker.rule-monitoring-alert.v1.schema.json`](../schemas/events/poker.rule-monitoring-alert.v1.schema.json).
Their UUIDv5 identity includes tenant, product, window, rule/version,
evaluation, rollout, and sorted reason codes, so replay produces the same alert
ID. Every alert links:

- tenant/product and monitoring interval;
- B5 evaluation ID and payload hash;
- rollout ID;
- model name/run;
- rule ID/version;
- observed values and exact thresholds; and
- a non-automatic recommended action.

Synthetic drift tests cover rate, precision, and volume violations. They also
verify that circular labels become critical and that alert records always set
`automatic_rule_disable=false` and `enforcement_authority=false`.

## Dashboard and Prometheus integration

The scheduled job writes `rule_monitoring.prom` with:

- per-rule status and eligibility;
- firing rate, precision, recall, and evidence volume;
- labeled hands, independent label rows, and positive-label yield;
- deterministic alerts by severity and reason; and
- circular/unknown label counts.

Use
[`rule-monitoring-dashboard.json`](../ops/grafana/rule-monitoring-dashboard.json)
and
[`rule-monitoring-alerts.yml`](../ops/prometheus/rule-monitoring-alerts.yml).
The dashboard filters by tenant, rule, rollout, and model run. The Streamlit
page `7 — Rules v2 monitoring` renders the same immutable lineage, current
status, alerts, and B5 segment reliability.

## Local reproduction

```bash
make rule-monitoring-test
make rule-monitoring
make rule-monitoring-check
make phase-b6-check \
  MAVEN=/private/tmp/apache-maven-3.9.9/bin/mvn \
  MAVEN_REPO=/private/tmp/codex-m2
```

Generated local files are:

```text
models/registry/rule_monitoring_window.json
models/registry/rule_monitoring_report.json
models/registry/rule_monitoring.prom
```

The current synthetic replay is `ok` for all seven rules with zero alerts
because it deliberately replays the same frozen population used to establish
the baseline. This proves contracts and wiring; it is not evidence that a
future production population is healthy.

## Phase C production boundary

The SPCS monitor job should aggregate persisted evidence and delayed review
labels by tenant and event time, write the same window contract, run this
evaluator, and publish the JSON/textfile artifacts. Real windows must never:

- treat unreviewed or inconclusive cases as negatives;
- mix synthetic and real quality metrics;
- use rule-derived labels to evaluate that rule;
- read private challenge labels; or
- enable enforcement because a dashboard is green.
