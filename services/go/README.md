# Go risk scorer

This service owns the latency-sensitive boundary after Flink emits
`poker.pair-features.v1`. It verifies the promoted artifact hashes, loads the
versioned preprocessing/calibration/decision contracts, collects the 15
canonical pairs for each six-player hand, and submits one batched Triton V2
request.

The scorer applies Platt calibration outside the model, selects alerts with the
frozen validation threshold, and aggregates pair probabilities to player and
hand risk with the versioned max-pair policy. It retains complete hands for a
bounded TTL so a higher `snapshot_revision` can re-score a corrected hand.
Every score and alert carries the decision-policy version, service
implementation, and immutable build version; set `--build-version` (or
`RISK_SERVICE_BUILD_VERSION`) to the deployed image/source identifier.
Both outputs also carry `rule_evidence_event_ids`. Rules v2 evaluates the six
governed, inference-safe pair signals before Triton inference and emits one
separate evidence record for each fired rule. Rule evidence is never blended
into the model probability.

Pair snapshots may also carry Java/Flink stateful observations in
`upstream_rule_evidence`. The scorer validates tenant/product/dataset scope,
pair and hand identity, snapshot revision, feature version, and deterministic
event identity before combining those events with its local stateless evidence.
The metadata is excluded from preprocessing, so the `[15, 58]` model tensor is
unchanged.

Phase B4 evaluates the separate governed
[`review-policy-v1.json`](../../schemas/policies/review-policy-v1.json) after
the score exists. Every score gets a deterministic event on
`poker.review-decisions.v1`. All current rules are soft evidence and cannot
change review routing. The current hard-rule list is empty; a future approved
hard rule can mandate analyst review without changing model probability.

Run from the repository root:

```bash
make go-risk-test
make go-risk-check
make go-risk-run TRITON_HTTP_URL=http://127.0.0.1:8000
make scoring-topics
make go-risk-kafka-check
make go-risk-kafka TRITON_HTTP_URL=http://127.0.0.1:8000
```

For a bounded replay smoke test, add `GO_RISK_KAFKA_FLAGS="--from-beginning
--max-scores 1 --group-id poker-go-risk-smoke-<unique-id>"`.

The Triton model repository is generated at
`models/pair-catboost-full-v2/triton`. Mount that directory as Triton's model
repository before starting the Go service.

Endpoints:

- `GET /healthz`: process and loaded-model identity.
- `GET /readyz`: live Triton model readiness.
- `GET /metrics`: dependency-free Prometheus text metrics.
- `POST /v1/score-hand`: score exactly 15 pair events in one request.
- `POST /v1/pair-feature`: correction-aware incremental hand assembly.

Production deployments should pass `--allowed-tenants` (or
`RISK_ALLOWED_TENANTS` for the Kafka adapter). Tenant identity participates in
hand assembly, scoring validation, deterministic output, audit payloads, and
the allowlist check. The development default accepts every structurally valid
tenant.

Prometheus metrics include request/error/throughput counters, an in-flight
gauge, readiness failures, and an end-to-end request-duration histogram. The
dashboard and alert rules live under `ops/grafana` and `ops/prometheus`.
Optional Go profiling is disabled by default; enable it only with an explicit
loopback address such as `--pprof-listen 127.0.0.1:6060`. Non-loopback pprof
listeners are rejected.

The Kafka adapter uses the same `HandAssembler` and `Scorer` interfaces. It
consumes `poker.pair-features.v1`, publishes fired observations to
`poker.rule-evidence.v1`, a complete audit decision to `poker.risk-scores.v1`,
one review-routing audit record to `poker.review-decisions.v1`, and policy
alerts to `poker.risk-alerts.v1`. Evidence precedes its referencing score,
review decision, and alert in one synchronous publish call. Input offsets
become committable only after the whole output batch is acknowledged. Invalid
input is acknowledged only after a versioned dead-letter write. All output IDs
are deterministic, so replayed at-least-once output can be deduplicated
downstream.

The current pair-feature topic is keyed by `pair_key`, which can spread one
hand over multiple partitions. Run exactly one `risk-kafka` consumer replica
for this first deployment. Before horizontal scaling, add a by-`hand_id`
repartition topic (or change the upstream output key) so every 15-pair hand is
owned by one group member. The adapter blocks revocation while a partial hand
is resident rather than risking a partial commit.
