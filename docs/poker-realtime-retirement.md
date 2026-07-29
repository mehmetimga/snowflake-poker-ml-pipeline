# POKER_REALTIME retirement runbook

Status: the 120-hour suspension recovery and exact rollback drill passed;
`POKER_REALTIME` is retained and running pending an explicit retention/drop
decision

Last reviewed: 2026-07-24

## Purpose

R6 proves that the canonical data plane has replaced the legacy
`POKER_REALTIME` service before that service is retired. The phase is
deliberately staged:

1. capture a read-only dependency and rollback baseline;
2. replay the already accepted, training-excluded D7 hands through the legacy
   topic while the canonical D7 result remains frozen;
3. verify bounded replacement coverage and zero lag;
4. suspend `POKER_REALTIME` for 24 hours;
5. verify canonical freshness throughout the observation window;
6. resume the exact legacy service as a rollback drill; and
7. request separate explicit approval before dropping anything.

Starting R6 does not authorize dropping the service, deleting Kafka topics,
deleting legacy tables, or removing rollback code.

## What parity means here

The legacy and canonical model contracts are not numerically interchangeable:

| Dimension | Legacy `POKER_REALTIME` | Canonical path | R6 gate |
|---|---|---|---|
| Input | Plain completed-hand payload on `hands.raw` | Same payload inside a versioned event envelope | Exact hand/player/action identity |
| Features | Six per-player `FEATURES` rows per six-player hand | Fifteen pair vectors plus resolved context | Complete expected coverage; do not compare unlike vectors |
| Score | Older per-player ensemble; only thresholded rows persist | Complete deterministic per-hand CatBoost score | Record both distributions; no false numeric-equality assertion |
| Policy | Threshold embedded in the monolith | Versioned decision and evidence events | Canonical D7 decision/evidence acceptance must pass |
| Alert ID | Random legacy ID | Deterministic event ID | Independently validate each contract |
| Persistence | Mutable legacy `PUBLIC` tables | Event-native immutable `SPCS` tables | Exact bounded counts and lineage |

R6 is therefore a replacement-contract proof. Exact input identity, complete
processing coverage, canonical acceptance, dependency isolation, DLQ status,
and lag are hard gates. Comparing values from different feature/model contracts
is evidence for human review, not an equality gate.

## Evidence chain

The default evidence directory is:

```text
/private/tmp/poker-r6-realtime-retirement/
```

It contains:

- `preflight-report.json`: live service dependencies, Kafka groups, offsets,
  exact rollback spec, image digest, and suspend/resume commands;
- `legacy-replay-report.json`: exact D7 hand IDs and acknowledged legacy Kafka
  offsets; and
- `parity-report.json`: raw input identity, row coverage, output comparison,
  target DLQ result, commits, lag, and latency observations;
- `suspension-start-report.json`: hash-bound start/minimum-end timestamps,
  before/after health, legacy dependencies, and rollback contract;
- timestamped `suspension-check-*.json` files: immutable health observations;
  and
- `rollback-report.json`: restored service identity, consumer membership,
  committed offsets, and lag.

Every live command refuses to overwrite an existing report. Use a new
`R6_RUN_DIR` for a new attempt so failed and accepted evidence cannot be
silently replaced.

The original July 24 evidence was written under `/private/tmp` and was purged
before the final check. Never use an operating-system temporary directory for
retained evidence. The recovery workflow writes to:

```text
evidence/r6-realtime-retirement-20260724/
```

It explicitly records that the deleted files and their byte hashes cannot be
reconstructed. It accepts the observation only from Snowflake's unchanged
service lifecycle timestamps/spec plus current Snowflake and Kafka health; it
does not fabricate replacement preflight, parity, or start reports.

## Local gate

```bash
make phase-r6-check
```

This runs the pure R6 contract tests and direct entrypoint smoke checks. No
Kafka record, Snowflake row, or service state changes.

## Live preflight

Run only from a clean, pushed commit:

```bash
make r6-preflight
```

The command is read-only. It fails unless:

- `POKER_REALTIME` is `RUNNING/READY`;
- only `POKER_REALTIME` references exact topics `hands.raw` or `alerts.out`;
- `realtime-processor` is the only active consumer group on `hands.raw`;
- no active group consumes `alerts.out`;
- no local generator, realtime processor, or Flink process is connected to the
  legacy path; and
- the exact service spec, spec digest, image digest, offsets, and rollback
  commands can be recorded.

Inactive historical Kafka groups are retained as evidence but do not block the
gate.

## Bounded dual-run

After preflight passes:

```bash
make r6-legacy-replay
make r6-parity-verify
```

Or run the ordered wrapper:

```bash
make r6-bounded-e2e
```

The replay publishes only the 16 sealed D7 hand payloads to `hands.raw`.
Canonical topics are not republished or changed. The legacy processor is
idempotent by hand ID and must commit every published offset only after its
Snowflake writes finish.

The verifier requires:

- 16 exact `RAW_HANDS` projections;
- 96 exact `RAW_PLAYERS` projections;
- the exact source action count and values;
- 96 unique `FEATURES` keys;
- 96 unique `RULE_FLAGS` keys;
- bounded, valid legacy alerts only for target hands;
- the previously accepted canonical counts of 16 hands, 96 contexts, 240 pair
  features, 16 scores, 176 evidence rows, 16 decisions, and 14 alerts;
- the exact accepted canonical admin IDs;
- zero target D7 dead letters;
- the required legacy consumer commits; and
- zero legacy and canonical sink lag.

Simulated event time is intentionally not used as wall-clock latency because
D7 uses a frozen synthetic clock. Canonical Kafka-to-Snowflake transport
latency is measured from Kafka record timestamp to sink ingestion time. Legacy
visibility is reported as a conservative replay-to-verification upper bound.

## Controlled 24-hour suspension

Do not begin the observation window unless the preflight and parity reports
both pass. The start record must include:

- start and minimum end timestamps;
- the accepted preflight/parity report hashes;
- the exact rollback spec and image digest;
- canonical admin row count and newest ingestion timestamp;
- sink group offsets and zero-lag baseline; and
- the current statuses of `POKER_SINK`, `POKER_ADMIN`, and `POKER_REALTIME`.

Start the controller only from a clean, pushed commit:

```bash
make r6-suspension-start
```

It validates the accepted evidence chain and live identities, suspends only
`POKER_ML_DEMO.SPCS.POKER_REALTIME`, waits for the service and its Kafka
consumer to become inactive, and checks the canonical path immediately. If an
immediate post-suspension gate fails, it automatically resumes the retained
service and waits for it to become ready and caught up.

During the window, verify:

- `POKER_REALTIME` remains suspended;
- `POKER_SINK` and `POKER_ADMIN` remain running and ready;
- canonical admin queries continue to return current data;
- sink lag remains within the agreed POC bound;
- target DLQ growth is zero; and
- no active dependency appears on `hands.raw` or `alerts.out`.

Create an immutable observation at any time with:

```bash
make r6-suspension-check
```

The default filename includes the current UTC timestamp so repeated checks do
not overwrite prior evidence. A passed check reports
`observation_in_progress` before the minimum end timestamp and
`observation_window_complete` after it.

No deletion occurs at the end of 24 hours.

## Rollback drill

After 24 hours, seal the final health observation and then resume the exact
retained object:

```bash
make r6-suspension-final-check
make r6-rollback
```

The controller blocks this drill before the minimum end timestamp and requires
the hash-bound final check to have status `observation_window_complete`. An
emergency early rollback is intentionally not exposed through the Make target;
an operator may run the controller's `rollback --allow-early` command when
service recovery is required.

The rollback passes only when:

- the original spec digest is unchanged;
- the original image digest is running;
- the container is `READY`;
- the `realtime-processor` group returns with its prior committed offsets; and
- no canonical service was modified to perform the rollback.

After the drill, the service may be suspended again for cost control only under
an explicit operational decision.

### Recovery workflow for the purged July 24 reports

Run from a clean, pushed controller commit:

```bash
make r6-observation-recover
make r6-recovery-rollback
```

The first command is read-only against Snowflake and Kafka. It requires:

- `POKER_REALTIME` to remain suspended with its original spec digest;
- Snowflake `suspended_on` to be at least 24 hours old, later than
  `resumed_on`, and equal to the last service update;
- `POKER_SINK` and `POKER_ADMIN` to be ready;
- the 14 accepted D7 admin rows and unchanged dead-letter total;
- zero canonical sink lag; and
- no active legacy dependency, with `hands.raw[0]` still at committed/end
  offset 99.

The second command resumes the retained service and passes only when its
original image/spec are ready, `realtime-processor` is active and caught up
from offset 99, and canonical admin/DLQ/lag health remains unchanged.

## Accepted live evidence

On 2026-07-24:

- R6 preflight commit `ef012e8257dc` passed with `POKER_REALTIME` as the only
  service using `hands.raw`/`alerts.out` and `realtime-processor` as the only
  active legacy consumer;
- exactly 16 sealed D7 hands were published to `hands.raw` offsets 83 through
  98 and committed through offset 99;
- bounded legacy persistence passed with 16 hands, 96 players, 247 actions, 96
  features, and 96 rule rows;
- the canonical accepted counts remained unchanged at 16 hands, 96 contexts,
  240 pair features, 16 scores, 176 evidence rows, 16 decisions, and 14 alerts;
- target D7 dead letters remained zero and both the legacy and canonical sink
  lag were zero; and
- the single legacy thresholded alert and 14 canonical policy alerts were
  recorded as different output contracts, not compared as numerically equal
  scores.

On 2026-07-29, Snowflake still reported `POKER_REALTIME` suspended with the
unchanged July 24 suspension timestamp and original spec digest. Live
post-window inspection also found both canonical services ready, 14 D7 admin
rows, the dead-letter total unchanged at 139, zero lag on every canonical
partition, no active legacy dependency, and legacy committed/end offset 99.
The durable recovery report passed with 120.4 observed suspension hours and no
blockers. The rollback then restored the exact original image digest
`sha256:79d875...fd3a` and spec digest `2941d733...249d`; the container became
ready with zero restarts, `realtime-processor` became stable at committed/end
offset 99 with zero lag, and canonical admin, dead-letter, and sink-lag health
remained unchanged.

## Final retirement boundary

Dropping `POKER_REALTIME` is a separate destructive action. It requires:

- accepted preflight, parity, 24-hour observation, and rollback reports;
- confirmation that no active producer or consumer depends on the legacy
  topics;
- an agreed retention location for the spec, image, offsets, and reports; and
- explicit approval naming `POKER_ML_DEMO.SPCS.POKER_REALTIME`.

Legacy Kafka topics and Snowflake tables have independent retention policies
and are never deleted as a side effect of dropping the service.
