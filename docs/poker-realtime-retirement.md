# POKER_REALTIME retirement runbook

Status: R6 tooling implemented locally; live preflight, bounded replay, and
24-hour suspension evidence are pending

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
  target DLQ result, commits, lag, and latency observations.

Every live command refuses to overwrite an existing report. Use a new
`R6_RUN_DIR` for a new attempt so failed and accepted evidence cannot be
silently replaced.

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

Suspend only the exact service:

```sql
ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_REALTIME SUSPEND;
```

During the window, verify:

- `POKER_REALTIME` remains suspended;
- `POKER_SINK` and `POKER_ADMIN` remain running and ready;
- canonical admin queries continue to return current data;
- sink lag remains within the agreed POC bound;
- target DLQ growth is zero; and
- no active dependency appears on `hands.raw` or `alerts.out`.

No deletion occurs at the end of 24 hours.

## Rollback drill

Resume the exact retained object:

```sql
ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_REALTIME RESUME;
```

The rollback passes only when:

- the original spec digest is unchanged;
- the original image digest is running;
- the container is `READY`;
- the `realtime-processor` group returns with its prior committed offsets; and
- no canonical service was modified to perform the rollback.

After the drill, the service may be suspended again for cost control only under
an explicit operational decision.

## Final retirement boundary

Dropping `POKER_REALTIME` is a separate destructive action. It requires:

- accepted preflight, parity, 24-hour observation, and rollback reports;
- confirmation that no active producer or consumer depends on the legacy
  topics;
- an agreed retention location for the spec, image, offsets, and reports; and
- explicit approval naming `POKER_ML_DEMO.SPCS.POKER_REALTIME`.

Legacy Kafka topics and Snowflake tables have independent retention policies
and are never deleted as a side effect of dropping the service.
