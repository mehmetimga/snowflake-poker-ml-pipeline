# PostgreSQL and Debezium hand-history ingress

## Status and scope

The C2 contract, Go runtime, simulation-only Docker/SPCS packaging, and local
PostgreSQL/Debezium simulation are implemented and tested. A real local
PostgreSQL 17.5 logical-WAL source, Debezium 3.6 worker, Kafka broker, PokerKit
writer, Protobuf binary, and Go adapter completed a bounded end-to-end run.
No image was pushed, and no poker-server database, Confluent environment,
Snowflake object, or SPCS service was changed.

The repository now freezes the proposed boundary between a future external
poker platform and the existing canonical ML pipeline. It provides:

- a proposed immutable PostgreSQL outbox row;
- a database-owned game-type allowlist that filters before Kafka;
- a Debezium PostgreSQL envelope contract;
- Python and Go adapters that produce the same canonical hand identity;
- a versioned binary-decoder interface;
- source transaction, LSN, and Kafka-position lineage;
- direct-versus-CDC parity fixtures;
- a Go consume/publish/DLQ processor with commit-after-ack recovery;
- an isolated `poker.sim.*` SPCS service contract and immutable image; and
- a deterministic `poker-hand-protobuf-v1` simulation codec; and
- rejection tests for mutable, malformed, unknown-codec, label-bearing, and
  publish-failure records.

Real poker-server ingestion is outside the current project scope. The fixture
codec is an executable simulation contract, not a claim that the production
binary format has been decoded. The command accepts it only when simulation
mode, exact `poker.sim.*` topics, and a `sim-*` dataset are configured together.

## Production boundary

```text
COMPANY / POKER PLATFORM                         ML PLATFORM

Poker server
    |
    | one PostgreSQL transaction
    +--> binary hand_history row
    +--> immutable hand_completed_outbox row
                    |
                    v
        Debezium PostgreSQL connector
                    |
                    v
Confluent: cdc.poker.hand-outbox.v1  -------->  SPCS: poker-adapter:<git-sha>
restricted raw CDC topic                        Go adapter + codec registry
                                                        |
                                                        v
                                           poker.hands.raw.v1
                                                        |
                                                        v
                                           Java/Flink feature jobs
                                                        |
                                                        v
                                           Go scorer + Triton + Snowflake
```

PostgreSQL and Debezium run in the company source environment. The future Go
adapter runs as its own SPCS Docker image. The poker server remains outside
this repository and outside Snowflake. It does not dual-write to Kafka.

The current project still substitutes PokerKit plus the direct Kafka
publisher. Both paths end at exactly the same canonical topic and value
contract, so Flink and all later components do not branch on source type.

## Why an immutable outbox

The poker server must write its authoritative binary hand history and one
completion outbox row in the same database transaction. This provides one
durable commit decision. Debezium publishes only committed database changes,
avoiding the inconsistent states possible when the server separately writes
PostgreSQL and Kafka.

The proposed table is documented in
[`schemas/cdc/hand-completed-outbox-v1.sql`](../schemas/cdc/hand-completed-outbox-v1.sql).
It is reference DDL only; this repository never applies it to the poker
database. The poker-platform team owns the reviewed production migration.

Each outbox row is insert-only. Production privileges must deny `UPDATE` and
`DELETE`. The adapter also rejects those operations, so a permission or
application failure cannot silently rewrite canonical hand history.

## Frozen outbox row v1

| Field | Purpose | Rule |
|---|---|---|
| `id` | Unique source record | PostgreSQL UUID; becomes audit lineage |
| `aggregate_type` | Outbox aggregate family | Exactly `poker-hand` |
| `aggregate_id` | Completed hand identity | Must equal decoded `hand_id` |
| `event_type` | Domain event | Exactly `poker.hand.completed` |
| `payload_schema_version` | Canonical target family | Exactly `1` |
| `tenant_id` | Security and state boundary | Non-empty and adapter-allowlisted |
| `product_id` | Product boundary | Non-empty |
| `game_type` | Trusted routing metadata | Filtered in PostgreSQL and rechecked by the adapter |
| `occurred_at` | Semantic hand completion time | Must equal decoded `played_at` |
| `emitted_at` | Stable outbox creation time | Must be at or after `occurred_at` |
| `codec_version` | Binary decoder selection | Exact registered value; no guessing |
| `payload_sha256` | End-to-end byte integrity | Lowercase SHA-256 of original `BYTEA` |
| `payload` | Versioned hand-history bytes | PostgreSQL `BYTEA`; base64 in CDC JSON |

`dataset_id` and `dataset_split` are deployment-owned adapter configuration,
not poker-server fields. Live production uses a governed live dataset scope;
offline train/validation/test assignment is performed later by time and tenant,
not chosen by the source application.

## Debezium input contract

The static schema is
[`schemas/cdc/debezium.hand-completed-outbox.v1.schema.json`](../schemas/cdc/debezium.hand-completed-outbox.v1.schema.json),
with a shared example at
[`schemas/examples/debezium.hand-completed-outbox.v1.json`](../schemas/examples/debezium.hand-completed-outbox.v1.json).

The adapter accepts the ordinary Debezium envelope either directly or inside
the Kafka Connect JSON `schema`/`payload` wrapper. The fields used by the
contract are:

- `before` and `after` row images;
- `op` (`c` for create or `r` for snapshot read);
- `source.connector`, connector name, database, schema, and table;
- `source.txId`, `source.lsn`, `source.ts_ms`, and snapshot marker;
- connector processing `ts_ms`; and
- optional transaction `id`, total order, and collection order.

The recommended connector settings to validate in the real environment are:

```properties
connector.class=io.debezium.connector.postgresql.PostgresConnector
plugin.name=pgoutput
table.include.list=public.hand_completed_outbox
binary.handling.mode=base64
provide.transaction.metadata=true
```

The exact connector configuration, topic prefix, replication slot,
publication, secrets, snapshot policy, heartbeat, and retention values remain
environment-owned. See the official
[Debezium PostgreSQL connector documentation](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
It documents the change envelope, source LSN/transaction metadata, `BYTEA`
mapping, and binary handling modes.

This frozen adapter input is the raw PostgreSQL change envelope. It does not
currently apply Debezium's Outbox Event Router because the adapter needs the
original binary payload plus source LSN and transaction fields. The
[Outbox Event Router](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html)
remains a possible routing option only if an integration test proves that it
preserves all frozen payload and lineage fields. Its documented update/delete
behavior is consistent with an insert-only outbox, but it must not erase the
audit information required here.

## Deterministic mapping

For an accepted record, the adapter performs these steps in order:

1. Unwrap the Kafka Connect JSON wrapper if present.
2. Validate the Debezium envelope and configured database/schema/table.
3. Accept only create or snapshot-read operations.
4. Require a PostgreSQL transaction ID for a live create.
5. Strictly base64-decode `payload`.
6. Recompute and compare `payload_sha256` before decoding the format.
7. Select a decoder by exact `codec_version`.
8. Validate the decoded canonical hand and recursively reject unknown/private
   truth fields.
9. Require hand ID, dataset split, and semantic event time to match the source
   row and adapter configuration.
10. Build the existing canonical v1 event and headers.

The canonical mapping is:

| Canonical value | Source |
|---|---|
| `event_type` | Constant `poker.hand.completed` |
| `event_id` | UUIDv5 of dataset, split, event type, and decoded hand ID |
| `trace_id` | UUIDv5 of dataset, split, `trace`, and decoded hand ID |
| `tenant_id`, `product_id` | Validated outbox row |
| `dataset_id`, `dataset_split` | Adapter deployment configuration |
| `occurred_at` | Outbox `occurred_at`, equal to payload `played_at` |
| `emitted_at` | Stable outbox `emitted_at`, never connector wall-clock time |
| `payload` | Decoded and validated `HandCompletedPayload` v1 |
| Kafka topic | `poker.hands.raw.v1` |
| Kafka key | Decoded `table_id` |

The legacy canonical field name `generator` remains in v1 for compatibility.
Its allowed values are now `pokerkit` and `poker-server`. Renaming that field
would require a future canonical schema version rather than an in-place break.

## Binary codec boundary

Two repository-owned simulation codecs exist. `canonical-hand-json-v1` remains
the small mapping fixture. `poker-hand-protobuf-v1` is the deterministic binary
stored in the local PostgreSQL `BYTEA` column and decoded by both Python and
Go. Neither is a production poker-server decoder, and neither may be relabeled
as one.

The real integration requires the poker-platform owner to provide:

- the exact binary format and versioning rules;
- a trusted decoder or reference implementation;
- hand-completion semantics;
- checksum calculation over the pre-CDC bytes;
- backward-compatibility fixtures for every retained codec version; and
- malformed, truncated, and unsupported-version examples.

The Go and Python interfaces select a decoder by exact `codec_version`. An
unknown version is poison data and goes to a durable DLQ; there is no fallback
or heuristic decoding.

## Replay and audit lineage

Connector metadata is not inserted into the canonical JSON value because that
would break strict downstream contracts. It is copied into `cdc_*` Kafka
headers:

- connector, connector name, database, schema, and table;
- source LSN and PostgreSQL transaction ID;
- Debezium transaction ID and ordering fields;
- create/snapshot operation and snapshot marker;
- source and connector timestamps;
- outbox UUID and binary checksum; and
- source Kafka topic, partition, and offset.

The canonical warehouse sink now persists these headers as JSON in
`RAW_EVENT_ENVELOPES.source_lineage`, added by migration
[`012_cdc_source_lineage.sql`](../sql/migrations/012_cdc_source_lineage.sql).
The restricted raw CDC topic remains the authoritative replay trail; the
canonical audit row provides the direct link from its event ID back to that
trail.

A snapshot or retry of the same outbox row produces the same canonical event
and trace IDs. Its source operation/offset can differ, but downstream
idempotency remains based on the stable canonical event ID.

## Operation and failure policy

| Input | Result |
|---|---|
| `op=c`, valid row, transaction lineage present | Publish canonical hand |
| `op=r`, valid snapshot marker | Publish same deterministic canonical hand |
| `op=u` | Reject: completed outbox is immutable |
| `op=d` | Reject: completed outbox is immutable |
| Kafka tombstone | Reject; never interpreted as a hand |
| Unknown codec | Reject; never guess |
| Checksum mismatch | Reject before codec execution |
| Source table/database mismatch | Reject |
| Tenant not allowlisted | Reject |
| Hand ID, split, or time mismatch | Reject |
| Private label/truth field | Reject |

The Go runtime publishes a rejection record to an ACL-protected durable DLQ
with source position, error code, immutable build version, and SHA-256 hashes,
then commits the source offset only after either the canonical output or DLQ
output is acknowledged. The versioned DLQ schema is
[`poker.cdc-hand.dead-lettered.v1.schema.json`](../schemas/cdc/poker.cdc-hand.dead-lettered.v1.schema.json).
It deliberately has no raw key, raw value, decoded hand, or error-detail field.
Logs alone do not satisfy the durability requirement, and proprietary binary
bytes cannot leak through this DLQ contract.

Publishing and committing are intentionally at-least-once rather than an
unsafe claim of atomic exactly-once delivery. If output succeeds and the
offset commit fails, Kafka replays the input and the adapter republishes a
byte-identical deterministic event. Downstream event-ID idempotency handles
that retry.

The runtime is implemented in
[`services/go/internal/cdc/processor.go`](../services/go/internal/cdc/processor.go),
the Kafka polling/headers bridge in
[`services/go/internal/kafkaio/adapter.go`](../services/go/internal/kafkaio/adapter.go),
and the fail-closed command in
[`services/go/cmd/hand-adapter`](../services/go/cmd/hand-adapter).

## Verification

Run the complete offline contract, runtime, and packaging gate:

```bash
make phase-c2-packaging-check
make phase-c2-cdc-simulation-check
```

The deterministic fixture report should include:

```text
canonical_equivalent: true
event_id: f00d27af-a72b-58bd-8180-14d6e38d3040
trace_id: e6dae691-09f7-523b-aece-0fa0a67d3609
source_lsn: 270113177
source_tx_id: 9001
target_topic: poker.hands.raw.v1
partition_key: c2_table_01
```

The gate covers Python, Go, source and DLQ schemas, base64/checksum integrity,
direct-versus-CDC equality, UUID parity, replay/snapshot identity, pluggable
binary decoding, mutation/delete/tombstone handling, source drift, private
truth rejection, Snowflake/DuckDB audit-lineage persistence, Kafka header
transport, sanitized deterministic DLQ output, publish failure, commit failure,
replay, and metrics.

Kafka authentication can be checked without enabling a decoder or consuming:

```bash
make go-hand-adapter-kafka-check
```

The currently runnable decoders are repository-owned simulation codecs. A
bounded simulation-topic run must opt in visibly and remains isolated:

```bash
make go-hand-adapter-sim GO_HAND_ADAPTER_FLAGS="--from-beginning --max-records 1"
```

The complete local PostgreSQL/Debezium path is one bounded command:

```bash
make cdc-sim-e2e
```

Its architecture, filter/parse boundary, runbook, and production transition
are documented in
[`postgres-debezium-simulation.md`](postgres-debezium-simulation.md).

Package verification and image commands are documented in
[`spcs-c2-adapter-simulation.md`](spcs-c2-adapter-simulation.md).

## Remaining remote simulation gates

C2 is proven locally. Remote shadow simulation is not complete until:

1. Create the three isolated `poker.sim.*` Confluent topics.
2. Give the adapter a least-privilege simulation service account.
3. Release the clean-commit image and deploy `POKER_ADAPTER_SIM` privately.
4. Repeat canonical/DLQ, lineage, replay, lag, and commit-recovery checks.
5. Configure a separate simulation Flink input before exercising downstream
   scoring; never redirect the deployed production-shaped Flink service.

The real external poker-server schema, binary hand history, and production CDC
topics remain explicitly deferred. The local simulation is active only on the
isolated `poker.sim.*` path.
