# Local PostgreSQL and Debezium CDC simulation

## What is implemented

This is the executable local substitute for a future poker-server integration:

```text
host Python                         local Docker
PokerKit hand generator
        |
        | INSERT one opaque BYTEA source row
        v
PostgreSQL hand_history
        |
        | AFTER INSERT trigger checks game_type allowlist
        +---- excluded game type ---> stays only in hand_history
        |
        +---- allowed game type ----> hand_completed_outbox
                                           |
                                           | logical WAL / pgoutput
                                           v
                                      Debezium Connect
                                           |
                                           | base64 BYTEA + CDC lineage
                                           v
                                  poker.sim.cdc-hand-outbox.v1
                                           |
                                           | host Go adapter in local test;
                                           | same image is SPCS-ready
                                           v
                                     Protobuf v1 decoder
                                           |
                       +-------------------+-------------------+
                       v                                       v
             poker.sim.hands.raw.v1          poker.sim.pipeline.dead-letter.v1
```

PostgreSQL 17.5, Kafka 3.8, and Debezium 3.6 run as local Docker
containers. The PokerKit writer and Go adapter run as host processes during
`make cdc-sim-e2e`. This phase does not push an image, deploy an SPCS service,
write to Confluent, or change Snowflake.

## Where filtering and parsing happen

Filtering happens **before Kafka**, but binary parsing happens **after Kafka**.
These are separate concerns:

1. The source row exposes trusted routing metadata in ordinary columns:
   `tenant_id`, `product_id`, and `game_type`.
2. A PostgreSQL trigger reads only `game_type`. If the value is enabled in
   `ml_cdc_game_type_allowlist`, it copies the opaque bytes and checksum into
   the insert-only outbox.
3. Debezium reads only `hand_completed_outbox`. It does not understand poker
   hands and emits PostgreSQL `BYTEA` as base64.
4. The Go adapter rechecks the game-type allowlist, verifies SHA-256, selects
   the exact `codec_version`, decodes the bytes, and validates the canonical
   hand before publishing it.

This keeps binary-format changes out of PostgreSQL triggers and Kafka Connect,
while keeping unwanted game families off Kafka. In production, the poker
server must write a normalized, trustworthy `game_type` column in the same
transaction as the binary hand. If the existing database does not expose that
metadata, the poker-platform team should add an outbox projection; Debezium's
scripting Filter SMT is deliberately not used here.

The database is the first filter, not the only filter. The Go adapter repeats
the allowlist check because an accidental publication/configuration change
must fail closed.

## Simulation binary format

The local binary is `poker-hand-protobuf-v1`, defined in
[`poker_hand_history_v1.proto`](../schemas/proto/poker_hand_history_v1.proto).
It is a real deterministic binary payload stored in PostgreSQL `BYTEA`, but it
is not a guess at the company's hand-history format.

Important rules:

- monetary values are signed integer millionths, never floating-point wire
  values;
- timestamps use UTC Unix milliseconds;
- `game_type` appears both in the row and binary and must match;
- private simulator truth such as `is_suspicious` and collusion labels is
  removed before serialization;
- Python and Go decode the same checked-in base64 fixture; and
- `payload_sha256` covers the exact bytes before Debezium encodes them.

When the real format is available, add a new decoder under its real immutable
codec name and retain old decoders for replay. Do not rename the simulation
codec or silently change its schema.

## Tables and transaction boundary

The local schema is
[`001_cdc_simulation.sql`](../infra/simulation/postgres/init/001_cdc_simulation.sql).

| Object | Responsibility |
|---|---|
| `hand_history` | All simulated games and their opaque hand bytes |
| `ml_cdc_game_type_allowlist` | Explicit game families eligible for ML CDC |
| `hand_completed_outbox` | Immutable eligible completion records only |
| `hand_history_to_ml_outbox` | Transactional routing trigger |
| `poker_sim_hand_outbox_pub` | Insert-only logical replication publication |

The writer inserts only `hand_history`; it never writes Kafka or the outbox.
The trigger executes inside the same PostgreSQL transaction, so a committed
source hand and its eligible outbox row cannot disagree.

`simulation_dataset_id` exists only on the local source table. It makes smoke
tests independently verifiable without deleting old data. It is not copied to
the production-shaped outbox because canonical `dataset_id` remains adapter
deployment configuration.

## Connector contract

The checked-in connector is
[`poker-hand-outbox-connector.json`](../infra/simulation/debezium/poker-hand-outbox-connector.json).
It uses:

- `pgoutput` and a manually created insert-only publication;
- one exact `table.include.list` entry;
- `binary.handling.mode=base64`;
- transaction metadata and PostgreSQL LSN/transaction lineage;
- schemaless JSON for the frozen adapter envelope; and
- a predicate-scoped route to `poker.sim.cdc-hand-outbox.v1`.

Debezium documents the PostgreSQL connector and binary handling in its
[PostgreSQL connector reference](https://debezium.io/documentation/reference/stable/connectors/postgresql.html).
PostgreSQL's publication behavior is described in its
[logical replication publication documentation](https://www.postgresql.org/docs/current/logical-replication-publication.html).
The unused scripting alternative is documented as the
[Debezium Filter SMT](https://debezium.io/documentation/reference/stable/transformations/filtering.html).

The checked-in passwords are local simulation credentials only. Production
uses a secret manager, TLS, scoped database grants, Kafka ACLs, reviewed
replication-slot retention, and monitored WAL growth.

## Run it

Install the pinned Python dependencies once:

```bash
make install
```

Validate files without starting anything:

```bash
make cdc-sim-config-check
make phase-c2-cdc-simulation-check
```

Run the complete bounded smoke test:

```bash
make cdc-sim-e2e
```

That command:

1. starts Kafka, PostgreSQL, and Debezium;
2. creates all three simulation topics explicitly;
3. idempotently registers the connector;
4. starts a new Go consumer at the current end of the CDC topic;
5. writes eight deterministic PokerKit hands over four game types;
6. waits until the four eligible records are acknowledged and committed; and
7. checks database routing, both Kafka topics, canonical payloads, lineage
   headers, Kafka keys, unique event IDs, and an empty DLQ.

Expected acceptance summary:

```text
PostgreSQL source rows:       8
PostgreSQL outbox rows:       4
Filtered before Kafka:        4
CDC records for the run:      4
Canonical records for run:    4
Dead letters:                 0
```

Check status without changing the connector:

```bash
make cdc-sim-status
```

Stop PostgreSQL and Debezium while retaining their data volume:

```bash
make cdc-sim-stop
```

The stop target intentionally leaves Kafka alone because other local project
flows share it. It also does not delete the PostgreSQL volume.

## Fault and recovery suite

Run the deterministic poison and commit-recovery proof with:

```bash
make cdc-sim-fault-replay-e2e
```

The fault manifest writes six source hands:

| Scenario | Expected result |
|---|---|
| `valid_cash` | One canonical event |
| `filtered_play_money` | Retained in `hand_history`; absent from outbox/Kafka |
| `checksum_mismatch` | Sanitized DLQ with `checksum_mismatch` |
| `malformed_protobuf` | Sanitized DLQ with `invalid_binary_payload` |
| `game_type_mismatch` | Sanitized DLQ with `game_type_mismatch` |
| `unknown_codec_version` | Sanitized DLQ with `unknown_codec_version` |

The verifier joins DLQs back to their source Kafka partition/offset, checks
the exact error code and deterministic event bytes, and confirms that neither
the raw CDC value nor hand identity appears in the DLQ.

Commit recovery uses a simulation-only flag that production mode rejects. A
baseline record first establishes the consumer group's committed position.
The test then:

1. publishes one valid recovery hand;
2. acknowledges its canonical output;
3. injects a failure before the source offset commit;
4. proves the committed offset did not advance;
5. restarts the same group without injection;
6. proves the source record is replayed and committed; and
7. requires both canonical records to have identical key, value, headers, and
   one stable event ID.

All bounded live targets wait for a stable Kafka group assignment before
inserting PostgreSQL rows. This removes the race where a new `latest` consumer
could establish its starting offset after the test records arrived.

## Production transition

The local topology proves the ownership and failure boundaries, not the real
poker format. Production integration still requires:

1. reviewed poker-server source and outbox DDL;
2. the real binary specification and cross-language golden fixtures;
3. a production decoder registered under a new codec version;
4. secret-managed PostgreSQL and Confluent connectivity;
5. topic ACLs and retention/replication settings;
6. WAL, replication-slot, connector-lag, DLQ, and adapter-lag monitoring; and
7. a shadow-only Confluent/SPCS replay before downstream Flink consumes it.

The downstream contract remains unchanged: Flink reads canonical
`poker.hands.raw.v1`-shaped events and never parses PostgreSQL bytes.
