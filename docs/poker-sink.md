# POKER_SINK: Kafka-to-Snowflake persistence

Status: implemented and locally verified; Snowflake/SPCS deployment and D7
reconciliation are pending

Last reviewed: 2026-07-23

## Purpose

`POKER_SINK` is the durable boundary between the canonical real-time topics and
Snowflake. It consumes all eight `poker.synthetic.*` topics used by the current
acceptance environment:

- completed hands;
- hand-player context v2;
- pair features from context v2;
- risk scores;
- rule evidence;
- review decisions;
- risk alerts; and
- the shared pipeline dead-letter topic.

It is not a simulator. The synthetic prefix isolates the current POC data from
future production topics.

## Runtime shape

One SPCS service contains two containers built from the same immutable image:

```text
Confluent Kafka
      |
      v
Go sink-kafka container
  validate envelope/topic/schema/tenant
  compute raw-record hashes
      |
      | private HTTP on 127.0.0.1:8091
      v
Python Snowflake writer sidecar
  SPCS OAuth token: /snowflake/session/token
  envelope row + event-native row in one transaction
      |
      v
Snowflake POKER_ML_DEMO.SPCS
      |
      v
Go commits Kafka offset only after inserted/duplicate acknowledgement
```

Go owns Kafka polling, validation, retry termination, metrics, and offset
commits. Python is deliberately limited to the Snowflake connector and
transaction. The writer container receives no Kafka secret. The service has
one Kafka external-access integration, one private metrics endpoint, and no
public data endpoint.

## Delivery and idempotency

The transport guarantee is at least once. The durable behavior is effectively
once per immutable event ID:

1. Go validates the topic-specific event type and schema version.
2. Go sends the event ID, compact-JSON event SHA-256, raw Kafka-record SHA-256,
   Kafka coordinates, and event JSON to the writer. Removing insignificant
   whitespace prevents formatting-only replays from becoming false immutable
   collisions; the raw hash preserves byte-level lineage.
3. The writer checks `POKER_EVENT_ENVELOPES`.
4. A missing ID inserts the ledger row and matching typed row in one
   transaction.
5. The same ID and hash returns `duplicate`.
6. The same ID with a different hash returns HTTP 409 and is never committed.
7. Go commits the source offset only after `inserted` or `duplicate`.

The service is fixed at one SPCS instance for the POC. The writer also
serializes its Snowflake session so the pre-check and insert cannot race inside
the service.

| Failure | Snowflake result | Kafka offset |
|---|---|---|
| Valid new event | Two rows committed atomically | Committed |
| Replay of identical event | No new row; duplicate acknowledged | Committed |
| Event-ID/hash collision | No write | Not committed |
| Writer unavailable | No acknowledged write | Not committed |
| Typed insert fails after ledger insert | Transaction rolls back | Not committed |
| Invalid/schema-mismatched event | Sanitized dead-letter audit row | Committed |
| Dead-letter insert fails | No acknowledged write | Not committed |

Poison records never copy their raw value into Snowflake. The audit table keeps
only the topic, partition, offset, timestamp, categorical error code, hashes,
and sink build version.

## Snowflake objects

[`sink.sql`](../infra/snowflake/sql/sink.sql) creates:

- `POKER_EVENT_ENVELOPES`, the immutable lineage ledger;
- one table for each valid event kind;
- `POKER_SINK_DEAD_LETTERS`;
- `POKER_ALERT_REVIEW_V`, the canonical admin read model; and
- `POKER_SINK_TOPIC_PROGRESS_V`, persisted topic/partition progress.

The event-specific tables project hand, table, entity, revision, timestamps,
and lineage while retaining the complete payload as `VARIANT`.

## Admin migration

SPCS `POKER_ADMIN` sets `ADMIN_DATA_MODE=canonical` and reads
`POKER_ALERT_REVIEW_V`. The old `ALERTS` query remains available only with
`ADMIN_DATA_MODE=legacy`; this is a rollback bridge, not the target path.

## Local verification and deployment

```bash
make r5-sink-test
make r5-sink-build
make r5-sink-image-smoke
make r5-admin-build
make r5-admin-image-smoke

# After committing the verified source so the tag equals the Git SHA:
make r5-sink-release-check
make r5-sink-push
make r5-admin-push
make r5-sink-render
make r5-sink-bootstrap
make r5-sink-deploy
make r5-admin-deploy
```

After the service catches up on the existing D7 topics:

```bash
make r5-sink-verify \
  ALERT_ACCEPTANCE_SPCS_MANIFEST=/private/tmp/poker-alert-acceptance-d7-spcs/replay-manifest.json \
  ALERT_ACCEPTANCE_SPCS_REPORT=/private/tmp/poker-alert-acceptance-d7-spcs/verification-report.json \
  ALERT_ACCEPTANCE_SINK_REPORT=/private/tmp/poker-alert-acceptance-d7-spcs/sink-verification-report.json
```

The last command requires exact counts for 16 hands, 96 contexts, 240 pair
features, 176 evidence events, 16 scores, 16 decisions, and 14 alerts. It also
requires the exact sealed alert IDs in the admin view and verifies that the
sink consumer group committed beyond every persisted source offset.
