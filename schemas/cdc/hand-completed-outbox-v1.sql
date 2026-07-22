-- Proposed external PostgreSQL contract; documentation only, never applied by
-- this ML repository. The poker platform owns the production migration.
CREATE TABLE public.hand_completed_outbox (
    id                     UUID        PRIMARY KEY,
    aggregate_type         TEXT        NOT NULL CHECK (aggregate_type = 'poker-hand'),
    aggregate_id           TEXT        NOT NULL,
    event_type             TEXT        NOT NULL CHECK (event_type = 'poker.hand.completed'),
    payload_schema_version INTEGER     NOT NULL CHECK (payload_schema_version = 1),
    tenant_id              TEXT        NOT NULL,
    product_id             TEXT        NOT NULL,
    game_type              TEXT        NOT NULL,
    occurred_at            TIMESTAMPTZ NOT NULL,
    emitted_at             TIMESTAMPTZ NOT NULL CHECK (emitted_at >= occurred_at),
    codec_version          TEXT        NOT NULL,
    payload_sha256         CHAR(64)    NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload                BYTEA       NOT NULL,
    UNIQUE (tenant_id, product_id, aggregate_id, event_type)
);

-- Production must grant the poker-server role INSERT only. UPDATE and DELETE
-- are forbidden because the adapter rejects those CDC operations.
