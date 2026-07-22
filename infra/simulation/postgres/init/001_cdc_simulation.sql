-- Local simulation source. This is intentionally separate from Snowflake and
-- from any future company poker-server database.

CREATE TABLE public.ml_cdc_game_type_allowlist (
    game_type TEXT PRIMARY KEY CHECK (game_type ~ '^[A-Z0-9_]+$'),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

INSERT INTO public.ml_cdc_game_type_allowlist (game_type) VALUES
    ('NLH_CASH_6MAX'),
    ('NLH_TOURNAMENT_6MAX');

CREATE TABLE public.hand_history (
    id UUID PRIMARY KEY,
    outbox_id UUID NOT NULL UNIQUE,
    simulation_dataset_id TEXT NOT NULL CHECK (simulation_dataset_id LIKE 'sim-%'),
    hand_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    game_type TEXT NOT NULL CHECK (game_type ~ '^[A-Z0-9_]+$'),
    payload_schema_version INTEGER NOT NULL CHECK (payload_schema_version = 1),
    codec_version TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload BYTEA NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    emitted_at TIMESTAMPTZ NOT NULL CHECK (emitted_at >= occurred_at),
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (tenant_id, product_id, hand_id)
);

CREATE TABLE public.hand_completed_outbox (
    id UUID PRIMARY KEY,
    aggregate_type TEXT NOT NULL CHECK (aggregate_type = 'poker-hand'),
    aggregate_id TEXT NOT NULL,
    event_type TEXT NOT NULL CHECK (event_type = 'poker.hand.completed'),
    payload_schema_version INTEGER NOT NULL CHECK (payload_schema_version = 1),
    tenant_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    game_type TEXT NOT NULL CHECK (game_type ~ '^[A-Z0-9_]+$'),
    occurred_at TIMESTAMPTZ NOT NULL,
    emitted_at TIMESTAMPTZ NOT NULL CHECK (emitted_at >= occurred_at),
    codec_version TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    payload BYTEA NOT NULL,
    UNIQUE (tenant_id, product_id, aggregate_id, event_type)
);

CREATE FUNCTION public.route_completed_hand_to_ml_outbox()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.ml_cdc_game_type_allowlist allowed
        WHERE allowed.game_type = NEW.game_type
          AND allowed.enabled
    ) THEN
        INSERT INTO public.hand_completed_outbox (
            id,
            aggregate_type,
            aggregate_id,
            event_type,
            payload_schema_version,
            tenant_id,
            product_id,
            game_type,
            occurred_at,
            emitted_at,
            codec_version,
            payload_sha256,
            payload
        ) VALUES (
            NEW.outbox_id,
            'poker-hand',
            NEW.hand_id,
            'poker.hand.completed',
            NEW.payload_schema_version,
            NEW.tenant_id,
            NEW.product_id,
            NEW.game_type,
            NEW.occurred_at,
            NEW.emitted_at,
            NEW.codec_version,
            NEW.payload_sha256,
            NEW.payload
        );
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER hand_history_to_ml_outbox
AFTER INSERT ON public.hand_history
FOR EACH ROW
EXECUTE FUNCTION public.route_completed_hand_to_ml_outbox();

CREATE FUNCTION public.reject_outbox_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'hand_completed_outbox is insert-only';
END;
$$;

CREATE TRIGGER hand_completed_outbox_is_immutable
BEFORE UPDATE OR DELETE ON public.hand_completed_outbox
FOR EACH ROW
EXECUTE FUNCTION public.reject_outbox_mutation();

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION PASSWORD 'debezium';
    END IF;
END;
$$;

GRANT CONNECT ON DATABASE poker_sim TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON public.hand_completed_outbox TO debezium;

CREATE PUBLICATION poker_sim_hand_outbox_pub
FOR TABLE public.hand_completed_outbox
WITH (publish = 'insert');
