-- Active-player context lookup source for the Flink JDBC cache. This table is
-- intentionally not part of the Debezium publication: only hands use CDC in
-- the selected POC architecture.

CREATE TABLE IF NOT EXISTS public.poker_user_context (
    user_id TEXT NOT NULL,
    context_version INTEGER NOT NULL CHECK (context_version > 0),
    effective_at TIMESTAMPTZ NOT NULL,
    account_created_at TIMESTAMPTZ NOT NULL,
    country_bucket TEXT NOT NULL,
    timezone TEXT NOT NULL,
    acquisition_channel TEXT NOT NULL,
    kyc_level TEXT NOT NULL,
    account_status TEXT NOT NULL,
    bankroll_bucket TEXT NOT NULL,
    preferred_stake_bucket TEXT NOT NULL,
    skill_rating DOUBLE PRECISION NOT NULL CHECK (
        skill_rating >= 0.0 AND skill_rating <= 1.0
    ),
    device_id TEXT NOT NULL,
    network_cluster_id TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (user_id, context_version),
    UNIQUE (user_id, effective_at)
);

CREATE INDEX IF NOT EXISTS poker_user_context_effective_lookup
ON public.poker_user_context (user_id, effective_at DESC, context_version DESC);

COMMENT ON TABLE public.poker_user_context IS
    'Narrow synthetic user-context history queried lazily for active hand players';
