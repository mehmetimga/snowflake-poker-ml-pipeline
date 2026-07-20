CREATE TABLE IF NOT EXISTS RAW_EVENT_ENVELOPES (
    event_id          STRING       NOT NULL,
    event_type        STRING       NOT NULL,
    schema_version    INT          NOT NULL,
    tenant_id         STRING       NOT NULL,
    product_id        STRING       NOT NULL,
    dataset_id        STRING       NOT NULL,
    dataset_split     STRING       NOT NULL,
    occurred_at       TIMESTAMP_TZ NOT NULL,
    emitted_at        TIMESTAMP_TZ NOT NULL,
    trace_id          STRING       NOT NULL,
    kafka_topic       STRING,
    kafka_partition   INT,
    kafka_offset      INT,
    kafka_timestamp_ms BIGINT,
    payload           VARIANT      NOT NULL,
    ingested_at       TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS USER_CONTEXT_EVENTS (
    event_id               STRING       NOT NULL,
    dataset_id             STRING       NOT NULL,
    dataset_split          STRING       NOT NULL,
    user_id                STRING       NOT NULL,
    context_version        INT          NOT NULL,
    effective_at           TIMESTAMP_TZ NOT NULL,
    account_created_at     TIMESTAMP_TZ NOT NULL,
    country_bucket         STRING       NOT NULL,
    timezone_name          STRING       NOT NULL,
    acquisition_channel    STRING       NOT NULL,
    kyc_level              STRING       NOT NULL,
    account_status         STRING       NOT NULL,
    bankroll_bucket        STRING       NOT NULL,
    preferred_stake_bucket STRING       NOT NULL,
    skill_rating           FLOAT        NOT NULL,
    device_id              STRING       NOT NULL,
    network_cluster_id     STRING       NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS USER_SESSION_EVENTS (
    event_id           STRING       NOT NULL,
    dataset_id         STRING       NOT NULL,
    dataset_split      STRING       NOT NULL,
    session_id         STRING       NOT NULL,
    user_id            STRING       NOT NULL,
    device_id          STRING       NOT NULL,
    network_cluster_id STRING       NOT NULL,
    started_at         TIMESTAMP_TZ NOT NULL,
    status             STRING       NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS ACCOUNT_LINK_EVENTS (
    event_id          STRING       NOT NULL,
    dataset_id        STRING       NOT NULL,
    dataset_split     STRING       NOT NULL,
    link_id           STRING       NOT NULL,
    user_id           STRING       NOT NULL,
    related_user_id   STRING       NOT NULL,
    link_type         STRING       NOT NULL,
    confidence_bucket STRING       NOT NULL,
    link_version      INT          NOT NULL,
    effective_at      TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS USER_CONTEXT_HISTORY (
    user_id                STRING       NOT NULL,
    context_version        INT          NOT NULL,
    source_event_id        STRING       NOT NULL,
    dataset_id             STRING       NOT NULL,
    dataset_split          STRING       NOT NULL,
    effective_from         TIMESTAMP_TZ NOT NULL,
    effective_to           TIMESTAMP_TZ,
    is_current             BOOLEAN      NOT NULL,
    account_created_at     TIMESTAMP_TZ NOT NULL,
    country_bucket         STRING       NOT NULL,
    timezone_name          STRING       NOT NULL,
    acquisition_channel    STRING       NOT NULL,
    kyc_level              STRING       NOT NULL,
    account_status         STRING       NOT NULL,
    bankroll_bucket        STRING       NOT NULL,
    preferred_stake_bucket STRING       NOT NULL,
    skill_rating           FLOAT        NOT NULL,
    device_id              STRING       NOT NULL,
    network_cluster_id     STRING       NOT NULL,
    PRIMARY KEY (user_id, context_version)
);

CREATE OR REPLACE VIEW USER_CONTEXT_CURRENT AS
SELECT
    user_id,
    context_version,
    source_event_id,
    dataset_id,
    dataset_split,
    effective_from,
    account_created_at,
    country_bucket,
    timezone_name,
    acquisition_channel,
    kyc_level,
    account_status,
    bankroll_bucket,
    preferred_stake_bucket,
    skill_rating,
    device_id,
    network_cluster_id
FROM USER_CONTEXT_HISTORY
WHERE is_current = TRUE;
