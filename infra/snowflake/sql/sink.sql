USE ROLE SYSADMIN;
USE DATABASE POKER_ML_DEMO;
USE SCHEMA SPCS;

-- Immutable Kafka envelope ledger. POKER_SINK writes this row and the matching
-- event-native row in one Snowflake transaction, then commits the Kafka offset.
CREATE TABLE IF NOT EXISTS POKER_EVENT_ENVELOPES (
    event_id            STRING       NOT NULL,
    event_type          STRING       NOT NULL,
    schema_version      INT          NOT NULL,
    event_kind          STRING       NOT NULL,
    tenant_id           STRING       NOT NULL,
    product_id          STRING       NOT NULL,
    dataset_id          STRING       NOT NULL,
    dataset_split       STRING       NOT NULL,
    occurred_at         TIMESTAMP_TZ NOT NULL,
    emitted_at          TIMESTAMP_TZ NOT NULL,
    trace_id            STRING       NOT NULL,
    event_sha256        STRING       NOT NULL,
    event_json          VARIANT      NOT NULL,
    source_topic        STRING       NOT NULL,
    source_partition    INT          NOT NULL,
    source_offset       BIGINT       NOT NULL,
    source_timestamp_ms BIGINT       NOT NULL,
    key_sha256          STRING       NOT NULL,
    sink_build_version  STRING       NOT NULL,
    ingested_at         TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

-- Each table deliberately has the same stable audit columns. The payload stays
-- lossless VARIANT while frequently joined identities are projected as columns.
CREATE TABLE IF NOT EXISTS POKER_HAND_EVENTS (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_PLAYER_CONTEXT_EVENTS (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_PAIR_FEATURE_EVENTS_V2 (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_RISK_SCORE_EVENTS (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_RULE_EVIDENCE_EVENTS_V2 (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_REVIEW_DECISION_EVENTS (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS POKER_RISK_ALERT_EVENTS (
    event_id STRING NOT NULL, tenant_id STRING NOT NULL, product_id STRING NOT NULL,
    dataset_id STRING NOT NULL, dataset_split STRING NOT NULL, hand_id STRING,
    table_id STRING, entity_key STRING NOT NULL, revision INT NOT NULL,
    occurred_at TIMESTAMP_TZ NOT NULL, emitted_at TIMESTAMP_TZ NOT NULL,
    trace_id STRING NOT NULL, payload VARIANT NOT NULL, event_sha256 STRING NOT NULL,
    ingested_at TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (event_id)
);

-- Poison events contain only categorical diagnostics and hashes. Raw bytes are
-- intentionally excluded so credentials or personal data cannot leak here.
CREATE TABLE IF NOT EXISTS POKER_SINK_DEAD_LETTERS (
    dead_letter_id      STRING       NOT NULL,
    source_topic       STRING       NOT NULL,
    source_partition   INT          NOT NULL,
    source_offset      BIGINT       NOT NULL,
    source_timestamp_ms BIGINT      NOT NULL,
    key_sha256         STRING       NOT NULL,
    event_sha256       STRING       NOT NULL,
    error_code         STRING       NOT NULL,
    sink_build_version STRING       NOT NULL,
    ingested_at        TIMESTAMP_TZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (dead_letter_id)
);

CREATE OR REPLACE VIEW POKER_ALERT_REVIEW_V AS
SELECT
    alerts.event_id                                           AS alert_event_id,
    alerts.payload:alert_id::STRING                           AS alert_id,
    alerts.tenant_id,
    alerts.product_id,
    alerts.dataset_id,
    alerts.dataset_split,
    alerts.hand_id,
    alerts.table_id,
    alerts.occurred_at                                        AS played_at,
    alerts.payload:risk_probability::FLOAT                    AS risk_probability,
    alerts.payload:highest_risk_pair:pair_key::STRING         AS highest_risk_pair,
    alerts.payload:model_name::STRING                         AS model_name,
    alerts.payload:model_run_id::STRING                       AS model_run_id,
    alerts.payload:policy_outcome::STRING                     AS policy_outcome,
    alerts.payload:policy_reason_codes                        AS policy_reason_codes,
    decisions.event_id                                        AS review_decision_event_id,
    decisions.payload:outcome::STRING                         AS review_outcome,
    decisions.payload:action::STRING                          AS review_action,
    scores.event_id                                            AS risk_score_event_id,
    scores.payload:hand_risk_probability::FLOAT               AS score_probability,
    scores.payload:alert::BOOLEAN                             AS score_alert,
    COALESCE(ARRAY_SIZE(alerts.payload:rule_evidence_event_ids), 0)
                                                                 AS rule_evidence_count,
    alerts.ingested_at
FROM POKER_RISK_ALERT_EVENTS alerts
LEFT JOIN POKER_REVIEW_DECISION_EVENTS decisions
    ON decisions.event_id =
       alerts.payload:review_decision_event_id::STRING
LEFT JOIN POKER_RISK_SCORE_EVENTS scores
    ON scores.event_id = alerts.payload:risk_score_event_id::STRING;

CREATE OR REPLACE VIEW POKER_SINK_TOPIC_PROGRESS_V AS
SELECT
    source_topic,
    source_partition,
    MAX(source_offset) AS maximum_persisted_offset,
    COUNT(*) AS persisted_events,
    MAX(ingested_at) AS last_ingested_at
FROM POKER_EVENT_ENVELOPES
GROUP BY source_topic, source_partition;
