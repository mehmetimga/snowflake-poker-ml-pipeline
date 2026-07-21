CREATE TABLE IF NOT EXISTS RULE_EVIDENCE_EVENTS (
    event_id                   STRING       NOT NULL,
    event_type                 STRING       NOT NULL,
    schema_version             INT          NOT NULL,
    tenant_id                  STRING       NOT NULL,
    product_id                 STRING       NOT NULL,
    dataset_id                 STRING       NOT NULL,
    dataset_split              STRING       NOT NULL,
    occurred_at                TIMESTAMP_TZ NOT NULL,
    emitted_at                 TIMESTAMP_TZ NOT NULL,
    trace_id                   STRING       NOT NULL,
    rule_event_id              STRING       NOT NULL,
    rule_id                    STRING       NOT NULL,
    rule_version               INT          NOT NULL,
    rule_owner                 STRING       NOT NULL,
    entity_type                STRING       NOT NULL,
    entity_key                 STRING       NOT NULL,
    hand_id                    STRING       NOT NULL,
    observation_revision       INT          NOT NULL,
    severity                   STRING       NOT NULL,
    raw_score                  FLOAT        NOT NULL,
    evidence                   VARIANT      NOT NULL,
    effective_at               TIMESTAMP_TZ NOT NULL,
    feature_definition_version STRING       NOT NULL,
    payload                    VARIANT      NOT NULL,
    event_sha256               STRING       NOT NULL,
    kafka_topic                STRING,
    kafka_partition            INT,
    kafka_offset               BIGINT,
    kafka_timestamp_ms         BIGINT,
    ingested_at                TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE TABLE IF NOT EXISTS RISK_SCORE_RULE_EVIDENCE (
    tenant_id          STRING       NOT NULL,
    product_id         STRING       NOT NULL,
    risk_score_event_id STRING      NOT NULL,
    model_run_id       STRING       NOT NULL,
    rule_event_id      STRING       NOT NULL,
    hand_id            STRING       NOT NULL,
    referenced_at      TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (tenant_id, product_id, risk_score_event_id, rule_event_id)
);

CREATE OR REPLACE VIEW RULE_EVIDENCE_WITH_MODEL_LINEAGE AS
SELECT
    rules.*,
    score_refs.risk_score_event_id,
    score_refs.model_run_id,
    score_refs.referenced_at
FROM RULE_EVIDENCE_EVENTS rules
LEFT JOIN RISK_SCORE_RULE_EVIDENCE score_refs
    ON score_refs.tenant_id = rules.tenant_id
    AND score_refs.product_id = rules.product_id
    AND score_refs.rule_event_id = rules.rule_event_id;
