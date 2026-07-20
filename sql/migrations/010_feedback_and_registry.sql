CREATE TABLE IF NOT EXISTS MODEL_REGISTRY (
    tenant_id                  STRING       NOT NULL,
    product_id                 STRING       NOT NULL,
    model_name                 STRING       NOT NULL,
    model_run_id               STRING       NOT NULL,
    stage                      STRING       NOT NULL,
    status                     STRING       NOT NULL,
    artifact_uri               STRING       NOT NULL,
    artifact_manifest_sha256   STRING       NOT NULL,
    dataset_id                 STRING       NOT NULL,
    feature_definition_version STRING       NOT NULL,
    evaluation                 VARIANT      NOT NULL,
    registered_at              TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (tenant_id, product_id, model_run_id)
);

CREATE TABLE IF NOT EXISTS MODEL_DEPLOYMENTS (
    tenant_id                  STRING       NOT NULL,
    product_id                 STRING       NOT NULL,
    environment                STRING       NOT NULL,
    deployment_id              STRING       NOT NULL,
    model_name                 STRING       NOT NULL,
    model_run_id               STRING       NOT NULL,
    artifact_manifest_sha256   STRING       NOT NULL,
    feature_definition_version STRING       NOT NULL,
    decision_policy_version    INT          NOT NULL,
    service_build_version      STRING       NOT NULL,
    state                      STRING       NOT NULL,
    deployed_by                STRING       NOT NULL,
    deployed_at                TIMESTAMP_TZ NOT NULL,
    rollback_payload           VARIANT      NOT NULL,
    PRIMARY KEY (tenant_id, product_id, environment, deployment_id)
);

CREATE TABLE IF NOT EXISTS MODEL_MONITORING_WINDOWS (
    tenant_id             STRING       NOT NULL,
    product_id            STRING       NOT NULL,
    model_run_id          STRING       NOT NULL,
    window_start          TIMESTAMP_TZ NOT NULL,
    window_end            TIMESTAMP_TZ NOT NULL,
    feature_drift         VARIANT      NOT NULL,
    score_drift           VARIANT      NOT NULL,
    alert_count           INT          NOT NULL,
    scored_pair_count     INT          NOT NULL,
    status                STRING       NOT NULL,
    recorded_at           TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (tenant_id, product_id, model_run_id, window_start, window_end)
);

CREATE TABLE IF NOT EXISTS ANALYST_FEEDBACK (
    tenant_id          STRING       NOT NULL,
    product_id         STRING       NOT NULL,
    feedback_id        STRING       NOT NULL,
    risk_score_event_id STRING      NOT NULL,
    risk_alert_event_id STRING,
    hand_id            STRING       NOT NULL,
    pair_key           STRING       NOT NULL,
    model_run_id       STRING       NOT NULL,
    disposition        STRING       NOT NULL,
    confidence         FLOAT,
    reason_code        STRING       NOT NULL,
    evidence           VARIANT      NOT NULL,
    analyst_subject    STRING       NOT NULL,
    reviewed_at        TIMESTAMP_TZ NOT NULL,
    label_available_at TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (tenant_id, product_id, feedback_id)
);

CREATE TABLE IF NOT EXISTS MODEL_AUDIT_LOG (
    tenant_id       STRING       NOT NULL,
    product_id      STRING       NOT NULL,
    audit_event_id  STRING       NOT NULL,
    event_type      STRING       NOT NULL,
    actor_subject   STRING       NOT NULL,
    model_run_id    STRING,
    deployment_id   STRING,
    result          STRING       NOT NULL,
    details         VARIANT      NOT NULL,
    occurred_at     TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (tenant_id, product_id, audit_event_id)
);
