CREATE TABLE IF NOT EXISTS ALERTS (
    alert_id              STRING       NOT NULL,
    hand_id               STRING       NOT NULL,
    table_id              STRING       NOT NULL,
    suspicious_player_id  STRING       NOT NULL,
    risk_score            FLOAT        NOT NULL,
    risk_level            STRING       NOT NULL,
    triggered_rules       ARRAY,
    model_scores          VARIANT,
    created_at            TIMESTAMP_TZ NOT NULL,
    status                STRING       NOT NULL DEFAULT 'pending',
    PRIMARY KEY (alert_id)
);
