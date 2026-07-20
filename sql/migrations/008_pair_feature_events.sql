CREATE TABLE IF NOT EXISTS PAIR_FEATURE_EVENTS (
    event_id                         STRING       NOT NULL,
    event_type                       STRING       NOT NULL,
    schema_version                   INT          NOT NULL,
    tenant_id                        STRING       NOT NULL,
    product_id                       STRING       NOT NULL,
    dataset_id                       STRING       NOT NULL,
    dataset_split                    STRING       NOT NULL,
    occurred_at                      TIMESTAMP_TZ NOT NULL,
    emitted_at                       TIMESTAMP_TZ NOT NULL,
    trace_id                         STRING       NOT NULL,
    hand_id                          STRING       NOT NULL,
    table_id                         STRING       NOT NULL,
    played_at                        TIMESTAMP_TZ NOT NULL,
    pair_key                         STRING       NOT NULL,
    player_a                         STRING       NOT NULL,
    player_b                         STRING       NOT NULL,
    num_players                      INT          NOT NULL,
    source_hand_event_id             STRING       NOT NULL,
    source_player_context_event_id_a STRING       NOT NULL,
    source_player_context_event_id_b STRING       NOT NULL,
    source_revision_a                INT          NOT NULL,
    source_revision_b                INT          NOT NULL,
    context_status_a                 STRING       NOT NULL,
    context_status_b                 STRING       NOT NULL,
    context_version_a                INT,
    context_version_b                INT,
    snapshot_revision                INT          NOT NULL,
    feature_definition_version       STRING       NOT NULL,
    payload                           VARIANT      NOT NULL,
    kafka_topic                       STRING,
    kafka_partition                   INT,
    kafka_offset                      BIGINT,
    kafka_timestamp_ms                BIGINT,
    ingested_at                       TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (event_id)
);

CREATE OR REPLACE VIEW PAIR_FEATURE_LATEST AS
SELECT *
FROM PAIR_FEATURE_EVENTS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY dataset_id, dataset_split, hand_id, pair_key
    ORDER BY snapshot_revision DESC, emitted_at DESC, event_id DESC
) = 1;
