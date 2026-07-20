CREATE TABLE IF NOT EXISTS PAIR_LABELS (
    example_id         STRING       NOT NULL,
    dataset_id         STRING       NOT NULL,
    dataset_split      STRING       NOT NULL,
    hand_id            STRING       NOT NULL,
    pair_key           STRING       NOT NULL,
    player_a           STRING       NOT NULL,
    player_b           STRING       NOT NULL,
    is_collusive       BOOLEAN      NOT NULL,
    collusion_pair_id  STRING,
    label_available_at TIMESTAMP_TZ NOT NULL,
    provenance         STRING       NOT NULL,
    ingested_at        TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (example_id)
);

CREATE OR REPLACE VIEW PAIR_TRAINING_EXAMPLES AS
SELECT
    f.event_id AS feature_event_id,
    l.example_id AS label_example_id,
    f.dataset_id,
    f.dataset_split,
    f.hand_id,
    f.pair_key,
    f.player_a,
    f.player_b,
    f.played_at,
    f.snapshot_revision,
    f.feature_definition_version,
    f.payload AS feature_payload,
    l.is_collusive AS target,
    l.label_available_at,
    l.provenance AS label_provenance
FROM PAIR_FEATURE_LATEST f
INNER JOIN PAIR_LABELS l
    ON l.dataset_id = f.dataset_id
    AND l.dataset_split = f.dataset_split
    AND l.hand_id = f.hand_id
    AND l.pair_key = f.pair_key
WHERE l.label_available_at >= f.played_at;
