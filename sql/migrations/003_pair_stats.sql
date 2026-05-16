CREATE TABLE IF NOT EXISTS PAIR_STATS (
    player_a                 STRING NOT NULL,
    player_b                 STRING NOT NULL,
    hands_together           INT    NOT NULL,
    chip_transfer_ratio      FLOAT  NOT NULL,
    fold_benefit_ratio       FLOAT  NOT NULL,
    soft_play_frequency      FLOAT  NOT NULL,
    showdown_avoidance_rate  FLOAT  NOT NULL,
    pair_score               FLOAT  NOT NULL,
    computed_at              TIMESTAMP_TZ,
    PRIMARY KEY (player_a, player_b)
);
