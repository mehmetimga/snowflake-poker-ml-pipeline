CREATE TABLE IF NOT EXISTS RAW_HANDS (
    hand_id        STRING       NOT NULL,
    table_id       STRING       NOT NULL,
    played_at      TIMESTAMP_TZ NOT NULL,
    small_blind    FLOAT        NOT NULL,
    big_blind      FLOAT        NOT NULL,
    num_players    INT          NOT NULL,
    pot_size       FLOAT        NOT NULL,
    board          ARRAY,
    PRIMARY KEY (hand_id)
);

CREATE TABLE IF NOT EXISTS RAW_ACTIONS (
    hand_id        STRING       NOT NULL,
    sequence_no    INT          NOT NULL,
    player_id      STRING       NOT NULL,
    street         STRING       NOT NULL,
    action_type    STRING       NOT NULL,
    amount         FLOAT        NOT NULL,
    PRIMARY KEY (hand_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS RAW_PLAYERS (
    hand_id        STRING       NOT NULL,
    player_id      STRING       NOT NULL,
    name           STRING       NOT NULL,
    position       STRING       NOT NULL,
    stack_start    FLOAT        NOT NULL,
    hole_cards     STRING,
    won_amount     FLOAT        NOT NULL,
    is_suspicious  BOOLEAN      NOT NULL DEFAULT FALSE,
    collusion_pair_id STRING,
    PRIMARY KEY (hand_id, player_id)
);
