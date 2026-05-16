CREATE TABLE IF NOT EXISTS MODEL_METRICS (
    run_id              STRING       NOT NULL,
    model_name          STRING       NOT NULL,
    roc_auc             FLOAT        NOT NULL,
    pr_auc              FLOAT        NOT NULL,
    f1                  FLOAT        NOT NULL,
    optimal_threshold   FLOAT        NOT NULL,
    n_train             INT          NOT NULL,
    n_test              INT          NOT NULL,
    trained_at          TIMESTAMP_TZ NOT NULL,
    PRIMARY KEY (run_id, model_name)
);
