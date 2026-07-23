package com.aicampions.poker.context.adapter.jdbc;

/** Operational callbacks kept independent of Flink's metrics API. */
public interface JdbcRepositoryObserver {
    JdbcRepositoryObserver NOOP = new JdbcRepositoryObserver() {
        @Override
        public void retry(JdbcFailureClassifier.Failure failure) {}

        @Override
        public void reconnect() {}
    };

    void retry(JdbcFailureClassifier.Failure failure);

    void reconnect();
}
