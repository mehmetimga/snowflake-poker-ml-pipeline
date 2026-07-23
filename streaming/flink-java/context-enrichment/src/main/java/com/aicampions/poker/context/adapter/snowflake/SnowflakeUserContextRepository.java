package com.aicampions.poker.context.adapter.snowflake;

import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.jdbc.JdbcRetryDelay;
import com.aicampions.poker.context.adapter.jdbc.JdbcUserContextRepository;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.aicampions.poker.context.port.UserContextRepository;
import java.util.Optional;

/** Point-in-time user-context projection read internally through the SPCS service identity. */
public final class SnowflakeUserContextRepository
        implements UserContextRepository {
    private final JdbcUserContextRepository delegate;

    public SnowflakeUserContextRepository(
            SnowflakeServiceCredentials credentials,
            String tableName,
            int queryTimeoutSeconds,
            int connectTimeoutSeconds,
            int validationTimeoutSeconds,
            long retryMaximumJitterMs,
            JdbcRepositoryObserver observer)
            throws Exception {
        delegate = new JdbcUserContextRepository(
                new SnowflakeServiceConnectionFactory(
                        credentials,
                        connectTimeoutSeconds,
                        Math.max(queryTimeoutSeconds + 5, 10)),
                tableName,
                queryTimeoutSeconds,
                validationTimeoutSeconds,
                JdbcRetryDelay.jittered(retryMaximumJitterMs),
                observer);
    }

    @Override
    public Optional<UserContextRecord> findEffective(
            ContextKey key, long playedAtMs)
            throws Exception {
        return delegate.findEffective(key, playedAtMs);
    }

    @Override
    public void close() {
        delegate.close();
    }
}
