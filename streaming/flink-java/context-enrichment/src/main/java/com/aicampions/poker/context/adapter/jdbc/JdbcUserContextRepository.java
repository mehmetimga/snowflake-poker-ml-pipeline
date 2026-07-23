package com.aicampions.poker.context.adapter.jdbc;

import com.aicampions.poker.context.config.JdbcTableName;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.aicampions.poker.context.port.UserContextRepository;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.sql.Timestamp;
import java.util.Optional;

/** Synchronous point-in-time JDBC lookup used only on Flink cache misses. */
public final class JdbcUserContextRepository implements UserContextRepository {
    private final JdbcConnectionFactory connectionFactory;
    private final String lookupSql;
    private final int queryTimeoutSeconds;
    private final int validationTimeoutSeconds;
    private final JdbcRetryDelay retryDelay;
    private final JdbcRepositoryObserver observer;

    private Connection connection;
    private PreparedStatement lookup;

    public JdbcUserContextRepository(
            String jdbcUrl,
            String username,
            String password,
            String tableName,
            int queryTimeoutSeconds)
            throws Exception {
        this(
                jdbcUrl,
                username,
                password,
                tableName,
                queryTimeoutSeconds,
                3,
                1,
                0L,
                JdbcRepositoryObserver.NOOP);
    }

    public JdbcUserContextRepository(
            String jdbcUrl,
            String username,
            String password,
            String tableName,
            int queryTimeoutSeconds,
            int connectTimeoutSeconds,
            int validationTimeoutSeconds,
            long retryMaximumJitterMs,
            JdbcRepositoryObserver observer)
            throws Exception {
        this(
                driverManagerFactory(
                        jdbcUrl,
                        username,
                        password,
                        connectTimeoutSeconds,
                        queryTimeoutSeconds),
                tableName,
                queryTimeoutSeconds,
                validationTimeoutSeconds,
                JdbcRetryDelay.jittered(retryMaximumJitterMs),
                observer);
    }

    public JdbcUserContextRepository(
            JdbcConnectionFactory connectionFactory,
            String tableName,
            int queryTimeoutSeconds,
            int validationTimeoutSeconds,
            JdbcRetryDelay retryDelay,
            JdbcRepositoryObserver observer)
            throws Exception {
        this.connectionFactory = require(connectionFactory, "connectionFactory");
        this.queryTimeoutSeconds = positive(
                queryTimeoutSeconds, "queryTimeoutSeconds");
        this.validationTimeoutSeconds = positive(
                validationTimeoutSeconds, "validationTimeoutSeconds");
        this.retryDelay = require(retryDelay, "retryDelay");
        this.observer = require(observer, "observer");
        String validatedTable = JdbcTableName.validate(tableName);
        this.lookupSql = """
                SELECT tenant_id, product_id, user_id, context_version,
                       effective_at, account_created_at,
                       country_bucket, timezone, acquisition_channel, kyc_level,
                       account_status, bankroll_bucket, preferred_stake_bucket,
                       skill_rating, device_id, network_cluster_id
                FROM %s
                WHERE tenant_id = ? AND product_id = ? AND user_id = ?
                  AND effective_at <= ?
                ORDER BY effective_at DESC, context_version DESC
                LIMIT 1
                """.formatted(validatedTable);
        connectInitially();
    }

    @Override
    public Optional<UserContextRecord> findEffective(
            ContextKey key, long playedAtMs)
            throws Exception {
        key.validate();
        try {
            ensureUsableConnection();
            return executeLookup(key, playedAtMs);
        } catch (Exception firstFailure) {
            JdbcFailureClassifier.Failure classified =
                    JdbcFailureClassifier.classify(firstFailure);
            if (classified.kind()
                    != JdbcFailureClassifier.Kind.TRANSIENT) {
                throw firstFailure;
            }
            observer.retry(classified);
            pauseBeforeRetry();
            reconnect();
            return executeLookup(key, playedAtMs);
        }
    }

    private void connectInitially() throws Exception {
        try {
            connect();
        } catch (Exception firstFailure) {
            JdbcFailureClassifier.Failure classified =
                    JdbcFailureClassifier.classify(firstFailure);
            if (classified.kind()
                    != JdbcFailureClassifier.Kind.TRANSIENT) {
                throw firstFailure;
            }
            observer.retry(classified);
            pauseBeforeRetry();
            connect();
        }
    }

    private void ensureUsableConnection() throws Exception {
        if (connection == null
                || connection.isClosed()
                || !connection.isValid(validationTimeoutSeconds)) {
            reconnect();
        }
    }

    private void reconnect() throws Exception {
        observer.reconnect();
        closeQuietly();
        connect();
    }

    private void connect() throws Exception {
        Connection opened = connectionFactory.open();
        PreparedStatement prepared = null;
        try {
            prepared = opened.prepareStatement(lookupSql);
            prepared.setQueryTimeout(queryTimeoutSeconds);
            connection = opened;
            lookup = prepared;
        } catch (Exception error) {
            if (prepared != null) {
                try {
                    prepared.close();
                } catch (Exception ignored) {
                    // Preserve the connection/setup failure.
                }
            }
            try {
                opened.close();
            } catch (Exception ignored) {
                // Preserve the connection/setup failure.
            }
            throw error;
        }
    }

    private Optional<UserContextRecord> executeLookup(
            ContextKey key, long playedAtMs)
            throws Exception {
        lookup.clearParameters();
        lookup.setString(1, key.getTenantId());
        lookup.setString(2, key.getProductId());
        lookup.setString(3, key.getPlayerId());
        lookup.setObject(
                4,
                OffsetDateTime.ofInstant(
                        Instant.ofEpochMilli(playedAtMs),
                        ZoneOffset.UTC));
        try (ResultSet rows = lookup.executeQuery()) {
            if (!rows.next()) {
                return Optional.empty();
            }
            return Optional.of(new UserContextRecord(
                    rows.getString("tenant_id"),
                    rows.getString("product_id"),
                    rows.getString("user_id"),
                    rows.getInt("context_version"),
                    instant(rows, "effective_at"),
                    instant(rows, "account_created_at"),
                    rows.getString("country_bucket"),
                    rows.getString("timezone"),
                    rows.getString("acquisition_channel"),
                    rows.getString("kyc_level"),
                    rows.getString("account_status"),
                    rows.getString("bankroll_bucket"),
                    rows.getString("preferred_stake_bucket"),
                    rows.getDouble("skill_rating"),
                    rows.getString("device_id"),
                    rows.getString("network_cluster_id")));
        }
    }

    private void pauseBeforeRetry() throws InterruptedException {
        try {
            retryDelay.pause();
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw error;
        }
    }

    private static JdbcConnectionFactory driverManagerFactory(
            String jdbcUrl,
            String username,
            String password,
            int connectTimeoutSeconds,
            int queryTimeoutSeconds)
            throws ClassNotFoundException {
        // Flink loads user JARs through a child-first classloader. Explicit
        // registration avoids DriverManager discovery crossing that boundary.
        Class.forName("org.postgresql.Driver");
        return new DriverManagerConnectionFactory(
                jdbcUrl,
                username,
                password,
                positive(connectTimeoutSeconds, "connectTimeoutSeconds"),
                Math.max(
                        positive(queryTimeoutSeconds, "queryTimeoutSeconds")
                                + 1,
                        2));
    }

    private static Instant instant(ResultSet rows, String column)
            throws Exception {
        try {
            OffsetDateTime value =
                    rows.getObject(column, OffsetDateTime.class);
            if (value != null) {
                return value.toInstant();
            }
        } catch (Exception unsupportedConversion) {
            Timestamp value = rows.getTimestamp(column);
            if (value != null) {
                return value.toInstant();
            }
            throw unsupportedConversion;
        }
        throw new IllegalArgumentException(
                "context timestamp is null: " + column);
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }

    private static <T> T require(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private void closeQuietly() {
        if (lookup != null) {
            try {
                lookup.close();
            } catch (Exception ignored) {
                // Reconnect replaces this resource.
            }
            lookup = null;
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception ignored) {
                // Reconnect replaces this resource.
            }
            connection = null;
        }
    }

    @Override
    public void close() {
        closeQuietly();
    }
}
