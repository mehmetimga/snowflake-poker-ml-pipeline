package com.aicampions.poker.context;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.regex.Pattern;

/** Synchronous, point-in-time PostgreSQL lookup used only on Flink cache misses. */
final class JdbcUserContextRepository implements UserContextRepository {
    private static final Pattern TABLE_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)?");

    private final Connection connection;
    private final PreparedStatement lookup;

    JdbcUserContextRepository(
            String jdbcUrl,
            String username,
            String password,
            String tableName,
            int queryTimeoutSeconds)
            throws Exception {
        validateTableName(tableName);
        connection = DriverManager.getConnection(jdbcUrl, username, password);
        connection.setReadOnly(true);
        lookup = connection.prepareStatement("""
                SELECT user_id, context_version, effective_at, account_created_at,
                       country_bucket, timezone, acquisition_channel, kyc_level,
                       account_status, bankroll_bucket, preferred_stake_bucket,
                       skill_rating, device_id, network_cluster_id
                FROM %s
                WHERE user_id = ? AND effective_at <= ?
                ORDER BY effective_at DESC, context_version DESC
                LIMIT 1
                """.formatted(tableName));
        lookup.setQueryTimeout(queryTimeoutSeconds);
    }

    static void validateTableName(String tableName) {
        if (tableName == null || !TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("invalid user-context table name");
        }
    }

    @Override
    public Optional<UserContextRecord> findEffective(String userId, long playedAtMs)
            throws Exception {
        lookup.clearParameters();
        lookup.setString(1, userId);
        lookup.setObject(
                2,
                OffsetDateTime.ofInstant(Instant.ofEpochMilli(playedAtMs), ZoneOffset.UTC));
        try (ResultSet rows = lookup.executeQuery()) {
            if (!rows.next()) {
                return Optional.empty();
            }
            return Optional.of(new UserContextRecord(
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

    private static Instant instant(ResultSet rows, String column) throws Exception {
        return rows.getObject(column, OffsetDateTime.class).toInstant();
    }

    @Override
    public void close() throws Exception {
        try {
            lookup.close();
        } finally {
            connection.close();
        }
    }
}
