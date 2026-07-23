package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

final class JdbcUserContextRepositoryTest {
    private static final String URL =
            "jdbc:h2:mem:context_lookup;MODE=PostgreSQL;DB_CLOSE_DELAY=-1";

    @BeforeEach
    void createProjection() throws Exception {
        try (Connection connection = DriverManager.getConnection(URL, "sa", "");
                Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS public.poker_user_context");
            statement.execute("CREATE SCHEMA IF NOT EXISTS public");
            statement.execute("""
                    CREATE TABLE public.poker_user_context (
                      tenant_id VARCHAR NOT NULL,
                      product_id VARCHAR NOT NULL,
                      user_id VARCHAR NOT NULL,
                      context_version INTEGER NOT NULL,
                      effective_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      account_created_at TIMESTAMP WITH TIME ZONE NOT NULL,
                      country_bucket VARCHAR NOT NULL,
                      timezone VARCHAR NOT NULL,
                      acquisition_channel VARCHAR NOT NULL,
                      kyc_level VARCHAR NOT NULL,
                      account_status VARCHAR NOT NULL,
                      bankroll_bucket VARCHAR NOT NULL,
                      preferred_stake_bucket VARCHAR NOT NULL,
                      skill_rating DOUBLE PRECISION NOT NULL,
                      device_id VARCHAR NOT NULL,
                      network_cluster_id VARCHAR NOT NULL,
                      PRIMARY KEY (tenant_id, product_id, user_id, context_version)
                    )
                    """);
            insert(statement, "tenant-a", "poker", 1,
                    "2026-07-23T08:00:00Z", "device-old");
            insert(statement, "tenant-a", "poker", 2,
                    "2026-07-23T10:00:00Z", "device-current");
            insert(statement, "tenant-a", "poker", 3,
                    "2026-07-23T13:00:00Z", "device-future");
            insert(statement, "tenant-b", "poker", 1,
                    "2026-07-23T09:00:00Z", "device-other-tenant");
        }
    }

    @Test
    void selectsLatestVersionEffectiveAtHandTime() throws Exception {
        try (JdbcUserContextRepository repository =
                new JdbcUserContextRepository(
                        URL, "sa", "", "public.poker_user_context", 1)) {
            UserContextRecord result = repository
                    .findEffective(
                            new ContextKey("tenant-a", "poker", "A"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .orElseThrow();

            assertEquals("tenant-a", result.tenantId());
            assertEquals("poker", result.productId());
            assertEquals(2, result.contextVersion());
            assertEquals("device-current", result.deviceId());
            assertTrue(repository
                    .findEffective(
                            new ContextKey("tenant-a", "poker", "G"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .isEmpty());
        }
    }

    @Test
    void isolatesTheSamePlayerIdAcrossTenants() throws Exception {
        try (JdbcUserContextRepository repository =
                new JdbcUserContextRepository(
                        URL, "sa", "", "public.poker_user_context", 1)) {
            UserContextRecord tenantA = repository
                    .findEffective(
                            new ContextKey("tenant-a", "poker", "A"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .orElseThrow();
            UserContextRecord tenantB = repository
                    .findEffective(
                            new ContextKey("tenant-b", "poker", "A"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .orElseThrow();

            assertEquals("device-current", tenantA.deviceId());
            assertEquals("device-other-tenant", tenantB.deviceId());
        }
    }

    @Test
    void rejectsUnsafeDynamicTableName() {
        assertThrows(
                IllegalArgumentException.class,
                () -> JdbcUserContextRepository.validateTableName(
                        "public.poker_user_context; DROP TABLE users"));
    }

    private static void insert(
            Statement statement,
            String tenantId,
            String productId,
            int version,
            String effectiveAt,
            String deviceId)
            throws Exception {
        statement.execute("""
                INSERT INTO public.poker_user_context VALUES (
                  '%s', '%s', 'A', %d, TIMESTAMP WITH TIME ZONE '%s',
                  TIMESTAMP WITH TIME ZONE '2025-01-01T00:00:00Z',
                  'TR', 'Europe/Istanbul', 'organic', 'verified', 'active',
                  'medium', 'low', 0.63, '%s', 'network-18'
                )
                """.formatted(tenantId, productId, version, effectiveAt, deviceId));
    }
}
