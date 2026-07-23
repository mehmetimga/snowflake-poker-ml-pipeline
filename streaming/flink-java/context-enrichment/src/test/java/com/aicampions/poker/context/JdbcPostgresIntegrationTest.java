package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aicampions.poker.context.adapter.jdbc.JdbcFailureClassifier;
import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.jdbc.JdbcUserContextRepository;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import java.time.Instant;
import java.sql.SQLException;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/** Optional contract test against the local PostgreSQL CDC simulation service. */
final class JdbcPostgresIntegrationTest {
    @Test
    void loadsOnlyTheRequestedPokerKitPlayer() throws Exception {
        String url = System.getenv("USER_CONTEXT_TEST_JDBC_URL");
        String userId = System.getenv("USER_CONTEXT_TEST_USER_ID");
        String tenantId =
                System.getenv().getOrDefault("USER_CONTEXT_TEST_TENANT_ID", "demo");
        String productId =
                System.getenv().getOrDefault("USER_CONTEXT_TEST_PRODUCT_ID", "poker");
        Assumptions.assumeTrue(url != null && !url.isBlank());
        Assumptions.assumeTrue(userId != null && !userId.isBlank());

        try (JdbcUserContextRepository repository =
                new JdbcUserContextRepository(
                        url,
                        System.getenv().getOrDefault("USER_CONTEXT_TEST_DB_USER", "poker_sim"),
                        System.getenv().getOrDefault("USER_CONTEXT_TEST_DB_PASSWORD", "poker_sim"),
                        "public.poker_user_context",
                        2)) {
            UserContextRecord context = repository
                    .findEffective(
                            new ContextKey(tenantId, productId, userId),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .orElseThrow();

            assertEquals(tenantId, context.tenantId());
            assertEquals(productId, context.productId());
            assertEquals(userId, context.userId());
            assertEquals(1, context.contextVersion());
            assertTrue(repository
                    .findEffective(
                            new ContextKey(tenantId, productId, "not-a-player-today"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .isEmpty());
        }
    }

    @Test
    void badPostgresCredentialsFailFastWithoutRetry() {
        String url = System.getenv("USER_CONTEXT_TEST_JDBC_URL");
        Assumptions.assumeTrue(url != null && !url.isBlank());

        SQLException failure = assertThrows(
                SQLException.class,
                () -> new JdbcUserContextRepository(
                        url,
                        System.getenv().getOrDefault(
                                "USER_CONTEXT_TEST_DB_USER",
                                "poker_sim"),
                        "deliberately-invalid-password",
                        "public.poker_user_context",
                        1,
                        1,
                        1,
                        0L,
                        JdbcRepositoryObserver.NOOP));

        assertEquals(
                JdbcFailureClassifier.Kind.AUTHENTICATION_OR_AUTHORIZATION,
                JdbcFailureClassifier.classify(failure).kind());
    }
}
