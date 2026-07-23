package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aicampions.poker.context.adapter.jdbc.JdbcConnectionFactory;
import com.aicampions.poker.context.adapter.jdbc.JdbcFailureClassifier;
import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.jdbc.JdbcUserContextRepository;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
                () -> new JdbcUserContextRepository(
                        URL,
                        "sa",
                        "",
                        "public.poker_user_context; DROP TABLE users",
                        1));
    }

    @Test
    void reconnectsAClosedConnectionBeforeLookup() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        AtomicReference<Connection> latest = new AtomicReference<>();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory factory = () -> {
            opens.incrementAndGet();
            Connection connection = realConnection();
            latest.set(connection);
            return connection;
        };

        try (JdbcUserContextRepository repository =
                repository(factory, observer, 1, 1)) {
            latest.get().close();

            UserContextRecord result = repository
                    .findEffective(key(), playedAt())
                    .orElseThrow();

            assertEquals(2, opens.get());
            assertEquals(1, observer.reconnects.get());
            assertEquals(2, result.contextVersion());
        }
    }

    @Test
    void retriesConnectionFailureOnceAfterReconnect() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory factory = () -> {
            int attempt = opens.incrementAndGet();
            Connection connection = realConnection();
            return attempt == 1
                    ? instrument(
                            connection,
                            "08006",
                            new AtomicInteger(),
                            new AtomicInteger())
                    : connection;
        };

        try (JdbcUserContextRepository repository =
                repository(factory, observer, 1, 1)) {
            UserContextRecord result = repository
                    .findEffective(key(), playedAt())
                    .orElseThrow();

            assertEquals(2, opens.get());
            assertEquals(1, observer.retries.get());
            assertEquals(1, observer.reconnects.get());
            assertEquals(2, result.contextVersion());
        }
    }

    @Test
    void retriesPostgresQueryTimeoutOnce() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory factory = () -> {
            int attempt = opens.incrementAndGet();
            Connection connection = realConnection();
            return attempt == 1
                    ? instrument(
                            connection,
                            "57014",
                            new AtomicInteger(),
                            new AtomicInteger())
                    : connection;
        };

        try (JdbcUserContextRepository repository =
                repository(factory, observer, 1, 1)) {
            assertTrue(repository
                    .findEffective(key(), playedAt())
                    .isPresent());
            assertEquals(2, opens.get());
            assertEquals(1, observer.retries.get());
            assertEquals(1, observer.reconnects.get());
        }
    }

    @Test
    void neverRetriesMoreThanOnce() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory factory = () -> {
            opens.incrementAndGet();
            return instrument(
                    realConnection(),
                    "08006",
                    new AtomicInteger(),
                    new AtomicInteger());
        };

        try (JdbcUserContextRepository repository =
                repository(factory, observer, 1, 1)) {
            SQLException failure = assertThrows(
                    SQLException.class,
                    () -> repository.findEffective(key(), playedAt()));

            assertEquals("08006", failure.getSQLState());
            assertEquals(2, opens.get());
            assertEquals(1, observer.retries.get());
            assertEquals(1, observer.reconnects.get());
        }
    }

    @Test
    void retriesInitialTransientOutageButAuthenticationFailsFast()
            throws Exception {
        AtomicInteger outageOpens = new AtomicInteger();
        RecordingObserver outageObserver = new RecordingObserver();
        JdbcConnectionFactory recovers = () -> {
            if (outageOpens.incrementAndGet() == 1) {
                throw new SQLException("database unavailable", "08001");
            }
            return realConnection();
        };

        try (JdbcUserContextRepository ignored =
                repository(recovers, outageObserver, 1, 1)) {
            assertEquals(2, outageOpens.get());
            assertEquals(1, outageObserver.retries.get());
            assertEquals(0, outageObserver.reconnects.get());
        }

        AtomicInteger authOpens = new AtomicInteger();
        RecordingObserver authObserver = new RecordingObserver();
        JdbcConnectionFactory denied = () -> {
            authOpens.incrementAndGet();
            throw new SQLException("secret account denied", "28000");
        };
        SQLException failure = assertThrows(
                SQLException.class,
                () -> repository(denied, authObserver, 1, 1));
        assertEquals("28000", failure.getSQLState());
        assertEquals(1, authOpens.get());
        assertEquals(0, authObserver.retries.get());
    }

    @Test
    void persistentInitialOutageStopsAfterOneRetry() {
        AtomicInteger opens = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory unavailable = () -> {
            opens.incrementAndGet();
            throw new SQLException("database unavailable", "08001");
        };

        SQLException failure = assertThrows(
                SQLException.class,
                () -> repository(unavailable, observer, 1, 1));

        assertEquals("08001", failure.getSQLState());
        assertEquals(2, opens.get());
        assertEquals(1, observer.retries.get());
        assertEquals(0, observer.reconnects.get());
    }

    @Test
    void nonTransientQueryFailureDoesNotReconnect() throws Exception {
        AtomicInteger opens = new AtomicInteger();
        RecordingObserver observer = new RecordingObserver();
        JdbcConnectionFactory factory = () -> {
            opens.incrementAndGet();
            return instrument(
                    realConnection(),
                    "42P01",
                    new AtomicInteger(),
                    new AtomicInteger());
        };

        try (JdbcUserContextRepository repository =
                repository(factory, observer, 1, 1)) {
            SQLException failure = assertThrows(
                    SQLException.class,
                    () -> repository.findEffective(key(), playedAt()));

            assertEquals("42P01", failure.getSQLState());
            assertEquals(1, opens.get());
            assertEquals(0, observer.retries.get());
            assertEquals(0, observer.reconnects.get());
        }
    }

    @Test
    void appliesQueryAndConnectionValidationTimeouts() throws Exception {
        AtomicInteger validationTimeout = new AtomicInteger(-1);
        AtomicInteger queryTimeout = new AtomicInteger(-1);
        JdbcConnectionFactory factory = () -> instrument(
                realConnection(),
                null,
                validationTimeout,
                queryTimeout);

        try (JdbcUserContextRepository repository =
                repository(
                        factory,
                        JdbcRepositoryObserver.NOOP,
                        7,
                        2)) {
            assertTrue(repository
                    .findEffective(key(), playedAt())
                    .isPresent());
            assertEquals(7, queryTimeout.get());
            assertEquals(2, validationTimeout.get());
        }
    }

    private static JdbcUserContextRepository repository(
            JdbcConnectionFactory factory,
            JdbcRepositoryObserver observer,
            int queryTimeoutSeconds,
            int validationTimeoutSeconds)
            throws Exception {
        return new JdbcUserContextRepository(
                factory,
                "public.poker_user_context",
                queryTimeoutSeconds,
                validationTimeoutSeconds,
                () -> {},
                observer);
    }

    private static ContextKey key() {
        return new ContextKey("tenant-a", "poker", "A");
    }

    private static long playedAt() {
        return Instant.parse("2026-07-23T12:00:00Z")
                .toEpochMilli();
    }

    private static Connection realConnection() throws SQLException {
        return DriverManager.getConnection(URL, "sa", "");
    }

    private static Connection instrument(
            Connection delegate,
            String queryFailureSqlState,
            AtomicInteger validationTimeout,
            AtomicInteger queryTimeout) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("isValid")) {
                        validationTimeout.set((Integer) arguments[0]);
                    }
                    Object result = invoke(delegate, method, arguments);
                    if (!method.getName().equals("prepareStatement")) {
                        return result;
                    }
                    PreparedStatement statement =
                            (PreparedStatement) result;
                    return Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[] {PreparedStatement.class},
                            (statementProxy, statementMethod, statementArgs) -> {
                                if (statementMethod
                                        .getName()
                                        .equals("setQueryTimeout")) {
                                    queryTimeout.set(
                                            (Integer) statementArgs[0]);
                                }
                                if (queryFailureSqlState != null
                                        && statementMethod
                                                .getName()
                                                .equals("executeQuery")) {
                                    throw new SQLException(
                                            "injected JDBC failure",
                                            queryFailureSqlState);
                                }
                                return invoke(
                                        statement,
                                        statementMethod,
                                        statementArgs);
                            });
                });
    }

    private static Object invoke(
            Object target,
            java.lang.reflect.Method method,
            Object[] arguments)
            throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }

    private static final class RecordingObserver
            implements JdbcRepositoryObserver {
        private final AtomicInteger retries = new AtomicInteger();
        private final AtomicInteger reconnects = new AtomicInteger();

        @Override
        public void retry(JdbcFailureClassifier.Failure failure) {
            retries.incrementAndGet();
        }

        @Override
        public void reconnect() {
            reconnects.incrementAndGet();
        }
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
