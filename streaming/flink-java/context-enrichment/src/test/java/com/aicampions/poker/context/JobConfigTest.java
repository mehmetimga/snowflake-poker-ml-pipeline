package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aicampions.poker.context.config.ContextJobConfig;
import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class JobConfigTest {
    @Test
    void parsesOperationalOptionsWithoutPrintingSecrets() {
        ContextJobConfig config = ContextJobConfig.parse(
                new String[] {
                    "--from-beginning",
                    "--bounded",
                    "--parallelism", "3",
                    "--allowed-lateness-ms", "5000"
                },
                Map.of(
                        "KAFKA_SECURITY_PROTOCOL", "SASL_SSL",
                        "KAFKA_SASL_MECHANISM", "PLAIN",
                        "KAFKA_SASL_USERNAME", "api-key",
                        "KAFKA_SASL_PASSWORD", "secret-value"));

        assertEquals(3, config.parallelism());
        assertEquals(5_000L, config.allowedLatenessMs());
        assertEquals(true, config.bounded());
        assertEquals(0L, config.contextBootstrapWaitMs());
        assertEquals("SASL_SSL", config.kafkaProperties().getProperty("security.protocol"));
        assertFalse(config.safeSummary().contains("secret-value"));
    }

    @Test
    void rejectsNegativeTimingPolicy() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {"--allowed-lateness-ms", "-1"}, Map.of()));
    }

    @Test
    void parsesJdbcActiveUserCacheWithoutSerializingCredentials() {
        ContextJobConfig config = ContextJobConfig.parse(
                new String[] {
                    "--context-source", "jdbc",
                    "--context-cache-ttl-hours", "36",
                    "--context-refresh-minutes", "60"
                },
                Map.of(
                        "USER_CONTEXT_JDBC_URL", "jdbc:postgresql://db.example/poker"));

        assertEquals("jdbc", config.contextSource());
        assertEquals("poker.hand-player-context.v2", config.outputTopic());
        assertEquals("flink-active-context-v2", config.groupId());
        assertEquals(36L, config.contextCacheTtlHours());
        assertEquals(60L, config.contextRefreshMinutes());
        assertFalse(config.safeSummary().contains("db.example"));
        assertFalse(Arrays.stream(config.getClass().getRecordComponents())
                .anyMatch(component -> component.getName().toLowerCase().contains("password")));
        assertFalse(Arrays.stream(config.getClass().getRecordComponents())
                .anyMatch(component -> component.getName().toLowerCase().contains("username")));
    }

    @Test
    void jdbcSourceRequiresConnectionConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {"--context-source", "jdbc"}, Map.of()));
    }

    @Test
    void parsesJdbcReliabilityAndFailureRateRestartLimits() {
        ContextJobConfig config = ContextJobConfig.parse(
                new String[] {
                    "--context-source", "jdbc",
                    "--context-jdbc-connect-timeout-seconds", "4",
                    "--context-jdbc-query-timeout-seconds", "2",
                    "--context-jdbc-validation-timeout-seconds", "3",
                    "--context-jdbc-retry-max-jitter-ms", "75",
                    "--restart-max-failures-per-interval", "5",
                    "--restart-failure-rate-interval-ms", "900000",
                    "--restart-delay-ms", "15000"
                },
                Map.of(
                        "USER_CONTEXT_JDBC_URL",
                        "jdbc:postgresql://db.example/poker"));

        assertEquals(4, config.contextJdbcConnectTimeoutSeconds());
        assertEquals(2, config.contextJdbcQueryTimeoutSeconds());
        assertEquals(3, config.contextJdbcValidationTimeoutSeconds());
        assertEquals(75L, config.contextJdbcRetryMaximumJitterMs());
        assertEquals(5, config.restartMaxFailuresPerInterval());
        assertEquals(900_000L, config.restartFailureRateIntervalMs());
        assertEquals(15_000L, config.restartDelayMs());
        assertFalse(config.safeSummary().contains("db.example"));
    }

    @Test
    void rejectsUnsafeJdbcReliabilityLimits() {
        Map<String, String> environment = Map.of(
                "USER_CONTEXT_JDBC_URL",
                "jdbc:postgresql://db.example/poker");

        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--context-jdbc-connect-timeout-seconds", "0"
                        },
                        environment));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--context-jdbc-retry-max-jitter-ms", "5001"
                        },
                        environment));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--context-jdbc-retry-max-jitter-ms", "-1"
                        },
                        environment));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--restart-max-failures-per-interval", "0"
                        },
                        environment));
    }

    @Test
    void rejectsJdbcCredentialsInCommandLineArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--context-jdbc-password", "must-not-enter-job-graph"
                        },
                        Map.of("USER_CONTEXT_JDBC_URL", "jdbc:postgresql://db/poker")));
    }

    @Test
    void simulationModeRequiresExactIsolatedBoundary() {
        ContextJobConfig config = ContextJobConfig.parse(
                new String[] {"--simulation-mode"},
                Map.of(
                        "KAFKA_WORLD_HANDS_TOPIC", "poker.sim.hands.raw.v1",
                        "KAFKA_USER_CONTEXT_TOPIC", "poker.sim.user-context.v1",
                        "KAFKA_PLAYER_CONTEXT_TOPIC", "poker.sim.hand-player-context.v1",
                        "KAFKA_DEAD_LETTER_TOPIC", "poker.sim.pipeline.dead-letter.v1",
                        "FLINK_CONTEXT_GROUP_ID", "flink-legacy-kafka-context-sim-v1",
                        "FLINK_CONTEXT_IDLE_SOURCE_TIMEOUT_MS", "5000"));
        assertEquals(true, config.simulationMode());
        assertEquals(5_000L, config.idleSourceTimeoutMs());

        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[] {"--simulation-mode"},
                        Map.of("KAFKA_WORLD_HANDS_TOPIC", "poker.hands.raw.v1")));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextJobConfig.parse(
                        new String[0],
                        Map.of("KAFKA_WORLD_HANDS_TOPIC", "poker.sim.hands.raw.v1")));
    }

    @Test
    void jdbcSimulationUsesItsOwnV2TopicAndGroup() {
        ContextJobConfig config = ContextJobConfig.parse(
                new String[] {"--simulation-mode"},
                Map.of(
                        "FLINK_CONTEXT_SOURCE", "jdbc",
                        "USER_CONTEXT_JDBC_URL", "jdbc:postgresql://db/poker",
                        "KAFKA_WORLD_HANDS_TOPIC", "poker.sim.hands.raw.v1",
                        "KAFKA_PLAYER_CONTEXT_V2_TOPIC",
                                "poker.sim.hand-player-context.v2",
                        "KAFKA_DEAD_LETTER_TOPIC", "poker.sim.pipeline.dead-letter.v1",
                        "FLINK_ACTIVE_CONTEXT_GROUP_ID",
                                "flink-active-context-sim-v2"));

        assertEquals("poker.sim.hand-player-context.v2", config.outputTopic());
        assertEquals("flink-active-context-sim-v2", config.groupId());
    }
}
