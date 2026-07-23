package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.Map;
import org.junit.jupiter.api.Test;

final class JobConfigTest {
    @Test
    void parsesOperationalOptionsWithoutPrintingSecrets() {
        ContextEnrichmentJob.JobConfig config = ContextEnrichmentJob.JobConfig.parse(
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
                () -> ContextEnrichmentJob.JobConfig.parse(
                        new String[] {"--allowed-lateness-ms", "-1"}, Map.of()));
    }

    @Test
    void parsesJdbcActiveUserCacheWithoutSerializingCredentials() {
        ContextEnrichmentJob.JobConfig config = ContextEnrichmentJob.JobConfig.parse(
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
                () -> ContextEnrichmentJob.JobConfig.parse(
                        new String[] {"--context-source", "jdbc"}, Map.of()));
    }

    @Test
    void rejectsJdbcCredentialsInCommandLineArguments() {
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextEnrichmentJob.JobConfig.parse(
                        new String[] {
                            "--context-source", "jdbc",
                            "--context-jdbc-password", "must-not-enter-job-graph"
                        },
                        Map.of("USER_CONTEXT_JDBC_URL", "jdbc:postgresql://db/poker")));
    }

    @Test
    void simulationModeRequiresExactIsolatedBoundary() {
        ContextEnrichmentJob.JobConfig config = ContextEnrichmentJob.JobConfig.parse(
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
                () -> ContextEnrichmentJob.JobConfig.parse(
                        new String[] {"--simulation-mode"},
                        Map.of("KAFKA_WORLD_HANDS_TOPIC", "poker.hands.raw.v1")));
        assertThrows(
                IllegalArgumentException.class,
                () -> ContextEnrichmentJob.JobConfig.parse(
                        new String[0],
                        Map.of("KAFKA_WORLD_HANDS_TOPIC", "poker.sim.hands.raw.v1")));
    }

    @Test
    void jdbcSimulationUsesItsOwnV2TopicAndGroup() {
        ContextEnrichmentJob.JobConfig config = ContextEnrichmentJob.JobConfig.parse(
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
