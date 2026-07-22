package com.aicampions.poker.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import org.junit.jupiter.api.Test;

class JobConfigTest {
    @Test
    void boundedArgumentsAndTopicOverridesAreParsed() {
        PairFeaturesJob.JobConfig config = PairFeaturesJob.JobConfig.parse(
                new String[] {
                    "--input-topic", "enriched.test",
                    "--output-topic", "features.test",
                    "--from-beginning",
                    "--bounded",
                    "--checkpoint-interval-ms", "0",
                    "--state-ttl-hours", "24",
                    "--stateful-rule-window-hours", "12",
                    "--stateful-rule-rate-threshold", "0.75",
                    "--stateful-rule-enabled", "false",
                    "--stateful-rule-correction-horizon-hours", "24",
                    "--stateful-rule-state-ttl-hours", "36"
                },
                Map.of());

        assertEquals("enriched.test", config.inputTopic());
        assertEquals("features.test", config.outputTopic());
        assertTrue(config.fromBeginning());
        assertTrue(config.bounded());
        assertEquals(0, config.checkpointIntervalMs());
        assertEquals(24, config.stateTtlHours());
        assertEquals(12, config.statefulRuleWindowHours());
        assertEquals(0.75, config.statefulRuleRateThreshold());
        assertEquals(false, config.statefulRuleEnabled());
        assertEquals(24, config.statefulRuleCorrectionHorizonHours());
        assertEquals(36, config.statefulRuleStateTtlHours());
    }

    @Test
    void invalidTtlIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PairFeaturesJob.JobConfig.parse(
                        new String[] {"--state-ttl-hours", "0"}, Map.of()));
    }

    @Test
    void invalidRuleEnablementIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PairFeaturesJob.JobConfig.parse(
                        new String[] {"--stateful-rule-enabled", "maybe"}, Map.of()));
    }

    @Test
    void simulationModeRequiresExactIsolatedBoundary() {
        PairFeaturesJob.JobConfig config = PairFeaturesJob.JobConfig.parse(
                new String[] {"--simulation-mode"},
                Map.of(
                        "KAFKA_PLAYER_CONTEXT_TOPIC", "poker.sim.hand-player-context.v1",
                        "KAFKA_PAIR_FEATURES_TOPIC", "poker.sim.pair-features.v1",
                        "KAFKA_DEAD_LETTER_TOPIC", "poker.sim.pipeline.dead-letter.v1",
                        "FLINK_PAIR_FEATURES_GROUP_ID", "flink-pair-features-sim-v1",
                        "FLINK_PAIR_IDLE_SOURCE_TIMEOUT_MS", "5000"));
        assertTrue(config.simulationMode());
        assertEquals(5_000L, config.idleSourceTimeoutMs());

        assertThrows(
                IllegalArgumentException.class,
                () -> PairFeaturesJob.JobConfig.parse(
                        new String[] {"--simulation-mode"},
                        Map.of("KAFKA_PAIR_FEATURES_TOPIC", "poker.pair-features.v1")));
        assertThrows(
                IllegalArgumentException.class,
                () -> PairFeaturesJob.JobConfig.parse(
                        new String[0],
                        Map.of("KAFKA_PAIR_FEATURES_TOPIC", "poker.sim.pair-features.v1")));
    }
}
