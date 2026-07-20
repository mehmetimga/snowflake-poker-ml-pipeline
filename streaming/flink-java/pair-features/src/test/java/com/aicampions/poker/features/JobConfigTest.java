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
                    "--state-ttl-hours", "24"
                },
                Map.of());

        assertEquals("enriched.test", config.inputTopic());
        assertEquals("features.test", config.outputTopic());
        assertTrue(config.fromBeginning());
        assertTrue(config.bounded());
        assertEquals(0, config.checkpointIntervalMs());
        assertEquals(24, config.stateTtlHours());
    }

    @Test
    void invalidTtlIsRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> PairFeaturesJob.JobConfig.parse(
                        new String[] {"--state-ttl-hours", "0"}, Map.of()));
    }
}
