package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}
