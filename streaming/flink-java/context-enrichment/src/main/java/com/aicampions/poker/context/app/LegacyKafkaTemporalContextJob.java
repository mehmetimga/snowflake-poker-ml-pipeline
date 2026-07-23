package com.aicampions.poker.context.app;

import com.aicampions.poker.context.ContextEnrichmentJob;

/** Rollback-only two-input Kafka temporal-join entrypoint. */
public final class LegacyKafkaTemporalContextJob {
    private LegacyKafkaTemporalContextJob() {}

    public static void main(String[] arguments) throws Exception {
        ContextEnrichmentJob.runLegacy(arguments);
    }
}
