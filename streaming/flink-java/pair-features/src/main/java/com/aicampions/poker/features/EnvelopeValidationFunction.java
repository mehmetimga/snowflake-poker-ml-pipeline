package com.aicampions.poker.features;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

final class EnvelopeValidationFunction extends ProcessFunction<String, String> {
    private final String sourceTopic;
    private final boolean simulationMode;
    private final int inputSchemaVersion;

    EnvelopeValidationFunction(
            String sourceTopic, boolean simulationMode, int inputSchemaVersion) {
        this.sourceTopic = sourceTopic;
        this.simulationMode = simulationMode;
        this.inputSchemaVersion = inputSchemaVersion;
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            PairEventJson.validateEnriched(value, inputSchemaVersion);
            if (simulationMode
                    && !PairEventJson.requireText(PairEventJson.parse(value), "dataset_id")
                            .startsWith("sim-")) {
                throw new IllegalArgumentException("dataset_id must start with sim-");
            }
            output.collect(value);
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(
                            sourceTopic, "validate-enriched-player-hand", error.getMessage(), value));
        }
    }
}
