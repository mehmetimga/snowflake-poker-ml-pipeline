package com.aicampions.poker.features;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

final class EnvelopeValidationFunction extends ProcessFunction<String, String> {
    private final String sourceTopic;
    private final boolean simulationMode;

    EnvelopeValidationFunction(String sourceTopic, boolean simulationMode) {
        this.sourceTopic = sourceTopic;
        this.simulationMode = simulationMode;
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            PairEventJson.validateEnriched(value);
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
