package com.aicampions.poker.features;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

final class EnvelopeValidationFunction extends ProcessFunction<String, String> {
    private final String sourceTopic;

    EnvelopeValidationFunction(String sourceTopic) {
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            PairEventJson.validateEnriched(value);
            output.collect(value);
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(
                            sourceTopic, "validate-enriched-player-hand", error.getMessage(), value));
        }
    }
}
