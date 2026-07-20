package com.aicampions.poker.context;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

final class EnvelopeValidationFunction extends ProcessFunction<String, String> {
    private final String sourceTopic;
    private final String requiredEventType;

    EnvelopeValidationFunction(String sourceTopic, String requiredEventType) {
        this.sourceTopic = sourceTopic;
        this.requiredEventType = requiredEventType;
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            EventJson.validateEnvelope(value, requiredEventType);
            output.collect(value);
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    EventJson.deadLetter(sourceTopic, "validation", safeMessage(error), value));
        }
    }

    private static String safeMessage(RuntimeException error) {
        String message = error.getMessage();
        return message == null || message.isBlank()
                ? error.getClass().getSimpleName()
                : message;
    }
}
