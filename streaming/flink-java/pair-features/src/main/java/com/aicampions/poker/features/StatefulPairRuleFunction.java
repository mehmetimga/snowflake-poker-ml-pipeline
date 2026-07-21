package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Checkpointed keyed wrapper around the pure repeated-fold rule transition. */
final class StatefulPairRuleFunction extends KeyedProcessFunction<String, String, String> {
    private final long stateTtlHours;
    private final String sourceTopic;
    private final StatefulFoldRuleEngine.Config config;
    private transient ValueState<String> state;
    private transient Counter evaluations;
    private transient Counter firings;
    private transient Counter duplicates;
    private transient Counter corrections;
    private transient Counter stale;
    private transient Counter late;
    private transient int lastStateSize;
    private transient long lastEventTimeLagMs;

    StatefulPairRuleFunction(
            long stateTtlHours,
            String sourceTopic,
            StatefulFoldRuleEngine.Config config) {
        this.stateTtlHours = stateTtlHours;
        this.sourceTopic = sourceTopic;
        this.config = config;
    }

    @Override
    public void open(Configuration parameters) {
        state = getRuntimeContext().getState(
                StateDescriptors.stringValue("stateful-pair-rules-v1", stateTtlHours));
        evaluations = getRuntimeContext().getMetricGroup().counter("stateful_rule_evaluations_total");
        firings = getRuntimeContext().getMetricGroup().counter("stateful_rule_firings_total");
        duplicates = getRuntimeContext().getMetricGroup().counter("stateful_rule_duplicates_total");
        corrections = getRuntimeContext().getMetricGroup().counter("stateful_rule_corrections_total");
        stale = getRuntimeContext().getMetricGroup().counter("stateful_rule_stale_total");
        late = getRuntimeContext().getMetricGroup().counter("stateful_rule_late_total");
        getRuntimeContext().getMetricGroup().gauge(
                "stateful_rule_state_size", () -> lastStateSize);
        getRuntimeContext().getMetricGroup().gauge(
                "stateful_rule_event_time_lag_ms", () -> lastEventTimeLagMs);
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            JsonNode event = PairEventJson.parse(value);
            StatefulFoldRuleEngine.Evaluation result = StatefulFoldRuleEngine.evaluate(
                    state.value(), event, context.timerService().currentWatermark(), config);
            state.update(result.stateJson());
            evaluations.inc();
            lastStateSize = result.stateSize();
            long playedAtMs = PairEventJson.parseInstant(
                    PairEventJson.requireText(event.path("payload"), "played_at")).toEpochMilli();
            lastEventTimeLagMs = Math.max(0L, Instant.now().toEpochMilli() - playedAtMs);
            switch (result.status()) {
                case "duplicate" -> duplicates.inc();
                case "corrected" -> corrections.inc();
                case "stale" -> stale.inc();
                case "too_late", "too_late_correction" -> late.inc();
                default -> { }
            }
            if (result.evidenceEvent() != null) {
                firings.inc();
            }
            output.collect(PairEventJson.compact(
                    StatefulFoldRuleEngine.enrichPairEvent(event, result.evidenceEvent())));
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(
                            sourceTopic, "stateful-pair-rules", error.getMessage(), value));
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }
}
