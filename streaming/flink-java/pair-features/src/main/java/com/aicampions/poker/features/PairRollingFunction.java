package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class PairRollingFunction extends KeyedProcessFunction<String, String, String> {
    private final long stateTtlHours;
    private final String sourceTopic;
    private transient ValueState<String> aggregateState;
    private transient MapState<String, String> historyByHand;
    private transient MapState<String, String> pendingByHandRevision;

    PairRollingFunction(long stateTtlHours, String sourceTopic) {
        this.stateTtlHours = stateTtlHours;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) {
        aggregateState = getRuntimeContext().getState(
                StateDescriptors.stringValue("pair-features-pair-aggregate-v1", stateTtlHours));
        historyByHand = getRuntimeContext().getMapState(
                StateDescriptors.stringMap("pair-features-pair-hand-history-v1", stateTtlHours));
        pendingByHandRevision = getRuntimeContext().getMapState(
                StateDescriptors.stringMap("pair-features-pending-event-time-v1", stateTtlHours));
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            JsonNode observation = PairEventJson.parse(value);
            JsonNode eventA = observation.path("a");
            JsonNode payloadA = eventA.path("payload");
            String handId = PairEventJson.requireText(payloadA, "hand_id");
            if (historyByHand.contains(handId)) {
                emitObservation(observation, output);
                return;
            }
            int revision = observation.path("snapshot_revision").asInt();
            pendingByHandRevision.put(handId + ":" + revision, value);
            long playedAtMs = PairEventJson.parseInstant(
                    PairEventJson.requireText(payloadA, "played_at")).toEpochMilli();
            context.timerService().registerEventTimeTimer(playedAtMs);
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(sourceTopic, "rolling-pair-features", error.getMessage(), value));
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<String> output) {
        try {
            List<Pending> due = new ArrayList<>();
            for (var entry : pendingByHandRevision.entries()) {
                JsonNode observation = PairEventJson.parse(entry.getValue());
                long playedAtMs = PairEventJson.parseInstant(PairEventJson.requireText(
                        observation.path("a").path("payload"), "played_at")).toEpochMilli();
                if (playedAtMs <= timestamp) {
                    due.add(new Pending(
                            entry.getKey(),
                            playedAtMs,
                            PairEventJson.requireText(
                                    observation.path("a").path("payload"), "hand_id"),
                            observation.path("snapshot_revision").asInt(),
                            observation));
                }
            }
            due.sort(Comparator
                    .comparingLong(Pending::playedAtMs)
                    .thenComparing(Pending::handId)
                    .thenComparingInt(Pending::revision));
            for (Pending pending : due) {
                try {
                    emitObservation(pending.observation(), output);
                } catch (RuntimeException error) {
                    context.output(
                            DeadLetters.TAG,
                            PairEventJson.deadLetter(
                                    sourceTopic,
                                    "rolling-pair-event-time",
                                    error.getMessage(),
                                    PairEventJson.compact(pending.observation())));
                } finally {
                    pendingByHandRevision.remove(pending.stateKey());
                }
            }
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(
                            sourceTopic, "rolling-pair-event-time", error.getMessage(), "timer=" + timestamp));
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }

    private void emitObservation(JsonNode observation, Collector<String> output) throws Exception {
        JsonNode eventA = observation.path("a");
        JsonNode payloadA = eventA.path("payload");
        String handId = PairEventJson.requireText(payloadA, "hand_id");
        String historyJson = historyByHand.get(handId);
        if (historyJson == null) {
            ObjectNode aggregate = aggregateState.value() == null
                    ? PairFeatureMath.emptyPairState()
                    : (ObjectNode) PairEventJson.parse(aggregateState.value());
            String tableId = PairEventJson.requireText(payloadA, "table_id");
            Instant playedAt = PairEventJson.parseInstant(
                    PairEventJson.requireText(payloadA, "played_at"));
            ObjectNode history = PairFeatureMath.pairSnapshot(aggregate, tableId, playedAt);
            historyJson = PairEventJson.compact(history);
            historyByHand.put(handId, historyJson);
            PairFeatureMath.updatePair(aggregate, eventA, observation.path("b"));
            aggregateState.update(PairEventJson.compact(aggregate));
        }
        output.collect(PairFeatureMath.buildPairFeatureEvent(
                observation, PairEventJson.parse(historyJson)));
    }

    private record Pending(
            String stateKey,
            long playedAtMs,
            String handId,
            int revision,
            JsonNode observation) {}
}
