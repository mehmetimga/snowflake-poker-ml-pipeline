package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

final class UserRollingFunction extends KeyedProcessFunction<String, String, String> {
    private final long stateTtlHours;
    private final String sourceTopic;
    private transient ValueState<String> aggregateState;
    private transient MapState<String, String> historyByHand;

    UserRollingFunction(long stateTtlHours, String sourceTopic) {
        this.stateTtlHours = stateTtlHours;
        this.sourceTopic = sourceTopic;
    }

    @Override
    public void open(Configuration parameters) {
        aggregateState = getRuntimeContext().getState(
                StateDescriptors.stringValue("pair-features-user-aggregate-v1", stateTtlHours));
        historyByHand = getRuntimeContext().getMapState(
                StateDescriptors.stringMap("pair-features-user-hand-history-v1", stateTtlHours));
    }

    @Override
    public void processElement(String value, Context context, Collector<String> output) {
        try {
            JsonNode event = PairEventJson.parse(value);
            String handId = PairEventJson.requireText(event.path("payload"), "hand_id");
            String historyJson = historyByHand.get(handId);
            if (historyJson == null) {
                ObjectNode aggregate = aggregateState.value() == null
                        ? PairFeatureMath.emptyUserState()
                        : (ObjectNode) PairEventJson.parse(aggregateState.value());
                ObjectNode history = PairFeatureMath.userSnapshot(aggregate);
                historyJson = PairEventJson.compact(history);
                historyByHand.put(handId, historyJson);
                PairFeatureMath.updateUser(aggregate, event);
                aggregateState.update(PairEventJson.compact(aggregate));
            }
            output.collect(PairEventJson.augmentUser(value, PairEventJson.parse(historyJson)));
        } catch (RuntimeException error) {
            context.output(
                    DeadLetters.TAG,
                    PairEventJson.deadLetter(sourceTopic, "rolling-user-features", error.getMessage(), value));
        } catch (Exception error) {
            throw new RuntimeException(error);
        }
    }
}
