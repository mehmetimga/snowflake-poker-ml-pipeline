package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

final class HandPlayerExpander implements FlatMapFunction<String, String> {
    @Override
    public void flatMap(String value, Collector<String> output) {
        JsonNode hand = EventJson.parse(value);
        for (JsonNode player : hand.path("payload").path("players")) {
            output.collect(EventJson.expandPlayer(hand, player));
        }
    }
}
