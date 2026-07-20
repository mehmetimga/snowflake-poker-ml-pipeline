package com.aicampions.poker.features;

import org.apache.flink.util.OutputTag;

final class DeadLetters {
    static final OutputTag<String> TAG = new OutputTag<String>("pair-feature-dead-letters") {};

    private DeadLetters() {}
}
