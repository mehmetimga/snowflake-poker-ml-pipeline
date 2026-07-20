package com.aicampions.poker.context;

import org.apache.flink.util.OutputTag;

final class DeadLetters {
    static final OutputTag<String> TAG = new OutputTag<String>("context-enrichment-dead-letters") {};

    private DeadLetters() {}
}
