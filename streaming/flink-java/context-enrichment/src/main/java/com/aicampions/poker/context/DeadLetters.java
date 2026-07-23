package com.aicampions.poker.context;

import org.apache.flink.util.OutputTag;

public final class DeadLetters {
    public static final OutputTag<String> TAG =
            new OutputTag<String>("context-enrichment-dead-letters") {};

    private DeadLetters() {}
}
