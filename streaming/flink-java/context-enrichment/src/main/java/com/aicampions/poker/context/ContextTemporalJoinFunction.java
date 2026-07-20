package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.util.Collector;

/** Keyed event-time join of hand/player rows and versioned user context. */
final class ContextTemporalJoinFunction
        extends KeyedCoProcessFunction<String, String, String, String> {
    private final long allowedLatenessMs;
    private final long correctionWindowMs;
    private final long stateTtlHours;
    private final long contextBootstrapWaitMs;
    private final String contextTopic;

    private transient MapState<String, String> contextsByEventId;
    private transient MapState<String, String> playerHandsByEventId;
    private transient ValueState<Long> arrivalSequence;
    private transient long bootstrapReadyAtMs;

    ContextTemporalJoinFunction(
            long allowedLatenessMs,
            long correctionWindowMs,
            long stateTtlHours,
            long contextBootstrapWaitMs,
            String contextTopic) {
        this.allowedLatenessMs = allowedLatenessMs;
        this.correctionWindowMs = correctionWindowMs;
        this.stateTtlHours = stateTtlHours;
        this.contextBootstrapWaitMs = contextBootstrapWaitMs;
        this.contextTopic = contextTopic;
    }

    @Override
    public void open(Configuration parameters) {
        bootstrapReadyAtMs = Math.addExact(System.currentTimeMillis(), contextBootstrapWaitMs);
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(stateTtlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        MapStateDescriptor<String, String> contextDescriptor = new MapStateDescriptor<>(
                "user-context-by-event-id", Types.STRING, Types.STRING);
        contextDescriptor.enableTimeToLive(ttl);
        contextsByEventId = getRuntimeContext().getMapState(contextDescriptor);

        MapStateDescriptor<String, String> handDescriptor = new MapStateDescriptor<>(
                "player-hands-by-event-id", Types.STRING, Types.STRING);
        handDescriptor.enableTimeToLive(ttl);
        playerHandsByEventId = getRuntimeContext().getMapState(handDescriptor);

        ValueStateDescriptor<Long> sequenceDescriptor = new ValueStateDescriptor<>(
                "arrival-sequence", Types.LONG);
        sequenceDescriptor.enableTimeToLive(ttl);
        arrivalSequence = getRuntimeContext().getState(sequenceDescriptor);
    }

    @Override
    public void processElement1(String expandedHand, Context context, Collector<String> output)
            throws Exception {
        JsonNode hand = EventJson.parse(expandedHand).path("hand");
        String handEventId = EventJson.requireText(hand, "event_id");
        if (playerHandsByEventId.contains(handEventId)) {
            return;
        }
        long sequence = nextSequence();
        long processingTime = context.timerService().currentProcessingTime();
        boolean bootstrapFallback = processingTime < bootstrapReadyAtMs;
        String state = TemporalJoinLogic.newPlayerHandState(
                expandedHand,
                sequence,
                allowedLatenessMs,
                correctionWindowMs,
                bootstrapReadyAtMs,
                bootstrapFallback);
        playerHandsByEventId.put(handEventId, state);
        long dueAt = TemporalJoinLogic.dueAtMs(state);
        long cleanupAt = TemporalJoinLogic.cleanupAtMs(state);
        context.timerService().registerEventTimeTimer(dueAt);
        context.timerService().registerEventTimeTimer(cleanupAt);
        long watermark = context.timerService().currentWatermark();
        if (watermark >= dueAt) {
            state = TemporalJoinLogic.markEventTime(state, true, watermark >= cleanupAt);
            playerHandsByEventId.put(handEventId, state);
        }
        if (processingTime < bootstrapReadyAtMs) {
            context.timerService().registerProcessingTimeTimer(bootstrapReadyAtMs);
        } else if (TemporalJoinLogic.eventTimeDue(state)) {
            emitInitial(handEventId, state, processingTime, output);
        }
    }

    @Override
    public void processElement2(String contextEvent, Context context, Collector<String> output)
            throws Exception {
        JsonNode event = EventJson.parse(contextEvent);
        String eventId = EventJson.requireText(event, "event_id");
        if (contextsByEventId.contains(eventId)) {
            return;
        }
        int newVersion = event.path("payload").path("context_version").asInt();
        for (String existing : contextsByEventId.values()) {
            if (TemporalJoinLogic.contextVersion(existing) == newVersion) {
                context.output(
                        DeadLetters.TAG,
                        EventJson.deadLetter(
                                contextTopic,
                                "context-version-conflict",
                                "duplicate context_version with a different event_id",
                                contextEvent));
                return;
            }
        }

        String wrappedContext = TemporalJoinLogic.wrapContext(contextEvent, nextSequence());
        contextsByEventId.put(eventId, wrappedContext);
        long watermark = context.timerService().currentWatermark();
        for (Map.Entry<String, String> entry : snapshotHands()) {
            String state = entry.getValue();
            if (!TemporalJoinLogic.emitted(state) || TemporalJoinLogic.cleanupDue(state)) {
                continue;
            }
            String selected = selectContext(state);
            if (selected == null) {
                continue;
            }
            String selectedId = TemporalJoinLogic.contextEventId(selected);
            if (selectedId.equals(TemporalJoinLogic.selectedContextEventId(state))) {
                continue;
            }
            int revision = TemporalJoinLogic.revision(state) + 1;
            output.collect(TemporalJoinLogic.enrich(
                    state,
                    selected,
                    "corrected",
                    revision,
                    allowedLatenessMs,
                    correctionWindowMs,
                    System.currentTimeMillis()));
            playerHandsByEventId.put(
                    entry.getKey(),
                    TemporalJoinLogic.updateEmissionState(state, revision, selectedId));
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext context, Collector<String> output)
            throws Exception {
        for (Map.Entry<String, String> entry : snapshotHands()) {
            String state = entry.getValue();
            long processingTime = context.timerService().currentProcessingTime();
            if (context.timeDomain() == TimeDomain.EVENT_TIME) {
                state = TemporalJoinLogic.markEventTime(
                        state,
                        TemporalJoinLogic.dueAtMs(state) <= timestamp,
                        TemporalJoinLogic.cleanupAtMs(state) <= timestamp);
                playerHandsByEventId.put(entry.getKey(), state);
            } else if (context.timeDomain() == TimeDomain.PROCESSING_TIME
                    && TemporalJoinLogic.bootstrapFallback(state)
                    && processingTime >= TemporalJoinLogic.notBeforeProcessingMs(state)) {
                state = TemporalJoinLogic.markEventTime(state, true, false);
                playerHandsByEventId.put(entry.getKey(), state);
            }
            boolean eventTimeDue = TemporalJoinLogic.eventTimeDue(state);
            boolean bootstrapReady = processingTime
                    >= TemporalJoinLogic.notBeforeProcessingMs(state);
            if (!TemporalJoinLogic.emitted(state) && eventTimeDue && bootstrapReady) {
                emitInitial(entry.getKey(), state, processingTime, output);
                state = playerHandsByEventId.get(entry.getKey());
            }
            if (state != null
                    && TemporalJoinLogic.emitted(state)
                    && TemporalJoinLogic.cleanupDue(state)) {
                playerHandsByEventId.remove(entry.getKey());
            } else if (state != null
                    && !TemporalJoinLogic.emitted(state)
                    && context.timeDomain() == TimeDomain.EVENT_TIME
                    && !bootstrapReady) {
                context.timerService().registerProcessingTimeTimer(
                        TemporalJoinLogic.notBeforeProcessingMs(state));
            }
        }
    }

    private void emitInitial(
            String handEventId, String state, long emittedAtMs, Collector<String> output)
            throws Exception {
        String selected = selectContext(state);
        String selectedId = selected == null ? null : TemporalJoinLogic.contextEventId(selected);
        String status;
        if (selected == null) {
            status = "missing";
        } else if (TemporalJoinLogic.contextArrivalSequence(selected)
                > TemporalJoinLogic.arrivalSequence(state)) {
            status = "matched_late";
        } else {
            status = "matched";
        }
        output.collect(TemporalJoinLogic.enrich(
                state,
                selected,
                status,
                1,
                allowedLatenessMs,
                correctionWindowMs,
                emittedAtMs));
        playerHandsByEventId.put(
                handEventId, TemporalJoinLogic.updateEmissionState(state, 1, selectedId));
    }

    private String selectContext(String state) throws Exception {
        return TemporalJoinLogic.selectContext(
                contextsByEventId.values(), TemporalJoinLogic.playedAtMs(state));
    }

    private long nextSequence() throws Exception {
        Long current = arrivalSequence.value();
        long next = current == null ? 1L : Math.addExact(current, 1L);
        arrivalSequence.update(next);
        return next;
    }

    private List<Map.Entry<String, String>> snapshotHands() throws Exception {
        List<Map.Entry<String, String>> snapshot = new ArrayList<>();
        for (Map.Entry<String, String> entry : playerHandsByEventId.entries()) {
            snapshot.add(Map.entry(entry.getKey(), entry.getValue()));
        }
        return snapshot;
    }
}
