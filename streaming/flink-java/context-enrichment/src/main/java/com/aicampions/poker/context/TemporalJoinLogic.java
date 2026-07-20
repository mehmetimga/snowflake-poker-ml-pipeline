package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;

/** Pure temporal-selection and output-building logic with no Flink dependency. */
final class TemporalJoinLogic {
    private static final UUID URL_NAMESPACE =
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");

    private TemporalJoinLogic() {}

    static String newPlayerHandState(
            String expandedHand,
            long arrivalSequence,
            long allowedLatenessMs,
            long correctionWindowMs,
            long notBeforeProcessingMs,
            boolean bootstrapFallback) {
        JsonNode expanded = EventJson.parse(expandedHand);
        JsonNode hand = expanded.path("hand");
        String playerId = EventJson.requireText(expanded, "player_id");
        long playedAtMs = EventJson.parseInstant(
                EventJson.requireText(hand.path("payload"), "played_at"));
        ObjectNode state = EventJson.MAPPER.createObjectNode();
        state.set("hand", hand.deepCopy());
        state.put("player_id", playerId);
        state.put("arrival_sequence", arrivalSequence);
        state.put("due_at_ms", Math.addExact(playedAtMs, allowedLatenessMs));
        state.put(
                "cleanup_at_ms",
                Math.addExact(Math.addExact(playedAtMs, allowedLatenessMs), correctionWindowMs));
        state.put("revision", 0);
        state.put("not_before_processing_ms", notBeforeProcessingMs);
        state.put("bootstrap_fallback", bootstrapFallback);
        state.put("event_time_due", false);
        state.put("cleanup_due", false);
        state.putNull("selected_context_event_id");
        state.put("emitted", false);
        return EventJson.compact(state);
    }

    static String wrapContext(String contextEvent, long arrivalSequence) {
        ObjectNode wrapped = EventJson.MAPPER.createObjectNode();
        wrapped.set("event", EventJson.parse(contextEvent));
        wrapped.put("arrival_sequence", arrivalSequence);
        return EventJson.compact(wrapped);
    }

    static String selectContext(Iterable<String> wrappedContexts, long playedAtMs) {
        List<JsonNode> candidates = new ArrayList<>();
        for (String value : wrappedContexts) {
            JsonNode wrapped = EventJson.parse(value);
            JsonNode payload = wrapped.path("event").path("payload");
            long effectiveAt = EventJson.parseInstant(
                    EventJson.requireText(payload, "effective_at"));
            if (effectiveAt <= playedAtMs) {
                candidates.add(wrapped);
            }
        }
        return candidates.stream()
                .max(Comparator
                        .comparingLong(TemporalJoinLogic::effectiveAt)
                        .thenComparingInt(TemporalJoinLogic::contextVersion)
                        .thenComparing(TemporalJoinLogic::contextEventId))
                .map(EventJson::compact)
                .orElse(null);
    }

    static String enrich(
            String stateJson,
            String wrappedContext,
            String status,
            int revision,
            long allowedLatenessMs,
            long correctionWindowMs,
            long emittedAtMs) {
        JsonNode state = EventJson.parse(stateJson);
        JsonNode hand = state.path("hand");
        JsonNode handPayload = hand.path("payload");
        String playerId = EventJson.requireText(state, "player_id");
        JsonNode player = findPlayer(handPayload.path("players"), playerId);
        JsonNode contextEvent = wrappedContext == null
                ? null
                : EventJson.parse(wrappedContext).path("event");
        JsonNode contextPayload = contextEvent == null ? null : contextEvent.path("payload");

        ObjectNode payload = EventJson.MAPPER.createObjectNode();
        copy(payload, handPayload, "hand_id");
        copy(payload, handPayload, "table_id");
        copy(payload, handPayload, "played_at");
        payload.set("player", player.deepCopy());
        payload.set("actions", handPayload.path("actions").deepCopy());
        payload.set("board", handPayload.path("board").deepCopy());
        copy(payload, handPayload, "small_blind");
        copy(payload, handPayload, "big_blind");
        copy(payload, handPayload, "num_players");
        copy(payload, handPayload, "pot_size");
        payload.put("source_hand_event_id", EventJson.requireText(hand, "event_id"));
        payload.put("context_status", status);
        if (contextPayload == null) {
            payload.putNull("context_version");
            payload.putNull("context_effective_at");
            payload.putNull("source_context_event_id");
            payload.putNull("context");
        } else {
            payload.set("context_version", contextPayload.path("context_version").deepCopy());
            payload.set("context_effective_at", contextPayload.path("effective_at").deepCopy());
            payload.put("source_context_event_id", EventJson.requireText(contextEvent, "event_id"));
            payload.set("context", contextPayload.deepCopy());
        }
        payload.put("revision", revision);
        payload.put("allowed_lateness_ms", allowedLatenessMs);
        payload.put("correction_window_ms", correctionWindowMs);
        payload.put("join_policy_version", EventJson.JOIN_POLICY);

        String derivedName = String.join(
                ":",
                EventJson.requireText(hand, "dataset_id"),
                EventJson.requireText(hand, "dataset_split"),
                EventJson.ENRICHED,
                EventJson.requireText(hand, "event_id"),
                playerId,
                Integer.toString(revision));
        ObjectNode output = EventJson.MAPPER.createObjectNode();
        output.put("event_id", uuid5(URL_NAMESPACE, derivedName).toString());
        output.put("event_type", EventJson.ENRICHED);
        output.put("schema_version", 1);
        copy(output, hand, "tenant_id");
        copy(output, hand, "product_id");
        copy(output, hand, "dataset_id");
        copy(output, hand, "dataset_split");
        copy(output, hand, "occurred_at");
        output.put("emitted_at", Instant.ofEpochMilli(emittedAtMs).toString());
        copy(output, hand, "trace_id");
        output.set("payload", payload);
        return EventJson.compact(output);
    }

    static String updateEmissionState(
            String stateJson, int revision, String selectedContextEventId) {
        ObjectNode state = (ObjectNode) EventJson.parse(stateJson);
        state.put("emitted", true);
        state.put("revision", revision);
        if (selectedContextEventId == null) {
            state.putNull("selected_context_event_id");
        } else {
            state.put("selected_context_event_id", selectedContextEventId);
        }
        return EventJson.compact(state);
    }

    static long playedAtMs(String stateJson) {
        JsonNode state = EventJson.parse(stateJson);
        return EventJson.parseInstant(
                EventJson.requireText(state.path("hand").path("payload"), "played_at"));
    }

    static long dueAtMs(String stateJson) {
        return EventJson.parse(stateJson).path("due_at_ms").asLong();
    }

    static long cleanupAtMs(String stateJson) {
        return EventJson.parse(stateJson).path("cleanup_at_ms").asLong();
    }

    static long arrivalSequence(String stateJson) {
        return EventJson.parse(stateJson).path("arrival_sequence").asLong();
    }

    static long notBeforeProcessingMs(String stateJson) {
        return EventJson.parse(stateJson).path("not_before_processing_ms").asLong();
    }

    static boolean emitted(String stateJson) {
        return EventJson.parse(stateJson).path("emitted").asBoolean();
    }

    static boolean eventTimeDue(String stateJson) {
        return EventJson.parse(stateJson).path("event_time_due").asBoolean();
    }

    static boolean cleanupDue(String stateJson) {
        return EventJson.parse(stateJson).path("cleanup_due").asBoolean();
    }

    static boolean bootstrapFallback(String stateJson) {
        return EventJson.parse(stateJson).path("bootstrap_fallback").asBoolean();
    }

    static String markEventTime(String stateJson, boolean due, boolean cleanupDue) {
        ObjectNode state = (ObjectNode) EventJson.parse(stateJson);
        if (due) {
            state.put("event_time_due", true);
        }
        if (cleanupDue) {
            state.put("cleanup_due", true);
        }
        return EventJson.compact(state);
    }

    static int revision(String stateJson) {
        return EventJson.parse(stateJson).path("revision").asInt();
    }

    static String selectedContextEventId(String stateJson) {
        JsonNode value = EventJson.parse(stateJson).path("selected_context_event_id");
        return value.isTextual() ? value.textValue() : null;
    }

    static String contextEventId(String wrappedContext) {
        return EventJson.requireText(EventJson.parse(wrappedContext).path("event"), "event_id");
    }

    static int contextVersion(String wrappedContext) {
        return EventJson.parse(wrappedContext)
                .path("event").path("payload").path("context_version").asInt();
    }

    static long contextArrivalSequence(String wrappedContext) {
        return EventJson.parse(wrappedContext).path("arrival_sequence").asLong();
    }

    private static long effectiveAt(JsonNode wrappedContext) {
        return EventJson.parseInstant(EventJson.requireText(
                wrappedContext.path("event").path("payload"), "effective_at"));
    }

    private static int contextVersion(JsonNode wrappedContext) {
        return wrappedContext.path("event").path("payload").path("context_version").asInt();
    }

    private static String contextEventId(JsonNode wrappedContext) {
        return EventJson.requireText(wrappedContext.path("event"), "event_id");
    }

    private static JsonNode findPlayer(JsonNode players, String playerId) {
        for (JsonNode player : players) {
            if (playerId.equals(player.path("player_id").asText())) {
                return player;
            }
        }
        throw new IllegalArgumentException("hand does not contain player " + playerId);
    }

    private static void copy(ObjectNode target, JsonNode source, String field) {
        JsonNode value = source.get(field);
        if (value == null) {
            throw new IllegalArgumentException("missing required field: " + field);
        }
        target.set(field, value.deepCopy());
    }

    static UUID uuid5(UUID namespace, String name) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            ByteBuffer namespaceBytes = ByteBuffer.allocate(16);
            namespaceBytes.putLong(namespace.getMostSignificantBits());
            namespaceBytes.putLong(namespace.getLeastSignificantBits());
            sha1.update(namespaceBytes.array());
            byte[] digest = sha1.digest(name.getBytes(StandardCharsets.UTF_8));
            digest[6] = (byte) ((digest[6] & 0x0f) | 0x50);
            digest[8] = (byte) ((digest[8] & 0x3f) | 0x80);
            ByteBuffer uuidBytes = ByteBuffer.wrap(digest, 0, 16);
            return new UUID(uuidBytes.getLong(), uuidBytes.getLong());
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-1 is unavailable", error);
        }
    }
}
