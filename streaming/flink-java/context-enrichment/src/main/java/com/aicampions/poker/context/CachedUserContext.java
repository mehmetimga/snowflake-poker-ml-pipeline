package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** JSON representation stored in Flink ValueState for one active player. */
final class CachedUserContext {
    private CachedUserContext() {}

    static String create(String contextEvent, long loadedAtMs) {
        ObjectNode cached = EventJson.MAPPER.createObjectNode();
        cached.put("loaded_at_ms", loadedAtMs);
        cached.set("event", EventJson.parse(contextEvent));
        return EventJson.compact(cached);
    }

    static String event(String cached) {
        return EventJson.compact(EventJson.parse(cached).path("event"));
    }

    static boolean isFresh(String cached, long nowMs, long refreshAfterMs) {
        long loadedAt = EventJson.parse(cached).path("loaded_at_ms").asLong(-1L);
        return loadedAt >= 0L && nowMs >= loadedAt && nowMs - loadedAt < refreshAfterMs;
    }

    static boolean isEffectiveFor(String cached, long playedAtMs) {
        JsonNode payload = EventJson.parse(cached).path("event").path("payload");
        return EventJson.parseInstant(EventJson.requireText(payload, "effective_at")) <= playedAtMs;
    }
}
