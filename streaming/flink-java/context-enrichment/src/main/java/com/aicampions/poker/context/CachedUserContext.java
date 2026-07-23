package com.aicampions.poker.context;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Versioned PostgreSQL snapshot stored in Flink ValueState for one active player. */
final class CachedUserContext {
    private CachedUserContext() {}

    static String create(UserContextRecord record, long loadedAtMs) {
        ObjectNode cached = EventJson.MAPPER.createObjectNode();
        cached.put("cache_schema_version", 2);
        cached.put("loaded_at_ms", loadedAtMs);
        cached.put("context_record_id", record.contextRecordId().toString());
        cached.set("context", record.toPayload());
        return EventJson.compact(cached);
    }

    static JsonNode context(String cached) {
        return EventJson.parse(cached).path("context");
    }

    static String contextRecordId(String cached) {
        return EventJson.requireText(EventJson.parse(cached), "context_record_id");
    }

    static boolean isFresh(String cached, long nowMs, long refreshAfterMs) {
        long loadedAt = EventJson.parse(cached).path("loaded_at_ms").asLong(-1L);
        return loadedAt >= 0L && nowMs >= loadedAt && nowMs - loadedAt < refreshAfterMs;
    }

    static boolean isEffectiveFor(String cached, long playedAtMs) {
        JsonNode payload = context(cached);
        return EventJson.parseInstant(EventJson.requireText(payload, "effective_at")) <= playedAtMs;
    }
}
