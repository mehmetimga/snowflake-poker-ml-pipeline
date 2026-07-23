package com.aicampions.poker.context.contract;

import com.aicampions.poker.context.EventJson;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/** Versioned PostgreSQL snapshot stored in Flink ValueState for one active player. */
public final class CachedUserContext {
    private CachedUserContext() {}

    public static String create(UserContextRecord record, long loadedAtMs) {
        ObjectNode cached = EventJson.MAPPER.createObjectNode();
        cached.put("cache_schema_version", 2);
        cached.put("loaded_at_ms", loadedAtMs);
        cached.put("context_record_id", record.contextRecordId().toString());
        ObjectNode payload = cached.putObject("context");
        payload.put("user_id", record.userId());
        payload.put("context_version", record.contextVersion());
        payload.put("effective_at", record.effectiveAt().toString());
        payload.put("account_created_at", record.accountCreatedAt().toString());
        payload.put("country_bucket", record.countryBucket());
        payload.put("timezone", record.timezone());
        payload.put("acquisition_channel", record.acquisitionChannel());
        payload.put("kyc_level", record.kycLevel());
        payload.put("account_status", record.accountStatus());
        payload.put("bankroll_bucket", record.bankrollBucket());
        payload.put("preferred_stake_bucket", record.preferredStakeBucket());
        payload.put("skill_rating", record.skillRating());
        payload.put("device_id", record.deviceId());
        payload.put("network_cluster_id", record.networkClusterId());
        return EventJson.compact(cached);
    }

    public static JsonNode context(String cached) {
        return EventJson.parse(cached).path("context");
    }

    public static String contextRecordId(String cached) {
        return EventJson.requireText(EventJson.parse(cached), "context_record_id");
    }

    public static boolean isFresh(String cached, long nowMs, long refreshAfterMs) {
        long loadedAt = EventJson.parse(cached).path("loaded_at_ms").asLong(-1L);
        return loadedAt >= 0L && nowMs >= loadedAt && nowMs - loadedAt < refreshAfterMs;
    }

    public static boolean isEffectiveFor(String cached, long playedAtMs) {
        JsonNode payload = context(cached);
        return EventJson.parseInstant(EventJson.requireText(payload, "effective_at")) <= playedAtMs;
    }
}
