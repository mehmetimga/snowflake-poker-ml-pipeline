package com.aicampions.poker.context.contract;

import com.aicampions.poker.context.EventJson;
import com.aicampions.poker.context.domain.ActiveContextCacheEntry;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

/** Deterministic schema-v2 player-hand event with explicit PostgreSQL lineage. */
public final class JdbcEnrichedEventV2 {
    public static final String EVENT_TYPE = "poker.hand-player-context.enriched";
    public static final String RESOLUTION_MODE = "postgresql_point_in_time";
    public static final String RESOLUTION_POLICY = "jdbc-effective-at-v1";
    public static final String RESOLUTION_SOURCE = "postgresql";
    private static final UUID URL_NAMESPACE =
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");

    private JdbcEnrichedEventV2() {}

    public static String create(
            String expandedHand,
            ActiveContextCacheEntry contextEntry) {
        JsonNode expanded = EventJson.parse(expandedHand);
        JsonNode hand = expanded.path("hand");
        JsonNode handPayload = hand.path("payload");
        UserContextRecord record = contextEntry.toRecord();
        ObjectNode context = context(record);
        String playerId = EventJson.requireText(expanded, "player_id");
        if (!playerId.equals(record.userId())) {
            throw new IllegalArgumentException("context user does not match hand player");
        }
        long effectiveAt = record.effectiveAt().toEpochMilli();
        long playedAt = EventJson.parseInstant(
                EventJson.requireText(handPayload, "played_at"));
        if (effectiveAt > playedAt) {
            throw new IllegalArgumentException("future context cannot enrich an older hand");
        }

        String contextRecordId = contextEntry.getContextRecordId();
        ObjectNode payload = EventJson.MAPPER.createObjectNode();
        copy(payload, handPayload, "hand_id");
        copy(payload, handPayload, "table_id");
        copy(payload, handPayload, "played_at");
        payload.set("player", findPlayer(handPayload.path("players"), playerId).deepCopy());
        payload.set("actions", handPayload.path("actions").deepCopy());
        payload.set("board", handPayload.path("board").deepCopy());
        copy(payload, handPayload, "small_blind");
        copy(payload, handPayload, "big_blind");
        copy(payload, handPayload, "num_players");
        copy(payload, handPayload, "pot_size");
        payload.put("source_hand_event_id", EventJson.requireText(hand, "event_id"));
        payload.put("context_status", "matched");
        payload.set("context", context.deepCopy());
        payload.put("revision", 1);

        ObjectNode resolution = payload.putObject("context_resolution");
        resolution.put("mode", RESOLUTION_MODE);
        resolution.put("policy_version", RESOLUTION_POLICY);
        resolution.put("source", RESOLUTION_SOURCE);
        resolution.put("context_record_id", contextRecordId);
        resolution.set("context_version", context.path("context_version").deepCopy());
        resolution.set("context_effective_at", context.path("effective_at").deepCopy());

        String derivedName = String.join(
                ":",
                EventJson.requireText(hand, "dataset_id"),
                EventJson.requireText(hand, "dataset_split"),
                EVENT_TYPE,
                "schema-v2",
                EventJson.requireText(hand, "event_id"),
                EventJson.requireText(hand, "tenant_id"),
                EventJson.requireText(hand, "product_id"),
                playerId,
                contextRecordId);
        ObjectNode output = EventJson.MAPPER.createObjectNode();
        output.put("event_id", uuid5(URL_NAMESPACE, derivedName).toString());
        output.put("event_type", EVENT_TYPE);
        output.put("schema_version", 2);
        copy(output, hand, "tenant_id");
        copy(output, hand, "product_id");
        copy(output, hand, "dataset_id");
        copy(output, hand, "dataset_split");
        copy(output, hand, "occurred_at");
        // A deterministic logical emission time keeps replays byte-stable.
        copy(output, hand, "emitted_at");
        copy(output, hand, "trace_id");
        output.set("payload", payload);
        return EventJson.compact(output);
    }

    private static ObjectNode context(UserContextRecord record) {
        ObjectNode payload = EventJson.MAPPER.createObjectNode();
        payload.put("user_id", record.userId());
        payload.put("context_version", record.contextVersion());
        payload.put("effective_at", record.effectiveAt().toString());
        payload.put(
                "account_created_at",
                record.accountCreatedAt().toString());
        payload.put("country_bucket", record.countryBucket());
        payload.put("timezone", record.timezone());
        payload.put(
                "acquisition_channel",
                record.acquisitionChannel());
        payload.put("kyc_level", record.kycLevel());
        payload.put("account_status", record.accountStatus());
        payload.put("bankroll_bucket", record.bankrollBucket());
        payload.put(
                "preferred_stake_bucket",
                record.preferredStakeBucket());
        payload.put("skill_rating", record.skillRating());
        payload.put("device_id", record.deviceId());
        payload.put(
                "network_cluster_id",
                record.networkClusterId());
        return payload;
    }

    private static JsonNode findPlayer(JsonNode players, String playerId) {
        for (JsonNode player : players) {
            if (playerId.equals(player.path("player_id").asText())) {
                return player;
            }
        }
        throw new IllegalArgumentException("hand does not contain requested player");
    }

    private static void copy(ObjectNode target, JsonNode source, String field) {
        JsonNode value = source.get(field);
        if (value == null || value.isMissingNode()) {
            throw new IllegalArgumentException("missing required field: " + field);
        }
        target.set(field, value.deepCopy());
    }

    private static UUID uuid5(UUID namespace, String name) {
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
