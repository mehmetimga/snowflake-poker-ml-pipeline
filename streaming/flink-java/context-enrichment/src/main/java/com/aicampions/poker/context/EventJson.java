package com.aicampions.poker.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class EventJson {
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String HAND_COMPLETED = "poker.hand.completed";
    static final String USER_CONTEXT_UPDATED = "poker.user-context.updated";
    static final String ENRICHED = "poker.hand-player-context.enriched";
    static final String JOIN_POLICY = "event-time-user-context-v1";
    private static final Set<String> FORBIDDEN_INFERENCE_FIELDS = Set.of(
            "collusion_group_id",
            "collusion_pair_id",
            "collusion_scenario",
            "is_collusive",
            "is_suspicious",
            "label",
            "label_available_at",
            "scenario_name");

    private EventJson() {}

    static JsonNode parse(String value) {
        try {
            return MAPPER.readTree(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("invalid JSON", error);
        }
    }

    static String compact(JsonNode value) {
        try {
            return MAPPER.writeValueAsString(value);
        } catch (JsonProcessingException error) {
            throw new IllegalArgumentException("cannot serialize JSON", error);
        }
    }

    static void validateEnvelope(String value, String requiredEventType) {
        JsonNode root = parse(value);
        parseUuid(requireText(root, "event_id"));
        String eventType = requireText(root, "event_type");
        if (!requiredEventType.equals(eventType)) {
            throw new IllegalArgumentException("unexpected event_type: " + eventType);
        }
        if (root.path("schema_version").asInt(-1) != 1) {
            throw new IllegalArgumentException("schema_version must be 1");
        }
        requireText(root, "tenant_id");
        requireText(root, "product_id");
        requireText(root, "dataset_id");
        requireText(root, "dataset_split");
        parseInstant(requireText(root, "occurred_at"));
        parseInstant(requireText(root, "emitted_at"));
        parseUuid(requireText(root, "trace_id"));
        JsonNode payload = root.path("payload");
        if (!payload.isObject()) {
            throw new IllegalArgumentException("payload must be an object");
        }
        rejectForbiddenFields(payload);
        if (HAND_COMPLETED.equals(requiredEventType)) {
            validateHand(payload);
        } else if (USER_CONTEXT_UPDATED.equals(requiredEventType)) {
            validateContext(payload);
        }
    }

    private static void validateHand(JsonNode payload) {
        requireText(payload, "hand_id");
        requireText(payload, "table_id");
        parseInstant(requireText(payload, "played_at"));
        JsonNode players = payload.path("players");
        if (!players.isArray() || players.size() < 2) {
            throw new IllegalArgumentException("players must contain at least two rows");
        }
        for (JsonNode player : players) {
            requireText(player, "player_id");
        }
        if (payload.path("num_players").asInt(-1) != players.size()) {
            throw new IllegalArgumentException("num_players does not match players");
        }
        if (!payload.path("actions").isArray() || !payload.path("board").isArray()) {
            throw new IllegalArgumentException("actions and board must be arrays");
        }
    }

    private static void validateContext(JsonNode payload) {
        requireText(payload, "user_id");
        if (payload.path("context_version").asInt(0) < 1) {
            throw new IllegalArgumentException("context_version must be positive");
        }
        parseInstant(requireText(payload, "effective_at"));
        requireText(payload, "account_created_at");
        requireText(payload, "country_bucket");
        requireText(payload, "timezone");
        requireText(payload, "acquisition_channel");
        requireText(payload, "kyc_level");
        requireText(payload, "account_status");
        requireText(payload, "bankroll_bucket");
        requireText(payload, "preferred_stake_bucket");
        requireText(payload, "device_id");
        requireText(payload, "network_cluster_id");
    }

    static String requireText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-empty string");
        }
        return value.textValue();
    }

    static long parseInstant(String value) {
        try {
            return Instant.parse(value).toEpochMilli();
        } catch (RuntimeException error) {
            throw new IllegalArgumentException("invalid UTC timestamp: " + value, error);
        }
    }

    private static UUID parseUuid(String value) {
        try {
            return UUID.fromString(value);
        } catch (RuntimeException error) {
            throw new IllegalArgumentException("invalid UUID: " + value, error);
        }
    }

    private static void rejectForbiddenFields(JsonNode value) {
        if (value.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = value.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                if (FORBIDDEN_INFERENCE_FIELDS.contains(field.getKey().toLowerCase())) {
                    throw new IllegalArgumentException(
                            "private label field found: " + field.getKey());
                }
                rejectForbiddenFields(field.getValue());
            }
        } else if (value.isArray()) {
            value.forEach(EventJson::rejectForbiddenFields);
        }
    }

    static long occurredAtMs(String value) {
        return parseInstant(requireText(parse(value), "occurred_at"));
    }

    static String contextUserId(String value) {
        return requireText(parse(value).path("payload"), "user_id");
    }

    static String playerIdFromExpandedHand(String value) {
        return requireText(parse(value), "player_id");
    }

    static String playerIdFromEnriched(String value) {
        return requireText(parse(value).path("payload").path("player"), "player_id");
    }

    static String expandPlayer(JsonNode hand, JsonNode player) {
        ObjectNode expanded = MAPPER.createObjectNode();
        expanded.set("hand", hand.deepCopy());
        expanded.put("player_id", requireText(player, "player_id"));
        return compact(expanded);
    }

    static String deadLetter(String sourceTopic, String stage, String reason, String rawValue) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("event_type", "poker.pipeline.dead-letter");
        root.put("schema_version", 1);
        root.put("source_topic", sourceTopic);
        root.put("stage", stage);
        root.put("reason", reason);
        root.put("emitted_at", Instant.now().toString());
        root.put("raw_value", rawValue);
        return compact(root);
    }

    static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

}
