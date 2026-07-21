package com.aicampions.poker.features;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

final class PairEventJson {
    static final ObjectMapper MAPPER = new ObjectMapper();
    static final String ENRICHED = "poker.hand-player-context.enriched";
    static final String PAIR_FEATURES = "poker.pair-features.computed";
    static final String FEATURE_VERSION = "pair-features-v1";
    private static final Set<String> FORBIDDEN_FIELDS = Set.of(
            "collusion_group_id", "collusion_pair_id", "collusion_scenario",
            "is_collusive", "is_suspicious", "label", "label_available_at",
            "scenario_name");

    private PairEventJson() {}

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

    static void validateEnriched(String value) {
        JsonNode root = parse(value);
        parseUuid(requireText(root, "event_id"));
        if (!ENRICHED.equals(requireText(root, "event_type"))) {
            throw new IllegalArgumentException("unexpected event_type");
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
        requireText(payload, "hand_id");
        requireText(payload, "table_id");
        parseInstant(requireText(payload, "played_at"));
        requireText(payload.path("player"), "player_id");
        parseUuid(requireText(payload, "source_hand_event_id"));
        if (payload.path("revision").asInt(0) < 1
                || payload.path("num_players").asInt(0) < 2) {
            throw new IllegalArgumentException("invalid revision or player count");
        }
        if (!payload.path("actions").isArray()) {
            throw new IllegalArgumentException("actions must be an array");
        }
        rejectForbiddenFields(payload);
    }

    static String requireText(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw new IllegalArgumentException(field + " must be a non-empty string");
        }
        return value.textValue();
    }

    static Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (RuntimeException error) {
            throw new IllegalArgumentException("invalid UTC timestamp: " + value, error);
        }
    }

    static String playerId(String value) {
        return requireText(parse(value).path("payload").path("player"), "player_id");
    }

    static String handId(String value) {
        return requireText(parse(value).path("payload"), "hand_id");
    }

    static String pairKey(String value) {
        return requireText(parse(value), "pair_key");
    }

    static String scopedPairKey(String value) {
        return scopedPairKey(parse(value));
    }

    static String scopedPairKey(JsonNode root) {
        return String.join(
                "\u001f",
                requireText(root, "tenant_id"),
                requireText(root, "product_id"),
                requireText(root, "dataset_id"),
                requireText(root, "dataset_split"),
                requireText(root.path("payload"), "pair_key"));
    }

    static long occurredAtMs(String value) {
        return parseInstant(requireText(parse(value), "occurred_at")).toEpochMilli();
    }

    static String augmentUser(String value, JsonNode history) {
        ObjectNode root = (ObjectNode) parse(value).deepCopy();
        root.set("user_history", history.deepCopy());
        return compact(root);
    }

    static String pairObservation(String pairKey, int revision, String left, String right) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("pair_key", pairKey);
        root.put("snapshot_revision", revision);
        root.set("a", parse(left));
        root.set("b", parse(right));
        return compact(root);
    }

    static List<String> canonicalPairs(Collection<String> playerIds) {
        List<String> ordered = new ArrayList<>(playerIds);
        ordered.sort(String::compareTo);
        List<String> output = new ArrayList<>();
        for (int left = 0; left < ordered.size(); left++) {
            for (int right = left + 1; right < ordered.size(); right++) {
                output.add(ordered.get(left) + ":" + ordered.get(right));
            }
        }
        return output;
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
                if (FORBIDDEN_FIELDS.contains(field.getKey().toLowerCase())) {
                    throw new IllegalArgumentException("private label field found: " + field.getKey());
                }
                rejectForbiddenFields(field.getValue());
            }
        } else if (value.isArray()) {
            value.forEach(PairEventJson::rejectForbiddenFields);
        }
    }
}
