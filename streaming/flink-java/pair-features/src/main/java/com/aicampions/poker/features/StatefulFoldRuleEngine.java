package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/** Pure state transition shared by the Flink operator and golden tests. */
final class StatefulFoldRuleEngine {
    static final String RULE_ID = "pair.repeated-fold-to-partner-wins";
    static final int RULE_VERSION = 1;
    static final String RULE_OWNER = "risk-analytics";
    static final String SEVERITY = "high";
    private static final UUID NAMESPACE_URL =
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
    private static final DateTimeFormatter IDENTITY_TIME =
            new DateTimeFormatterBuilder().appendInstant(6).toFormatter();

    private StatefulFoldRuleEngine() {}

    record Config(
            long windowMs,
            int minimumHands,
            int minimumDirectionalCount,
            double rateThreshold,
            long allowedLatenessMs,
            long correctionHorizonMs) {
        Config {
            if (windowMs < 1 || minimumHands < 1 || minimumDirectionalCount < 1
                    || rateThreshold < 0 || rateThreshold > 1
                    || allowedLatenessMs < 0 || correctionHorizonMs < windowMs) {
                throw new IllegalArgumentException("invalid stateful fold rule configuration");
            }
        }
    }

    record Evaluation(
            String stateJson,
            String status,
            int windowHandCount,
            int directionalCount,
            double directionalRate,
            ObjectNode evidenceEvent,
            int stateSize) {}

    static Evaluation evaluate(
            String previousState,
            JsonNode pairEvent,
            long watermarkMs,
            Config config) {
        validatePairEvent(pairEvent);
        ObjectNode state = previousState == null
                ? emptyState(pairEvent)
                : (ObjectNode) PairEventJson.parse(previousState).deepCopy();
        String expectedScope = PairEventJson.scopedPairKey(pairEvent);
        if (!expectedScope.equals(PairEventJson.requireText(state, "scope"))) {
            throw new IllegalArgumentException("stateful rule cannot combine scoped pair keys");
        }

        JsonNode payload = pairEvent.path("payload");
        String handId = PairEventJson.requireText(payload, "hand_id");
        int revision = payload.path("snapshot_revision").asInt(0);
        long playedAtMs = PairEventJson.parseInstant(
                PairEventJson.requireText(payload, "played_at")).toEpochMilli();
        ObjectNode observations = (ObjectNode) state.path("observations");
        JsonNode existing = observations.path(handId);

        if (existing.isMissingNode()
                && watermarkMs != Long.MIN_VALUE
                && playedAtMs < watermarkMs - config.allowedLatenessMs()) {
            return emptyEvaluation(state, "too_late");
        }

        ObjectNode current = observation(pairEvent);
        String status;
        if (existing.isObject()) {
            int existingRevision = existing.path("snapshot_revision").asInt();
            if (revision < existingRevision) {
                return emptyEvaluation(state, "stale");
            }
            if (revision == existingRevision) {
                if (!existing.equals(current)) {
                    throw new IllegalArgumentException(
                            "same hand revision has conflicting rule inputs");
                }
                return evaluateWindow(state, pairEvent, current, "duplicate", config);
            }
            long maxEventTime = state.path("max_event_time_ms").asLong(playedAtMs);
            if (playedAtMs < maxEventTime - config.correctionHorizonMs()) {
                return emptyEvaluation(state, "too_late_correction");
            }
            status = "corrected";
        } else {
            status = "accepted";
        }

        observations.set(handId, current);
        long maxEventTime = state.path("max_event_time_ms").isIntegralNumber()
                ? Math.max(state.path("max_event_time_ms").asLong(), playedAtMs)
                : playedAtMs;
        state.put("max_event_time_ms", maxEventTime);
        prune(state, maxEventTime - config.correctionHorizonMs());
        return evaluateWindow(state, pairEvent, current, status, config);
    }

    static ObjectNode enrichPairEvent(JsonNode pairEvent, ObjectNode evidenceEvent) {
        ObjectNode enriched = (ObjectNode) pairEvent.deepCopy();
        ArrayNode evidence = enriched.putArray("upstream_rule_evidence");
        if (evidenceEvent != null) {
            evidence.add(evidenceEvent.deepCopy());
        }
        return enriched;
    }

    private static Evaluation evaluateWindow(
            ObjectNode state,
            JsonNode pairEvent,
            JsonNode current,
            String status,
            Config config) {
        long currentMs = current.path("played_at_ms").asLong();
        long startMs = currentMs - config.windowMs();
        int hands = 0;
        int aCount = 0;
        int bCount = 0;
        for (JsonNode value : state.path("observations")) {
            long playedAtMs = value.path("played_at_ms").asLong();
            if (playedAtMs < startMs || playedAtMs > currentMs) {
                continue;
            }
            hands++;
            aCount += value.path("a_fold_b_win").asBoolean() ? 1 : 0;
            bCount += value.path("b_fold_a_win").asBoolean() ? 1 : 0;
        }
        String direction = aCount >= bCount ? "a_fold_b_win" : "b_fold_a_win";
        int directionalCount = Math.max(aCount, bCount);
        double rate = hands == 0 ? 0.0 : quantize((double) directionalCount / hands);
        boolean fired = hands >= config.minimumHands()
                && directionalCount >= config.minimumDirectionalCount()
                && rate >= config.rateThreshold();
        ObjectNode evidence = fired
                ? buildEvidence(pairEvent, direction, hands, directionalCount, rate, config)
                : null;
        return new Evaluation(
                PairEventJson.compact(state), status, hands, directionalCount, rate,
                evidence, state.path("observations").size());
    }

    private static Evaluation emptyEvaluation(ObjectNode state, String status) {
        return new Evaluation(
                PairEventJson.compact(state), status, 0, 0, 0.0, null,
                state.path("observations").size());
    }

    private static ObjectNode emptyState(JsonNode pairEvent) {
        ObjectNode state = PairEventJson.MAPPER.createObjectNode();
        state.put("scope", PairEventJson.scopedPairKey(pairEvent));
        state.putNull("max_event_time_ms");
        state.set("observations", PairEventJson.MAPPER.createObjectNode());
        return state;
    }

    private static ObjectNode observation(JsonNode event) {
        JsonNode payload = event.path("payload");
        JsonNode current = payload.path("current_hand");
        ObjectNode value = PairEventJson.MAPPER.createObjectNode();
        value.put("event_id", PairEventJson.requireText(event, "event_id"));
        value.put("trace_id", PairEventJson.requireText(event, "trace_id"));
        value.put("emitted_at", PairEventJson.requireText(event, "emitted_at"));
        value.put("played_at", PairEventJson.requireText(payload, "played_at"));
        value.put("played_at_ms", PairEventJson.parseInstant(
                PairEventJson.requireText(payload, "played_at")).toEpochMilli());
        value.put("snapshot_revision", payload.path("snapshot_revision").asInt());
        value.put(
                "a_fold_b_win",
                current.path("fold_actions_a").asInt() > 0
                        && current.path("won_amount_b").asDouble() > 0);
        value.put(
                "b_fold_a_win",
                current.path("fold_actions_b").asInt() > 0
                        && current.path("won_amount_a").asDouble() > 0);
        return value;
    }

    private static void prune(ObjectNode state, long oldestMs) {
        ObjectNode observations = (ObjectNode) state.path("observations");
        List<String> expired = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = observations.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getValue().path("played_at_ms").asLong() < oldestMs) {
                expired.add(field.getKey());
            }
        }
        expired.forEach(observations::remove);
    }

    private static ObjectNode buildEvidence(
            JsonNode event,
            String direction,
            int hands,
            int directionalCount,
            double rate,
            Config config) {
        JsonNode payload = event.path("payload");
        String playedAt = PairEventJson.requireText(payload, "played_at");
        Instant effectiveAt = PairEventJson.parseInstant(playedAt);
        int revision = payload.path("snapshot_revision").asInt();
        String identity = String.join(
                "\u001f",
                PairEventJson.requireText(event, "tenant_id"),
                PairEventJson.requireText(event, "product_id"),
                PairEventJson.requireText(event, "dataset_id"),
                PairEventJson.requireText(event, "dataset_split"),
                RULE_ID,
                Integer.toString(RULE_VERSION),
                "pair",
                PairEventJson.requireText(payload, "pair_key"),
                PairEventJson.requireText(payload, "hand_id"),
                Integer.toString(revision),
                IDENTITY_TIME.format(effectiveAt),
                PairEventJson.requireText(payload, "feature_definition_version"));
        String eventId = uuid5(NAMESPACE_URL, identity).toString();

        ObjectNode evidence = PairEventJson.MAPPER.createObjectNode();
        evidence.put("window_hours", config.windowMs() / 3_600_000L);
        evidence.put("window_hand_count", hands);
        evidence.put("direction", direction);
        evidence.put("directional_fold_win_count", directionalCount);
        evidence.put("directional_fold_win_rate", rate);
        evidence.put("minimum_hands", config.minimumHands());
        evidence.put("minimum_directional_count", config.minimumDirectionalCount());
        evidence.put("rate_threshold", config.rateThreshold());
        evidence.put("source_pair_feature_event_id", PairEventJson.requireText(event, "event_id"));
        evidence.put("snapshot_revision", revision);

        ObjectNode rulePayload = PairEventJson.MAPPER.createObjectNode();
        rulePayload.put("rule_event_id", eventId);
        rulePayload.put("rule_id", RULE_ID);
        rulePayload.put("rule_version", RULE_VERSION);
        rulePayload.put("rule_owner", RULE_OWNER);
        rulePayload.put("entity_type", "pair");
        rulePayload.put("entity_key", PairEventJson.requireText(payload, "pair_key"));
        rulePayload.put("hand_id", PairEventJson.requireText(payload, "hand_id"));
        rulePayload.put("observation_revision", revision);
        rulePayload.put("severity", SEVERITY);
        rulePayload.put("raw_score", quantize(rate * 100.0));
        rulePayload.set("evidence", evidence);
        rulePayload.put("effective_at", playedAt);
        rulePayload.put(
                "feature_definition_version",
                PairEventJson.requireText(payload, "feature_definition_version"));

        ObjectNode root = PairEventJson.MAPPER.createObjectNode();
        root.put("event_id", eventId);
        root.put("event_type", "poker.rule-evidence.recorded");
        root.put("schema_version", 1);
        copyText(event, root, "tenant_id");
        copyText(event, root, "product_id");
        copyText(event, root, "dataset_id");
        copyText(event, root, "dataset_split");
        root.put("occurred_at", playedAt);
        root.put("emitted_at", PairEventJson.requireText(event, "emitted_at"));
        copyText(event, root, "trace_id");
        root.set("payload", rulePayload);
        return root;
    }

    private static void validatePairEvent(JsonNode event) {
        if (!PairEventJson.PAIR_FEATURES.equals(PairEventJson.requireText(event, "event_type"))
                || event.path("schema_version").asInt() != 1) {
            throw new IllegalArgumentException("stateful rules require pair-features v1");
        }
        JsonNode payload = event.path("payload");
        if (!PairEventJson.FEATURE_VERSION.equals(
                        PairEventJson.requireText(payload, "feature_definition_version"))
                || payload.path("snapshot_revision").asInt() < 1
                || !payload.path("current_hand").isObject()) {
            throw new IllegalArgumentException("invalid pair snapshot for stateful rules");
        }
        PairEventJson.parseInstant(PairEventJson.requireText(payload, "played_at"));
        PairEventJson.parseInstant(PairEventJson.requireText(event, "emitted_at"));
        PairEventJson.requireText(payload, "hand_id");
        PairEventJson.requireText(payload, "pair_key");
    }

    private static void copyText(JsonNode source, ObjectNode target, String field) {
        target.put(field, PairEventJson.requireText(source, field));
    }

    private static double quantize(double value) {
        return BigDecimal.valueOf(value).setScale(9, RoundingMode.HALF_EVEN).doubleValue();
    }

    private static UUID uuid5(UUID namespace, String name) {
        try {
            MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
            ByteBuffer namespaceBytes = ByteBuffer.allocate(16)
                    .putLong(namespace.getMostSignificantBits())
                    .putLong(namespace.getLeastSignificantBits());
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
