package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

final class PairFeatureMath {
    private static final double EPSILON = 1e-9;
    private static final UUID NAMESPACE_URL =
            UUID.fromString("6ba7b811-9dad-11d1-80b4-00c04fd430c8");
    private static final Map<String, Integer> POSITIONS = Map.of(
            "UTG", 0, "MP", 1, "CO", 2, "BTN", 3, "SB", 4, "BB", 5);
    private static final Map<String, Integer> BANKROLL = Map.of(
            "low", 0, "medium", 1, "high", 2);
    private static final Map<String, Integer> STAKES = Map.of(
            "micro", 0, "low", 1, "medium", 2, "high", 3);

    private PairFeatureMath() {}

    static ObjectNode emptyUserState() {
        ObjectNode state = PairEventJson.MAPPER.createObjectNode();
        state.put("hands", 0);
        state.put("total_won", 0.0);
        state.put("fold_hands", 0);
        state.put("raise_hands", 0);
        state.put("saw_flop_hands", 0);
        state.putNull("last_played_at");
        return state;
    }

    static ObjectNode userSnapshot(JsonNode state) {
        int hands = state.path("hands").asInt();
        double divisor = hands == 0 ? 1.0 : hands;
        double totalWon = state.path("total_won").asDouble();
        ObjectNode snapshot = PairEventJson.MAPPER.createObjectNode();
        snapshot.put("hands_seen", hands);
        snapshot.put("total_won_amount", quantize(totalWon));
        snapshot.put("mean_won_amount", quantize(totalWon / divisor));
        snapshot.put("fold_rate", quantize(state.path("fold_hands").asInt() / divisor));
        snapshot.put("raise_rate", quantize(state.path("raise_hands").asInt() / divisor));
        snapshot.put("saw_flop_rate", quantize(state.path("saw_flop_hands").asInt() / divisor));
        return snapshot;
    }

    static void updateUser(ObjectNode state, JsonNode event) {
        JsonNode payload = event.path("payload");
        Instant playedAt = PairEventJson.parseInstant(PairEventJson.requireText(payload, "played_at"));
        ensureOrdered(state.path("last_played_at"), playedAt, "player");
        ActionSummary actions = actionSummary(payload, playerId(event));
        state.put("hands", state.path("hands").asInt() + 1);
        state.put(
                "total_won",
                state.path("total_won").asDouble()
                        + payload.path("player").path("won_amount").asDouble());
        state.put("fold_hands", state.path("fold_hands").asInt() + (actions.folds() > 0 ? 1 : 0));
        state.put("raise_hands", state.path("raise_hands").asInt() + (actions.raises() > 0 ? 1 : 0));
        state.put(
                "saw_flop_hands",
                state.path("saw_flop_hands").asInt() + (actions.sawFlop() ? 1 : 0));
        state.put("last_played_at", playedAt.toString());
    }

    static ObjectNode emptyPairState() {
        ObjectNode state = PairEventJson.MAPPER.createObjectNode();
        state.put("hands", 0);
        state.put("total_won_a", 0.0);
        state.put("total_won_b", 0.0);
        state.put("a_fold_b_win_hands", 0);
        state.put("b_fold_a_win_hands", 0);
        state.put("both_saw_flop_hands", 0);
        state.set("table_counts", PairEventJson.MAPPER.createObjectNode());
        state.putNull("last_played_at");
        return state;
    }

    static ObjectNode pairSnapshot(JsonNode state, String tableId, Instant playedAt) {
        int hands = state.path("hands").asInt();
        double divisor = hands == 0 ? 1.0 : hands;
        double totalA = state.path("total_won_a").asDouble();
        double totalB = state.path("total_won_b").asDouble();
        double total = totalA + totalB;
        ObjectNode snapshot = PairEventJson.MAPPER.createObjectNode();
        snapshot.put("hands_together", hands);
        snapshot.put("total_won_amount_a", quantize(totalA));
        snapshot.put("total_won_amount_b", quantize(totalB));
        snapshot.put(
                "outcome_asymmetry",
                total > 0 ? quantize(Math.abs(totalA - totalB) / (total + EPSILON)) : 0.0);
        snapshot.put(
                "a_fold_b_win_rate",
                quantize(state.path("a_fold_b_win_hands").asInt() / divisor));
        snapshot.put(
                "b_fold_a_win_rate",
                quantize(state.path("b_fold_a_win_hands").asInt() / divisor));
        snapshot.put(
                "both_saw_flop_rate",
                quantize(state.path("both_saw_flop_hands").asInt() / divisor));
        snapshot.put("same_table_rate", quantize(state.path("table_counts").path(tableId).asInt() / divisor));
        if (state.path("last_played_at").isTextual()) {
            Instant last = PairEventJson.parseInstant(state.path("last_played_at").textValue());
            snapshot.put(
                    "last_seen_age_seconds",
                    quantize(Math.max(0.0, Duration.between(last, playedAt).toMillis() / 1000.0)));
        } else {
            snapshot.putNull("last_seen_age_seconds");
        }
        return snapshot;
    }

    static void updatePair(ObjectNode state, JsonNode eventA, JsonNode eventB) {
        JsonNode payloadA = eventA.path("payload");
        JsonNode payloadB = eventB.path("payload");
        Instant playedAt = PairEventJson.parseInstant(PairEventJson.requireText(payloadA, "played_at"));
        ensureOrdered(state.path("last_played_at"), playedAt, "pair");
        ActionSummary actionsA = actionSummary(payloadA, playerId(eventA));
        ActionSummary actionsB = actionSummary(payloadB, playerId(eventB));
        double wonA = payloadA.path("player").path("won_amount").asDouble();
        double wonB = payloadB.path("player").path("won_amount").asDouble();
        state.put("hands", state.path("hands").asInt() + 1);
        state.put("total_won_a", state.path("total_won_a").asDouble() + wonA);
        state.put("total_won_b", state.path("total_won_b").asDouble() + wonB);
        state.put(
                "a_fold_b_win_hands",
                state.path("a_fold_b_win_hands").asInt()
                        + (actionsA.folds() > 0 && wonB > 0 ? 1 : 0));
        state.put(
                "b_fold_a_win_hands",
                state.path("b_fold_a_win_hands").asInt()
                        + (actionsB.folds() > 0 && wonA > 0 ? 1 : 0));
        state.put(
                "both_saw_flop_hands",
                state.path("both_saw_flop_hands").asInt()
                        + (actionsA.sawFlop() && actionsB.sawFlop() ? 1 : 0));
        String tableId = PairEventJson.requireText(payloadA, "table_id");
        ObjectNode tableCounts = (ObjectNode) state.path("table_counts");
        tableCounts.put(tableId, tableCounts.path(tableId).asInt() + 1);
        state.put("last_played_at", playedAt.toString());
    }

    static String buildPairFeatureEvent(JsonNode observation, JsonNode pairHistory) {
        JsonNode eventA = observation.path("a");
        JsonNode eventB = observation.path("b");
        JsonNode payloadA = eventA.path("payload");
        JsonNode payloadB = eventB.path("payload");
        String playerA = playerId(eventA);
        String playerB = playerId(eventB);
        String pairKey = PairEventJson.requireText(observation, "pair_key");
        if (playerA.compareTo(playerB) >= 0 || !pairKey.equals(playerA + ":" + playerB)) {
            throw new IllegalArgumentException("pair endpoints must use canonical order");
        }
        ObjectNode payload = PairEventJson.MAPPER.createObjectNode();
        copyText(payloadA, payload, "hand_id");
        copyText(payloadA, payload, "table_id");
        copyText(payloadA, payload, "played_at");
        payload.put("pair_key", pairKey);
        payload.put("player_a", playerA);
        payload.put("player_b", playerB);
        payload.put("num_players", payloadA.path("num_players").asInt());
        copyText(payloadA, payload, "source_hand_event_id");
        payload.put("source_player_context_event_id_a", PairEventJson.requireText(eventA, "event_id"));
        payload.put("source_player_context_event_id_b", PairEventJson.requireText(eventB, "event_id"));
        payload.put("source_revision_a", payloadA.path("revision").asInt());
        payload.put("source_revision_b", payloadB.path("revision").asInt());
        payload.put("context_status_a", PairEventJson.requireText(payloadA, "context_status"));
        payload.put("context_status_b", PairEventJson.requireText(payloadB, "context_status"));
        payload.put("context_version_a", PairEventJson.contextVersion(payloadA));
        payload.put("context_version_b", PairEventJson.contextVersion(payloadB));
        payload.put("snapshot_revision", observation.path("snapshot_revision").asInt());
        payload.put("feature_definition_version", PairEventJson.FEATURE_VERSION);
        payload.set("current_hand", currentHand(payloadA, payloadB));
        payload.set("context", context(payloadA, payloadB));
        payload.set("user_history_a", eventA.path("user_history").deepCopy());
        payload.set("user_history_b", eventB.path("user_history").deepCopy());
        payload.set("pair_history", pairHistory.deepCopy());

        String emittedAt = maxInstantText(
                PairEventJson.requireText(eventA, "emitted_at"),
                PairEventJson.requireText(eventB, "emitted_at"));
        String eventName = String.join(
                ":",
                PairEventJson.requireText(eventA, "dataset_id"),
                PairEventJson.requireText(eventA, "dataset_split"),
                PairEventJson.PAIR_FEATURES,
                PairEventJson.requireText(payloadA, "source_hand_event_id"),
                pairKey,
                PairEventJson.requireText(eventA, "event_id"),
                PairEventJson.requireText(eventB, "event_id"),
                PairEventJson.FEATURE_VERSION);
        ObjectNode root = PairEventJson.MAPPER.createObjectNode();
        root.put("event_id", uuid5(NAMESPACE_URL, eventName).toString());
        root.put("event_type", PairEventJson.PAIR_FEATURES);
        root.put("schema_version", 1);
        copyText(eventA, root, "tenant_id");
        copyText(eventA, root, "product_id");
        copyText(eventA, root, "dataset_id");
        copyText(eventA, root, "dataset_split");
        root.put("occurred_at", PairEventJson.requireText(payloadA, "played_at"));
        root.put("emitted_at", emittedAt);
        copyText(eventA, root, "trace_id");
        root.set("payload", payload);
        return PairEventJson.compact(root);
    }

    private static ObjectNode currentHand(JsonNode payloadA, JsonNode payloadB) {
        String playerA = PairEventJson.requireText(payloadA.path("player"), "player_id");
        String playerB = PairEventJson.requireText(payloadB.path("player"), "player_id");
        ActionSummary actionsA = actionSummary(payloadA, playerA);
        ActionSummary actionsB = actionSummary(payloadB, playerB);
        double pot = Math.max(payloadA.path("pot_size").asDouble(), EPSILON);
        double wonA = payloadA.path("player").path("won_amount").asDouble();
        double wonB = payloadB.path("player").path("won_amount").asDouble();
        int positionA = POSITIONS.get(PairEventJson.requireText(payloadA.path("player"), "position"));
        int positionB = POSITIONS.get(PairEventJson.requireText(payloadB.path("player"), "position"));
        ObjectNode current = PairEventJson.MAPPER.createObjectNode();
        current.put("position_index_a", positionA);
        current.put("position_index_b", positionB);
        current.put("position_gap", Math.abs(positionA - positionB));
        current.put("invested_amount_a", quantize(actionsA.invested()));
        current.put("invested_amount_b", quantize(actionsB.invested()));
        current.put("invested_pot_ratio_a", quantize(actionsA.invested() / pot));
        current.put("invested_pot_ratio_b", quantize(actionsB.invested() / pot));
        current.put("invested_abs_diff_ratio", quantize(Math.abs(actionsA.invested() - actionsB.invested()) / pot));
        current.put("won_amount_a", quantize(wonA));
        current.put("won_amount_b", quantize(wonB));
        current.put("outcome_abs_diff_ratio", quantize(Math.abs(wonA - wonB) / pot));
        current.put("aggressive_actions_a", actionsA.aggressive());
        current.put("aggressive_actions_b", actionsB.aggressive());
        current.put("fold_actions_a", actionsA.folds());
        current.put("fold_actions_b", actionsB.folds());
        current.put("both_saw_flop", actionsA.sawFlop() && actionsB.sawFlop());
        current.put("both_saw_river", actionsA.sawRiver() && actionsB.sawRiver());
        current.put(
                "one_folded_other_won",
                (actionsA.folds() > 0 && wonB > 0) || (actionsB.folds() > 0 && wonA > 0));
        return current;
    }

    private static ObjectNode context(JsonNode payloadA, JsonNode payloadB) {
        JsonNode contextA = payloadA.path("context");
        JsonNode contextB = payloadB.path("context");
        boolean missingA = !contextA.isObject();
        boolean missingB = !contextB.isObject();
        Instant playedAt = PairEventJson.parseInstant(PairEventJson.requireText(payloadA, "played_at"));
        double skillA = missingA ? 0.0 : contextA.path("skill_rating").asDouble();
        double skillB = missingB ? 0.0 : contextB.path("skill_rating").asDouble();
        double ageA = missingA ? 0.0 : ageDays(contextA, playedAt);
        double ageB = missingB ? 0.0 : ageDays(contextB, playedAt);
        ObjectNode output = PairEventJson.MAPPER.createObjectNode();
        output.put("context_missing_a", missingA);
        output.put("context_missing_b", missingB);
        output.put("skill_rating_a", quantize(skillA));
        output.put("skill_rating_b", quantize(skillB));
        output.put("skill_rating_abs_diff", quantize(Math.abs(skillA - skillB)));
        output.put("account_age_days_a", quantize(ageA));
        output.put("account_age_days_b", quantize(ageB));
        output.put("account_age_abs_diff_days", quantize(Math.abs(ageA - ageB)));
        output.put("same_country", same(contextA, contextB, "country_bucket"));
        output.put("same_timezone", same(contextA, contextB, "timezone"));
        output.put("same_acquisition_channel", same(contextA, contextB, "acquisition_channel"));
        output.put("same_device", same(contextA, contextB, "device_id"));
        output.put("same_network", same(contextA, contextB, "network_cluster_id"));
        output.put("bankroll_bucket_distance", bucketDistance(contextA, contextB, "bankroll_bucket", BANKROLL));
        output.put("preferred_stake_bucket_distance", bucketDistance(contextA, contextB, "preferred_stake_bucket", STAKES));
        return output;
    }

    private static ActionSummary actionSummary(JsonNode payload, String playerId) {
        double invested = 0.0;
        int aggressive = 0;
        int folds = 0;
        int raises = 0;
        boolean sawFlop = false;
        boolean sawRiver = false;
        for (JsonNode action : payload.path("actions")) {
            if (!playerId.equals(action.path("player_id").asText())) {
                continue;
            }
            String type = action.path("action_type").asText();
            String street = action.path("street").asText();
            invested += action.path("amount").asDouble();
            aggressive += type.equals("bet") || type.equals("raise") ? 1 : 0;
            folds += type.equals("fold") ? 1 : 0;
            raises += type.equals("raise") ? 1 : 0;
            sawFlop |= street.equals("flop");
            sawRiver |= street.equals("river");
        }
        return new ActionSummary(invested, aggressive, folds, raises, sawFlop, sawRiver);
    }

    private static String playerId(JsonNode event) {
        return PairEventJson.requireText(event.path("payload").path("player"), "player_id");
    }

    private static boolean same(JsonNode left, JsonNode right, String field) {
        return left.isObject()
                && right.isObject()
                && left.path(field).isTextual()
                && left.path(field).equals(right.path(field));
    }

    private static int bucketDistance(
            JsonNode left, JsonNode right, String field, Map<String, Integer> values) {
        int a = left.isObject() ? values.getOrDefault(left.path(field).asText(), 0) : 0;
        int b = right.isObject() ? values.getOrDefault(right.path(field).asText(), 0) : 0;
        return Math.abs(a - b);
    }

    private static double ageDays(JsonNode context, Instant playedAt) {
        Instant created = PairEventJson.parseInstant(PairEventJson.requireText(context, "account_created_at"));
        return Math.max(0.0, Duration.between(created, playedAt).toMillis() / 86_400_000.0);
    }

    private static void ensureOrdered(JsonNode previous, Instant current, String entity) {
        if (previous.isTextual()
                && current.isBefore(PairEventJson.parseInstant(previous.textValue()))) {
            throw new IllegalArgumentException(
                    "new " + entity + " hands must be processed in event-time order");
        }
    }

    private static void copyText(JsonNode source, ObjectNode target, String field) {
        target.put(field, PairEventJson.requireText(source, field));
    }

    private static void copyNullableInteger(
            JsonNode source, ObjectNode target, String sourceField, String targetField) {
        if (source.path(sourceField).isIntegralNumber()) {
            target.put(targetField, source.path(sourceField).asInt());
        } else {
            target.putNull(targetField);
        }
    }

    private static String maxInstantText(String left, String right) {
        return PairEventJson.parseInstant(left).compareTo(PairEventJson.parseInstant(right)) >= 0
                ? left
                : right;
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

    private record ActionSummary(
            double invested,
            int aggressive,
            int folds,
            int raises,
            boolean sawFlop,
            boolean sawRiver) {}
}
