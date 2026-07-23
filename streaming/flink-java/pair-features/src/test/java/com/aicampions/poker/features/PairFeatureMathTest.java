package com.aicampions.poker.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class PairFeatureMathTest {
    @Test
    void sixPlayersExpandToFifteenCanonicalPairs() {
        List<String> pairs = PairEventJson.canonicalPairs(
                List.of("p6", "p2", "p1", "p5", "p4", "p3"));

        assertEquals(15, pairs.size());
        assertEquals("p1:p2", pairs.get(0));
        assertEquals("p5:p6", pairs.get(14));
    }

    @Test
    void rollingSnapshotsExcludeCurrentHand() {
        ObjectNode user = PairFeatureMath.emptyUserState();
        JsonNode event = enriched("p1", "BTN", 30.0, 0.6, "device-a", 365);

        JsonNode before = PairFeatureMath.userSnapshot(user);
        PairFeatureMath.updateUser(user, event);
        JsonNode after = PairFeatureMath.userSnapshot(user);

        assertEquals(0, before.path("hands_seen").asInt());
        assertEquals(1, after.path("hands_seen").asInt());
        assertEquals(30.0, after.path("total_won_amount").asDouble());
        assertEquals(1.0, after.path("raise_rate").asDouble());
        assertEquals(1.0, after.path("saw_flop_rate").asDouble());
    }

    @Test
    void pairFeatureVectorMatchesVersionOneGoldenSemantics() {
        ObjectNode eventA = enriched("p1", "BTN", 30.0, 0.6, "device-a", 365);
        ObjectNode eventB = enriched("p2", "BB", 0.0, 0.4, "device-b", 730);
        ObjectNode userHistory = PairFeatureMath.userSnapshot(PairFeatureMath.emptyUserState());
        eventA.set("user_history", userHistory.deepCopy());
        eventB.set("user_history", userHistory.deepCopy());
        ObjectNode pairState = PairFeatureMath.emptyPairState();
        ObjectNode pairHistory = PairFeatureMath.pairSnapshot(
                pairState, "table-1", Instant.parse("2026-01-01T00:00:00Z"));
        JsonNode observation = PairEventJson.parse(
                PairEventJson.pairObservation(
                        "p1:p2", 1, PairEventJson.compact(eventA), PairEventJson.compact(eventB)));

        JsonNode output = PairEventJson.parse(
                PairFeatureMath.buildPairFeatureEvent(observation, pairHistory));
        JsonNode payload = output.path("payload");
        JsonNode current = payload.path("current_hand");
        JsonNode context = payload.path("context");

        assertEquals(PairEventJson.PAIR_FEATURES, output.path("event_type").asText());
        assertEquals(PairEventJson.FEATURE_VERSION, payload.path("feature_definition_version").asText());
        assertEquals("p1:p2", payload.path("pair_key").asText());
        assertEquals(3, current.path("position_index_a").asInt());
        assertEquals(5, current.path("position_index_b").asInt());
        assertEquals(10.0, current.path("invested_amount_a").asDouble());
        assertEquals(15.0, current.path("invested_amount_b").asDouble());
        assertEquals(0.333333333, current.path("invested_pot_ratio_a").asDouble(), 1e-12);
        assertEquals(0.5, current.path("invested_pot_ratio_b").asDouble(), 1e-12);
        assertEquals(1, current.path("aggressive_actions_a").asInt());
        assertEquals(1, current.path("fold_actions_b").asInt());
        assertTrue(current.path("both_saw_flop").asBoolean());
        assertFalse(current.path("both_saw_river").asBoolean());
        assertTrue(current.path("one_folded_other_won").asBoolean());
        assertEquals(0.2, context.path("skill_rating_abs_diff").asDouble(), 1e-12);
        assertTrue(context.path("same_country").asBoolean());
        assertFalse(context.path("same_device").asBoolean());
        assertTrue(context.path("same_network").asBoolean());
        assertEquals(0, pairHistory.path("hands_together").asInt());

        PairFeatureMath.updatePair(pairState, eventA, eventB);
        JsonNode next = PairFeatureMath.pairSnapshot(
                pairState, "table-1", Instant.parse("2026-01-01T00:01:00Z"));
        assertEquals(1, next.path("hands_together").asInt());
        assertEquals(1.0, next.path("same_table_rate").asDouble());
        assertEquals(60.0, next.path("last_seen_age_seconds").asDouble());
    }

    @Test
    void schemaTwoJdbcInputAdaptsToTheSamePairFeatureVersion() {
        ObjectNode eventA = jdbcV2(enriched("p1", "BTN", 30.0, 0.6, "device-a", 365));
        ObjectNode eventB = jdbcV2(enriched("p2", "BB", 0.0, 0.4, "device-b", 730));
        PairEventJson.validateEnriched(PairEventJson.compact(eventA), 2);
        PairEventJson.validateEnriched(PairEventJson.compact(eventB), 2);
        JsonNode userHistory = PairFeatureMath.userSnapshot(PairFeatureMath.emptyUserState());
        eventA.set("user_history", userHistory.deepCopy());
        eventB.set("user_history", userHistory.deepCopy());
        JsonNode observation = PairEventJson.parse(PairEventJson.pairObservation(
                "p1:p2",
                1,
                PairEventJson.compact(eventA),
                PairEventJson.compact(eventB)));

        JsonNode output = PairEventJson.parse(PairFeatureMath.buildPairFeatureEvent(
                observation, PairFeatureMath.emptyPairState()));

        assertEquals(1, output.path("payload").path("context_version_a").asInt());
        assertEquals(1, output.path("payload").path("context_version_b").asInt());
        assertEquals(
                PairEventJson.FEATURE_VERSION,
                output.path("payload").path("feature_definition_version").asText());
    }

    @Test
    void schemaTwoAcceptsSnowflakeLineageAndRejectsMixedPolicies() {
        ObjectNode snowflake = snowflakeV2(
                enriched("p1", "BTN", 30.0, 0.6, "device-a", 365));
        PairEventJson.validateEnriched(PairEventJson.compact(snowflake), 2);

        snowflake
                .withObject("/payload/context_resolution")
                .put("policy_version", "jdbc-effective-at-v1");
        assertThrows(
                IllegalArgumentException.class,
                () -> PairEventJson.validateEnriched(
                        PairEventJson.compact(snowflake), 2));
    }

    private static ObjectNode enriched(
            String playerId,
            String position,
            double wonAmount,
            double skill,
            String deviceId,
            int accountAgeDays) {
        ObjectNode root = PairEventJson.MAPPER.createObjectNode();
        root.put("event_id", UUID.nameUUIDFromBytes(("event-" + playerId).getBytes()).toString());
        root.put("event_type", PairEventJson.ENRICHED);
        root.put("schema_version", 1);
        root.put("tenant_id", "demo");
        root.put("product_id", "poker");
        root.put("dataset_id", "golden-v1");
        root.put("dataset_split", "train");
        root.put("occurred_at", "2026-01-01T00:00:00Z");
        root.put("emitted_at", "2026-01-01T00:00:01Z");
        root.put("trace_id", "00000000-0000-0000-0000-000000000001");
        ObjectNode payload = root.putObject("payload");
        payload.put("hand_id", "hand-1");
        payload.put("table_id", "table-1");
        payload.put("played_at", "2026-01-01T00:00:00Z");
        ObjectNode player = payload.putObject("player");
        player.put("player_id", playerId);
        player.put("name", playerId);
        player.put("position", position);
        player.put("stack_start", 100.0);
        player.put("hole_cards", "As Ks");
        player.put("won_amount", wonAmount);
        payload.set("actions", actions());
        payload.set("board", PairEventJson.MAPPER.createArrayNode());
        payload.put("small_blind", 0.5);
        payload.put("big_blind", 1.0);
        payload.put("num_players", 2);
        payload.put("pot_size", 30.0);
        payload.put("source_hand_event_id", "00000000-0000-0000-0000-000000000010");
        payload.put("context_status", "matched");
        payload.put("context_version", 1);
        payload.put("context_effective_at", "2025-01-01T00:00:00Z");
        payload.put("source_context_event_id", "00000000-0000-0000-0000-000000000020");
        ObjectNode context = payload.putObject("context");
        context.put("user_id", playerId);
        context.put("context_version", 1);
        context.put("effective_at", "2025-01-01T00:00:00Z");
        context.put(
                "account_created_at",
                Instant.parse("2026-01-01T00:00:00Z")
                        .minusSeconds(accountAgeDays * 86_400L)
                        .toString());
        context.put("country_bucket", "TR");
        context.put("timezone", "Europe/Istanbul");
        context.put("acquisition_channel", "organic");
        context.put("kyc_level", "verified");
        context.put("account_status", "active");
        context.put("bankroll_bucket", "medium");
        context.put("preferred_stake_bucket", "low");
        context.put("skill_rating", skill);
        context.put("device_id", deviceId);
        context.put("network_cluster_id", "network-shared");
        payload.put("revision", 1);
        payload.put("allowed_lateness_ms", 30_000);
        payload.put("correction_window_ms", 300_000);
        payload.put("join_policy_version", "event-time-user-context-v1");
        return root;
    }

    private static ArrayNode actions() {
        ArrayNode actions = PairEventJson.MAPPER.createArrayNode();
        addAction(actions, 0, "p1", "preflop", "raise", 10.0);
        addAction(actions, 1, "p2", "preflop", "call", 10.0);
        addAction(actions, 2, "p1", "flop", "check", 0.0);
        addAction(actions, 3, "p2", "flop", "call", 5.0);
        addAction(actions, 4, "p2", "river", "fold", 0.0);
        return actions;
    }

    private static ObjectNode jdbcV2(ObjectNode root) {
        return pointInTimeV2(root, "postgresql");
    }

    private static ObjectNode snowflakeV2(ObjectNode root) {
        return pointInTimeV2(root, "snowflake");
    }

    private static ObjectNode pointInTimeV2(
            ObjectNode root, String source) {
        root.put("schema_version", 2);
        ObjectNode payload = (ObjectNode) root.path("payload");
        payload.remove("context_version");
        payload.remove("context_effective_at");
        payload.remove("source_context_event_id");
        payload.remove("allowed_lateness_ms");
        payload.remove("correction_window_ms");
        payload.remove("join_policy_version");
        ObjectNode resolution = payload.putObject("context_resolution");
        boolean snowflake = source.equals("snowflake");
        resolution.put(
                "mode",
                snowflake
                        ? "snowflake_point_in_time"
                        : "postgresql_point_in_time");
        resolution.put(
                "policy_version",
                snowflake
                        ? "snowflake-effective-at-v1"
                        : "jdbc-effective-at-v1");
        resolution.put("source", source);
        resolution.put(
                "context_record_id",
                UUID.nameUUIDFromBytes(
                                ("context-" + payload.path("player").path("player_id").asText())
                                        .getBytes())
                        .toString());
        resolution.put("context_version", 1);
        resolution.put("context_effective_at", "2025-01-01T00:00:00Z");
        return root;
    }

    private static void addAction(
            ArrayNode actions,
            int sequence,
            String player,
            String street,
            String type,
            double amount) {
        ObjectNode action = actions.addObject();
        action.put("sequence_no", sequence);
        action.put("player_id", player);
        action.put("street", street);
        action.put("action_type", type);
        action.put("amount", amount);
    }
}
