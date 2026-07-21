package com.aicampions.poker.features;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class StatefulFoldRuleEngineTest {
    private static final StatefulFoldRuleEngine.Config CONFIG =
            new StatefulFoldRuleEngine.Config(
                    24 * 3_600_000L, 5, 3, 0.6, 120_000L, 48 * 3_600_000L);

    @Test
    void goldenReplayCorrectionLatenessAndCheckpointRestoreMatchPython() throws Exception {
        JsonNode fixture = PairEventJson.MAPPER.readTree(Files.readString(
                Path.of("../../../schemas/examples/stateful-fold-rule-v1.golden.json")));
        String state = null;
        String restoredState = null;

        for (int index = 0; index < fixture.path("operations").size(); index++) {
            JsonNode operation = fixture.path("operations").get(index);
            JsonNode event = pairEvent(fixture.path("scope"), operation);
            long watermark = operation.path("watermark").isTextual()
                    ? Instant.parse(operation.path("watermark").asText()).toEpochMilli()
                    : Long.MIN_VALUE;
            StatefulFoldRuleEngine.Evaluation result =
                    StatefulFoldRuleEngine.evaluate(state, event, watermark, CONFIG);
            state = result.stateJson();
            assertExpected(operation, result);

            if (restoredState != null) {
                StatefulFoldRuleEngine.Evaluation restored =
                        StatefulFoldRuleEngine.evaluate(
                                restoredState, event, watermark, CONFIG);
                restoredState = restored.stateJson();
                assertEquals(identity(result), identity(restored));
            }
            if (index == 3) {
                restoredState = state;
            }
        }

        assertEquals(state, restoredState);
        assertEquals(6, PairEventJson.parse(state).path("observations").size());
    }

    @Test
    void conflictingSameRevisionFailsClosed() throws Exception {
        JsonNode fixture = PairEventJson.MAPPER.readTree(Files.readString(
                Path.of("../../../schemas/examples/stateful-fold-rule-v1.golden.json")));
        JsonNode operation = fixture.path("operations").get(0);
        ObjectNode event = pairEvent(fixture.path("scope"), operation);
        String state = StatefulFoldRuleEngine.evaluate(
                null, event, Long.MIN_VALUE, CONFIG).stateJson();
        ((ObjectNode) event.path("payload").path("current_hand"))
                .put("fold_actions_a", 0);

        assertThrows(
                IllegalArgumentException.class,
                () -> StatefulFoldRuleEngine.evaluate(
                        state, event, Long.MIN_VALUE, CONFIG));
    }

    private static void assertExpected(
            JsonNode expected,
            StatefulFoldRuleEngine.Evaluation actual) {
        assertEquals(expected.path("expected_status").asText(), actual.status());
        assertEquals(expected.path("expected_hands").asInt(), actual.windowHandCount());
        assertEquals(expected.path("expected_count").asInt(), actual.directionalCount());
        assertEquals(
                expected.path("expected_rate").asDouble(),
                actual.directionalRate(),
                1e-12);
        if (expected.path("expected_rule_event_id").isNull()) {
            assertNull(actual.evidenceEvent());
        } else {
            assertNotNull(actual.evidenceEvent());
            assertEquals(
                    expected.path("expected_rule_event_id").asText(),
                    actual.evidenceEvent().path("event_id").asText());
            assertEquals(
                    expected.path("snapshot_revision").asInt(),
                    actual.evidenceEvent().path("payload")
                            .path("observation_revision").asInt());
        }
    }

    private static String identity(StatefulFoldRuleEngine.Evaluation value) {
        return String.join(
                "|",
                value.status(),
                Integer.toString(value.windowHandCount()),
                Integer.toString(value.directionalCount()),
                Double.toString(value.directionalRate()),
                value.evidenceEvent() == null
                        ? ""
                        : value.evidenceEvent().path("event_id").asText());
    }

    private static ObjectNode pairEvent(JsonNode scope, JsonNode operation) {
        ObjectNode root = PairEventJson.MAPPER.createObjectNode();
        root.put("event_id", operation.path("event_id").asText());
        root.put("event_type", PairEventJson.PAIR_FEATURES);
        root.put("schema_version", 1);
        root.put("tenant_id", scope.path("tenant_id").asText());
        root.put("product_id", scope.path("product_id").asText());
        root.put("dataset_id", scope.path("dataset_id").asText());
        root.put("dataset_split", scope.path("dataset_split").asText());
        root.put("occurred_at", operation.path("played_at").asText());
        root.put("emitted_at", operation.path("emitted_at").asText());
        root.put("trace_id", scope.path("trace_id").asText());
        ObjectNode payload = root.putObject("payload");
        payload.put("hand_id", operation.path("hand_id").asText());
        payload.put("table_id", "table-golden");
        payload.put("played_at", operation.path("played_at").asText());
        payload.put("pair_key", scope.path("pair_key").asText());
        payload.put("snapshot_revision", operation.path("snapshot_revision").asInt());
        payload.put("feature_definition_version", PairEventJson.FEATURE_VERSION);
        ObjectNode current = payload.putObject("current_hand");
        boolean aFold = operation.path("a_fold_b_win").asBoolean();
        boolean bFold = operation.path("b_fold_a_win").asBoolean();
        current.put("fold_actions_a", aFold ? 1 : 0);
        current.put("fold_actions_b", bFold ? 1 : 0);
        current.put("won_amount_a", bFold ? 10.0 : 0.0);
        current.put("won_amount_b", aFold ? 10.0 : 0.0);
        return root;
    }
}
