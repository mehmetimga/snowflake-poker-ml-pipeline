package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

final class EventJsonDiagnosticTest {
    private static final String EXPANDED_HAND = """
            {
              "player_id":"player-1",
              "hand":{
                "event_id":"11111111-1111-1111-1111-111111111111",
                "tenant_id":"tenant-a",
                "product_id":"poker",
                "dataset_id":"dataset-a",
                "payload":{
                  "hand_id":"hand-1",
                  "players":[{"player_id":"player-1","hole_cards":"As Kd"}]
                }
              }
            }
            """;

    @Test
    void diagnosticKeepsReferencesButNeverCopiesTheRawHand() {
        String diagnostic = EventJson.deadLetter(
                "poker.hands.raw.v1",
                "jdbc-user-context-not-found",
                "context-not-found",
                EXPANDED_HAND);
        JsonNode value = EventJson.parse(diagnostic);

        assertEquals("tenant-a", value.path("tenant_id").asText());
        assertEquals("player-1", value.path("player_id").asText());
        assertEquals(64, value.path("raw_value_sha256").asText().length());
        assertTrue(value.path("raw_value_bytes").asInt() > 0);
        assertFalse(value.has("raw_value"));
        assertFalse(diagnostic.contains("As Kd"));
    }
}
