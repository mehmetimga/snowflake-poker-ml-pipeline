package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.junit.jupiter.api.Test;

final class TemporalJoinLogicTest {
    private static final String HAND = """
            {
              "event_id":"11111111-1111-1111-1111-111111111111",
              "event_type":"poker.hand.completed",
              "schema_version":1,
              "tenant_id":"demo",
              "product_id":"poker",
              "dataset_id":"temporal-test-v1",
              "dataset_split":"train",
              "occurred_at":"2026-01-01T12:00:00Z",
              "emitted_at":"2026-01-01T12:00:00Z",
              "trace_id":"22222222-2222-2222-2222-222222222222",
              "payload":{
                "hand_id":"H-1",
                "table_id":"T-1",
                "played_at":"2026-01-01T12:00:00Z",
                "dataset_split":"train",
                "generator":"pokerkit",
                "small_blind":1.0,
                "big_blind":2.0,
                "num_players":2,
                "pot_size":4.0,
                "board":[],
                "actions":[],
                "players":[
                  {"player_id":"user-1","name":"One","position":"SB","stack_start":100.0,"hole_cards":"As Kd","won_amount":4.0},
                  {"player_id":"user-2","name":"Two","position":"BB","stack_start":100.0,"hole_cards":"Qc Jh","won_amount":0.0}
                ]
              }
            }
            """;

    @Test
    void selectsLatestEffectiveContextAndNeverFutureContext() {
        String oldContext = wrappedContext("c-1", 1, "2026-01-01T10:00:00Z", 1L);
        String latestContext = wrappedContext("c-3", 3, "2026-01-01T11:00:00Z", 2L);
        String futureContext = wrappedContext("c-2", 2, "2026-01-01T13:00:00Z", 3L);

        String selected = TemporalJoinLogic.selectContext(
                List.of(oldContext, latestContext, futureContext),
                EventJson.parseInstant("2026-01-01T12:00:00Z"));

        assertEquals("c-3", TemporalJoinLogic.contextEventId(selected));
    }

    @Test
    void buildsSchemaCompatibleOutputAndPythonCompatibleUuidV5() {
        String state = TemporalJoinLogic.newPlayerHandState(
                EventJson.expandPlayer(
                        EventJson.parse(HAND),
                        EventJson.parse(HAND).path("payload").path("players").get(0)),
                2L,
                30_000L,
                300_000L,
                0L,
                false);
        String context = wrappedContext("c-1", 1, "2026-01-01T11:00:00Z", 1L);

        JsonNode output = EventJson.parse(TemporalJoinLogic.enrich(
                state,
                context,
                "matched",
                1,
                30_000L,
                300_000L,
                EventJson.parseInstant("2026-01-01T12:00:30Z")));

        assertEquals("2472ea58-377f-585a-b742-0c9587f4cd39", output.path("event_id").asText());
        assertEquals("poker.hand-player-context.enriched", output.path("event_type").asText());
        assertEquals("user-1", output.path("payload").path("player").path("player_id").asText());
        assertEquals(1, output.path("payload").path("context_version").asInt());
        assertEquals("event-time-user-context-v1",
                output.path("payload").path("join_policy_version").asText());
    }

    @Test
    void missingOutputContainsExplicitNullContext() {
        String state = TemporalJoinLogic.newPlayerHandState(
                EventJson.expandPlayer(
                        EventJson.parse(HAND),
                        EventJson.parse(HAND).path("payload").path("players").get(0)),
                1L,
                1_000L,
                2_000L,
                0L,
                false);

        JsonNode output = EventJson.parse(TemporalJoinLogic.enrich(
                state,
                null,
                "missing",
                1,
                1_000L,
                2_000L,
                EventJson.parseInstant("2026-01-01T12:00:01Z")));

        assertEquals("missing", output.path("payload").path("context_status").asText());
        assertNull(output.path("payload").get("context").textValue());
    }

    @Test
    void validationRejectsPrivateTruthAnywhereInInferencePayload() {
        String unsafe = HAND.replace(
                "\"won_amount\":4.0",
                "\"won_amount\":4.0,\"is_suspicious\":true");

        assertThrows(
                IllegalArgumentException.class,
                () -> EventJson.validateEnvelope(unsafe, EventJson.HAND_COMPLETED));
    }

    private static String wrappedContext(
            String eventId, int version, String effectiveAt, long arrivalSequence) {
        String context = """
                {
                  "event_id":"%s",
                  "event_type":"poker.user-context.updated",
                  "schema_version":1,
                  "tenant_id":"demo",
                  "product_id":"poker",
                  "dataset_id":"temporal-test-v1",
                  "dataset_split":"train",
                  "occurred_at":"%s",
                  "emitted_at":"%s",
                  "trace_id":"33333333-3333-3333-3333-333333333333",
                  "payload":{
                    "user_id":"user-1",
                    "context_version":%d,
                    "effective_at":"%s",
                    "account_created_at":"2025-01-01T00:00:00Z",
                    "country_bucket":"TR",
                    "timezone":"Europe/Istanbul",
                    "acquisition_channel":"organic",
                    "kyc_level":"verified",
                    "account_status":"active",
                    "bankroll_bucket":"medium",
                    "preferred_stake_bucket":"low",
                    "skill_rating":0.5,
                    "device_id":"device-1",
                    "network_cluster_id":"network-1"
                  }
                }
                """.formatted(eventId, effectiveAt, effectiveAt, version, effectiveAt);
        return TemporalJoinLogic.wrapContext(context, arrivalSequence);
    }
}
