package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class JdbcEnrichedEventV2Test {
    private static final String HAND = """
            {
              "event_id":"11111111-1111-5111-8111-111111111111",
              "event_type":"poker.hand.completed",
              "schema_version":1,
              "tenant_id":"demo",
              "product_id":"poker",
              "dataset_id":"synthetic-context-v2",
              "dataset_split":"live",
              "occurred_at":"2026-07-23T12:00:00Z",
              "emitted_at":"2026-07-23T12:00:00Z",
              "trace_id":"33333333-3333-4333-8333-333333333333",
              "payload":{
                "hand_id":"HAND-1",
                "table_id":"TABLE-1",
                "played_at":"2026-07-23T12:00:00Z",
                "small_blind":1.0,
                "big_blind":2.0,
                "num_players":2,
                "pot_size":4.0,
                "board":[],
                "actions":[],
                "players":[
                  {"player_id":"A","name":"A","position":"SB","stack_start":100.0,"hole_cards":"As Kd","won_amount":4.0},
                  {"player_id":"B","name":"B","position":"BB","stack_start":100.0,"hole_cards":"Qc Jh","won_amount":0.0}
                ]
              }
            }
            """;

    @Test
    void emitsDeterministicExplicitPostgresLineageWithoutLegacyJoinFields() {
        String expanded = EventJson.expandPlayer(
                EventJson.parse(HAND),
                EventJson.parse(HAND).path("payload").path("players").get(0));
        UserContextRecord record = record();
        String first = JdbcEnrichedEventV2.create(
                expanded, CachedUserContext.create(record, 1_000L));
        String replay = JdbcEnrichedEventV2.create(
                expanded, CachedUserContext.create(record, 9_000L));
        JsonNode output = EventJson.parse(first);
        JsonNode payload = output.path("payload");
        JsonNode resolution = payload.path("context_resolution");

        assertEquals(first, replay);
        assertEquals(2, output.path("schema_version").asInt());
        assertEquals("postgresql_point_in_time", resolution.path("mode").asText());
        assertEquals("jdbc-effective-at-v1", resolution.path("policy_version").asText());
        assertEquals(record.contextRecordId().toString(),
                resolution.path("context_record_id").asText());
        assertEquals(1, resolution.path("context_version").asInt());
        assertFalse(payload.has("source_context_event_id"));
        assertFalse(payload.has("join_policy_version"));
        assertFalse(payload.has("allowed_lateness_ms"));
        assertFalse(payload.has("correction_window_ms"));
    }

    private static UserContextRecord record() {
        return new UserContextRecord(
                "demo",
                "poker",
                "A",
                1,
                Instant.parse("2026-07-23T11:00:00Z"),
                Instant.parse("2025-07-23T11:00:00Z"),
                "TR",
                "Europe/Istanbul",
                "organic",
                "verified",
                "active",
                "medium",
                "low",
                0.5,
                "device-a",
                "network-a");
    }
}
