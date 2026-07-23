package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.snowflake.SnowflakeContextProxyRepository;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class SnowflakeContextProxyRepositoryTest {
    @Test
    void readsPointInTimeContextFromPrivateSidecar() throws Exception {
        byte[] response = """
                {
                  "tenant_id":"demo",
                  "product_id":"poker",
                  "user_id":"A",
                  "context_version":2,
                  "effective_at":"2026-07-23T10:00:00Z",
                  "account_created_at":"2025-07-23T10:00:00Z",
                  "country_bucket":"TR",
                  "timezone":"Europe/Istanbul",
                  "acquisition_channel":"organic",
                  "kyc_level":"full",
                  "account_status":"active",
                  "bankroll_bucket":"medium",
                  "preferred_stake_bucket":"low",
                  "skill_rating":0.72,
                  "device_id":"device-a",
                  "network_cluster_id":"network-a"
                }
                """.getBytes(StandardCharsets.UTF_8);
        HttpServer server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/user-context/lookup", exchange -> {
            exchange.sendResponseHeaders(200, response.length);
            exchange.getResponseBody().write(response);
            exchange.close();
        });
        server.start();
        try {
            SnowflakeContextProxyRepository repository =
                    new SnowflakeContextProxyRepository(
                            "http://127.0.0.1:" + server.getAddress().getPort(),
                            1,
                            1,
                            0,
                            JdbcRepositoryObserver.NOOP);

            UserContextRecord record = repository
                    .findEffective(
                            new ContextKey("demo", "poker", "A"),
                            Instant.parse("2026-07-23T12:00:00Z").toEpochMilli())
                    .orElseThrow();

            assertEquals(2, record.contextVersion());
            assertEquals("device-a", record.deviceId());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void mapsNotFoundToAnEmptyLookup() throws Exception {
        HttpServer server = HttpServer.create(
                new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/user-context/lookup", exchange -> {
            exchange.sendResponseHeaders(404, -1);
            exchange.close();
        });
        server.start();
        try {
            SnowflakeContextProxyRepository repository =
                    new SnowflakeContextProxyRepository(
                            "http://localhost:" + server.getAddress().getPort(),
                            1,
                            1,
                            0,
                            JdbcRepositoryObserver.NOOP);
            assertTrue(repository
                    .findEffective(
                            new ContextKey("demo", "poker", "missing"),
                            1)
                    .isEmpty());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void rejectsNonLocalOrCredentialBearingUrls() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new SnowflakeContextProxyRepository(
                        "https://context.example.com",
                        1,
                        1,
                        0,
                        JdbcRepositoryObserver.NOOP));
        assertThrows(
                IllegalArgumentException.class,
                () -> new SnowflakeContextProxyRepository(
                        "http://user:secret@127.0.0.1:8090",
                        1,
                        1,
                        0,
                        JdbcRepositoryObserver.NOOP));
    }
}
