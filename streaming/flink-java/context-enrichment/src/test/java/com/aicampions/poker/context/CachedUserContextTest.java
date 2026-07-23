package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import org.junit.jupiter.api.Test;

final class CachedUserContextTest {
    @Test
    void distinguishesFreshHitFromRefreshWithoutUsingResidencyTtl() {
        String cached = CachedUserContext.create(record(), 1_000L);

        assertTrue(CachedUserContext.isFresh(cached, 1_500L, 1_000L));
        assertFalse(CachedUserContext.isFresh(cached, 2_000L, 1_000L));
    }

    @Test
    void neverUsesFutureContextForAnOlderHand() {
        String cached = CachedUserContext.create(record(), 1_000L);

        assertTrue(CachedUserContext.isEffectiveFor(
                cached, EventJson.parseInstant("2026-07-23T10:00:00Z")));
        assertFalse(CachedUserContext.isEffectiveFor(
                cached, EventJson.parseInstant("2026-07-23T09:59:59Z")));
    }

    private static UserContextRecord record() {
        return new UserContextRecord(
                "demo",
                "poker",
                "A",
                1,
                Instant.parse("2026-07-23T10:00:00Z"),
                Instant.parse("2025-01-01T00:00:00Z"),
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
