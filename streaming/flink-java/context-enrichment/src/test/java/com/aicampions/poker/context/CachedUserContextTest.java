package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

final class CachedUserContextTest {
    private static final String EVENT = """
            {
              "event_id":"11111111-1111-1111-1111-111111111111",
              "payload":{
                "user_id":"A",
                "effective_at":"2026-07-23T10:00:00Z"
              }
            }
            """;

    @Test
    void distinguishesFreshHitFromRefreshWithoutUsingResidencyTtl() {
        String cached = CachedUserContext.create(EVENT, 1_000L);

        assertTrue(CachedUserContext.isFresh(cached, 1_500L, 1_000L));
        assertFalse(CachedUserContext.isFresh(cached, 2_000L, 1_000L));
    }

    @Test
    void neverUsesFutureContextForAnOlderHand() {
        String cached = CachedUserContext.create(EVENT, 1_000L);

        assertTrue(CachedUserContext.isEffectiveFor(
                cached, EventJson.parseInstant("2026-07-23T10:00:00Z")));
        assertFalse(CachedUserContext.isEffectiveFor(
                cached, EventJson.parseInstant("2026-07-23T09:59:59Z")));
    }
}
