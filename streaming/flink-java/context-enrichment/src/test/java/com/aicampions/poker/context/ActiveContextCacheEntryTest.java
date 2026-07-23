package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aicampions.poker.context.domain.ActiveContextCacheEntry;
import com.aicampions.poker.context.domain.UserContextRecord;
import java.time.Duration;
import java.time.Instant;
import org.junit.jupiter.api.Test;

final class ActiveContextCacheEntryTest {
    @Test
    void distinguishesFreshHitFromRefreshWithoutUsingResidencyTtl() {
        long loadedAtMs = 1_000L;
        long refreshAfterMs = Duration.ofMinutes(60L).toMillis();
        ActiveContextCacheEntry cached =
                ActiveContextCacheEntry.from(
                        record(1, "10:00:00"),
                        loadedAtMs);

        assertTrue(cached.isFresh(
                loadedAtMs + refreshAfterMs - 1L,
                refreshAfterMs));
        assertFalse(cached.isFresh(
                loadedAtMs + refreshAfterMs,
                refreshAfterMs));
    }

    @Test
    void neverUsesFutureContextForAnOlderHand() {
        ActiveContextCacheEntry cached =
                ActiveContextCacheEntry.from(record(1, "10:00:00"), 1_000L);

        assertTrue(cached.isEffectiveFor(
                EventJson.parseInstant("2026-07-23T10:00:00Z")));
        assertFalse(cached.isEffectiveFor(
                EventJson.parseInstant("2026-07-23T09:59:59Z")));
    }

    @Test
    void rejectsAnUnknownStateSchemaVersion() {
        ActiveContextCacheEntry cached =
                ActiveContextCacheEntry.from(record(1, "10:00:00"), 1_000L);
        cached.setStateSchemaVersion(2);

        assertThrows(IllegalStateException.class, cached::validate);
    }

    @Test
    void preservesDatabaseTimestampPrecisionAndRecordIdentity() {
        UserContextRecord record = new UserContextRecord(
                "demo",
                "poker",
                "A",
                4,
                Instant.parse("2026-07-23T10:00:00.123456Z"),
                Instant.parse("2025-01-01T00:00:00.654321Z"),
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

        ActiveContextCacheEntry cached =
                ActiveContextCacheEntry.from(record, 1_000L);

        assertEquals(record, cached.toRecord());
        assertEquals(
                record.contextRecordId().toString(),
                cached.getContextRecordId());
    }

    @Test
    void lateHandLookupCannotDowngradeNewerCachedContext() {
        ActiveContextCacheEntry current = ActiveContextCacheEntry.from(
                record(3, "13:00:00"), 3_000L);
        ActiveContextCacheEntry prior = ActiveContextCacheEntry.from(
                record(2, "10:00:00"), 4_000L);
        ActiveContextCacheEntry refreshedCurrent =
                ActiveContextCacheEntry.from(
                        record(3, "13:00:00"), 5_000L);

        assertFalse(current.shouldBeReplacedBy(prior));
        assertTrue(current.shouldBeReplacedBy(refreshedCurrent));
    }

    private static UserContextRecord record(
            int contextVersion,
            String effectiveTime) {
        return new UserContextRecord(
                "demo",
                "poker",
                "A",
                contextVersion,
                Instant.parse("2026-07-23T" + effectiveTime + "Z"),
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
