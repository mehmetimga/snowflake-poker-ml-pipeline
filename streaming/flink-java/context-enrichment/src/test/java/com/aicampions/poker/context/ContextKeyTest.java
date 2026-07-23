package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.aicampions.poker.context.domain.ContextKey;
import org.junit.jupiter.api.Test;

final class ContextKeyTest {
    @Test
    void tenantAndProductArePartOfIdentity() {
        ContextKey key = new ContextKey("tenant-a", "poker", "player-1");

        assertEquals(key, new ContextKey("tenant-a", "poker", "player-1"));
        assertNotEquals(key, new ContextKey("tenant-b", "poker", "player-1"));
        assertNotEquals(key, new ContextKey("tenant-a", "casino", "player-1"));
        assertEquals("ContextKey[redacted]", key.toString());
    }

    @Test
    void rejectsIncompleteScope() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new ContextKey("", "poker", "player-1"));
    }
}
