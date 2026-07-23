package com.aicampions.poker.context.adapter.jdbc;

import java.util.concurrent.ThreadLocalRandom;

/** Bounded delay applied once before retrying a transient JDBC failure. */
@FunctionalInterface
public interface JdbcRetryDelay {
    long MAXIMUM_ALLOWED_JITTER_MS = 5_000L;

    void pause() throws InterruptedException;

    static JdbcRetryDelay jittered(long maximumJitterMs) {
        if (maximumJitterMs < 0L
                || maximumJitterMs > MAXIMUM_ALLOWED_JITTER_MS) {
            throw new IllegalArgumentException(
                    "maximumJitterMs must be between 0 and "
                            + MAXIMUM_ALLOWED_JITTER_MS);
        }
        return () -> {
            if (maximumJitterMs == 0L) {
                return;
            }
            long delay = ThreadLocalRandom.current()
                    .nextLong(maximumJitterMs + 1L);
            if (delay > 0L) {
                Thread.sleep(delay);
            }
        };
    }
}
