package com.aicampions.poker.context;

import java.util.Optional;

/** Point-in-time lookup boundary for active poker-user context. */
interface UserContextRepository extends AutoCloseable {
    Optional<UserContextRecord> findEffective(ContextKey key, long playedAtMs) throws Exception;

    @Override
    void close() throws Exception;
}
