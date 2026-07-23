package com.aicampions.poker.context.port;

import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import java.util.Optional;

/** Point-in-time lookup boundary for active poker-user context. */
public interface UserContextRepository extends AutoCloseable {
    Optional<UserContextRecord> findEffective(ContextKey key, long playedAtMs) throws Exception;

    @Override
    void close() throws Exception;
}
