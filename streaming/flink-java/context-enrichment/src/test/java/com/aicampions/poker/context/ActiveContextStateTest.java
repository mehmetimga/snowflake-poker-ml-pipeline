package com.aicampions.poker.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.aicampions.poker.context.domain.ActiveContextCacheEntry;
import com.aicampions.poker.context.flink.ActiveContextState;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.junit.jupiter.api.Test;

final class ActiveContextStateTest {
    @Test
    void declaresVersionedPojoStateWithResidencyTtl() {
        ValueStateDescriptor<ActiveContextCacheEntry> descriptor =
                ActiveContextState.descriptor(36L);
        StateTtlConfig ttl = descriptor.getTtlConfig();

        assertEquals(
                ActiveContextState.STATE_NAME,
                descriptor.getName());
        assertEquals(
                ActiveContextCacheEntry.class,
                ActiveContextState.typeInformation().getTypeClass());
        assertTrue(ttl.isEnabled());
        assertEquals(
                StateTtlConfig.UpdateType.OnReadAndWrite,
                ttl.getUpdateType());
        assertEquals(
                StateTtlConfig.StateVisibility.NeverReturnExpired,
                ttl.getStateVisibility());
        assertEquals(Duration.ofHours(36L), ttl.getTimeToLive());
    }

    @Test
    void makesTheDerivedCacheUpgradePolicyExplicit() {
        assertEquals(
                ActiveContextState.RestorePolicy.REUSE_TYPED_STATE,
                ActiveContextState.restorePolicy(
                        ActiveContextState.STATE_NAME));
        assertEquals(
                ActiveContextState.RestorePolicy.REBUILD_DERIVED_CACHE,
                ActiveContextState.restorePolicy(
                        ActiveContextState.LEGACY_JSON_STATE_NAME));
        assertEquals(
                ActiveContextState.RestorePolicy.REJECT_UNKNOWN_STATE,
                ActiveContextState.restorePolicy("unexpected-state"));
    }
}
