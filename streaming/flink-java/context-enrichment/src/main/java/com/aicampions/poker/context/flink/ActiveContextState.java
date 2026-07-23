package com.aicampions.poker.context.flink;

import com.aicampions.poker.context.domain.ActiveContextCacheEntry;
import java.time.Duration;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

/** Declares the typed cache serializer, state name, TTL, and upgrade policy. */
public final class ActiveContextState {
    public static final String STATE_NAME =
            "active-user-context-cache-v1";
    public static final String LEGACY_JSON_STATE_NAME =
            "active-user-context-jdbc-v2";

    public enum RestorePolicy {
        REUSE_TYPED_STATE,
        REBUILD_DERIVED_CACHE,
        REJECT_UNKNOWN_STATE
    }

    private ActiveContextState() {}

    public static TypeInformation<ActiveContextCacheEntry>
            typeInformation() {
        return Types.POJO(ActiveContextCacheEntry.class);
    }

    public static ValueStateDescriptor<ActiveContextCacheEntry>
            descriptor(long cacheTtlHours) {
        if (cacheTtlHours < 1L) {
            throw new IllegalArgumentException(
                    "cacheTtlHours must be positive");
        }
        StateTtlConfig ttl = StateTtlConfig
                .newBuilder(Duration.ofHours(cacheTtlHours))
                .setUpdateType(
                        StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(
                        StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1_000L)
                .build();
        ValueStateDescriptor<ActiveContextCacheEntry> descriptor =
                new ValueStateDescriptor<>(
                        STATE_NAME,
                        typeInformation());
        descriptor.enableTimeToLive(ttl);
        return descriptor;
    }

    public static RestorePolicy restorePolicy(String stateName) {
        if (STATE_NAME.equals(stateName)) {
            return RestorePolicy.REUSE_TYPED_STATE;
        }
        if (LEGACY_JSON_STATE_NAME.equals(stateName)) {
            return RestorePolicy.REBUILD_DERIVED_CACHE;
        }
        return RestorePolicy.REJECT_UNKNOWN_STATE;
    }
}
