package com.aicampions.poker.features;

import java.time.Duration;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;

final class StateDescriptors {
    private StateDescriptors() {}

    static ValueStateDescriptor<String> stringValue(String name, long ttlHours) {
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>(name, Types.STRING);
        descriptor.enableTimeToLive(ttl(ttlHours));
        return descriptor;
    }

    static MapStateDescriptor<String, String> stringMap(String name, long ttlHours) {
        MapStateDescriptor<String, String> descriptor =
                new MapStateDescriptor<>(name, Types.STRING, Types.STRING);
        descriptor.enableTimeToLive(ttl(ttlHours));
        return descriptor;
    }

    static MapStateDescriptor<String, Integer> integerMap(String name, long ttlHours) {
        MapStateDescriptor<String, Integer> descriptor =
                new MapStateDescriptor<>(name, Types.STRING, Types.INT);
        descriptor.enableTimeToLive(ttl(ttlHours));
        return descriptor;
    }

    private static StateTtlConfig ttl(long ttlHours) {
        return StateTtlConfig.newBuilder(Duration.ofHours(ttlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
    }
}
