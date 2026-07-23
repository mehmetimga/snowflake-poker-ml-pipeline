package com.aicampions.poker.context;

import java.time.Duration;
import java.util.Optional;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Lazy synchronous JDBC enrichment with one TTL-managed cache entry per active player. */
final class JdbcContextEnrichmentFunction
        extends KeyedProcessFunction<String, String, String> {
    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String tableName;
    private final int queryTimeoutSeconds;
    private final long cacheTtlHours;
    private final long refreshAfterMs;
    private final String handTopic;

    private transient UserContextRepository repository;
    private transient ValueState<String> cachedContext;
    private transient Counter cacheHits;
    private transient Counter cacheMisses;
    private transient Counter cacheRefreshes;
    private transient Counter lookupFailures;
    private transient Counter contextNotFound;

    JdbcContextEnrichmentFunction(
            String jdbcUrl,
            String username,
            String password,
            String tableName,
            int queryTimeoutSeconds,
            long cacheTtlHours,
            long refreshMinutes,
            String handTopic) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.cacheTtlHours = cacheTtlHours;
        this.refreshAfterMs = Duration.ofMinutes(refreshMinutes).toMillis();
        this.handTopic = handTopic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StateTtlConfig ttl = StateTtlConfig.newBuilder(Duration.ofHours(cacheTtlHours))
                .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(1_000L)
                .build();
        ValueStateDescriptor<String> descriptor =
                new ValueStateDescriptor<>("active-user-context-jdbc-v1", Types.STRING);
        descriptor.enableTimeToLive(ttl);
        cachedContext = getRuntimeContext().getState(descriptor);
        repository = new JdbcUserContextRepository(
                jdbcUrl, username, password, tableName, queryTimeoutSeconds);
        cacheHits = counter("context_cache_hits");
        cacheMisses = counter("context_cache_misses");
        cacheRefreshes = counter("context_cache_refreshes");
        lookupFailures = counter("context_lookup_failures");
        contextNotFound = counter("context_not_found");
    }

    @Override
    public void processElement(String expandedHand, Context context, Collector<String> output)
            throws Exception {
        long nowMs = context.timerService().currentProcessingTime();
        long playedAtMs = EventJson.parseInstant(EventJson.requireText(
                EventJson.parse(expandedHand).path("hand").path("payload"), "played_at"));
        String playerId = EventJson.playerIdFromExpandedHand(expandedHand);
        String cached = cachedContext.value();
        String contextEvent;

        if (cached != null
                && CachedUserContext.isFresh(cached, nowMs, refreshAfterMs)
                && CachedUserContext.isEffectiveFor(cached, playedAtMs)) {
            cacheHits.inc();
            contextEvent = CachedUserContext.event(cached);
        } else {
            if (cached == null) {
                cacheMisses.inc();
            } else {
                cacheRefreshes.inc();
            }
            Optional<UserContextRecord> loaded;
            try {
                loaded = repository.findEffective(playerId, playedAtMs);
            } catch (Exception error) {
                lookupFailures.inc();
                context.output(
                        DeadLetters.TAG,
                        EventJson.deadLetter(
                                handTopic,
                                "jdbc-user-context-lookup",
                                error.getClass().getSimpleName() + ": " + safeMessage(error),
                                expandedHand));
                return;
            }
            if (loaded.isEmpty()) {
                contextNotFound.inc();
                context.output(
                        DeadLetters.TAG,
                        EventJson.deadLetter(
                                handTopic,
                                "jdbc-user-context-not-found",
                                "no effective user context for player_id=" + playerId,
                                expandedHand));
                return;
            }
            contextEvent = loaded.orElseThrow().toCanonicalEvent(expandedHand);
            cachedContext.update(CachedUserContext.create(contextEvent, nowMs));
        }

        String handState = TemporalJoinLogic.newPlayerHandState(
                expandedHand, 1L, 0L, 0L, nowMs, false);
        output.collect(TemporalJoinLogic.enrich(
                handState,
                TemporalJoinLogic.wrapContext(contextEvent, 1L),
                "matched",
                1,
                0L,
                0L,
                nowMs));
    }

    @Override
    public void close() throws Exception {
        if (repository != null) {
            repository.close();
        }
    }

    private Counter counter(String name) {
        return getRuntimeContext().getMetricGroup().counter(name);
    }

    private static String safeMessage(Exception error) {
        String message = error.getMessage();
        if (message == null || message.isBlank()) {
            return "lookup failed";
        }
        return message.length() <= 240 ? message : message.substring(0, 240);
    }
}
