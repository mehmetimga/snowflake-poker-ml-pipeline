package com.aicampions.poker.context.flink;

import com.aicampions.poker.context.DeadLetters;
import com.aicampions.poker.context.EventJson;
import com.aicampions.poker.context.adapter.jdbc.JdbcCredentials;
import com.aicampions.poker.context.adapter.jdbc.JdbcFailureClassifier;
import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.jdbc.JdbcUserContextRepository;
import com.aicampions.poker.context.adapter.jdbc.UserContextLookupException;
import com.aicampions.poker.context.adapter.snowflake.SnowflakeContextProxyRepository;
import com.aicampions.poker.context.contract.JdbcEnrichedEventV2;
import com.aicampions.poker.context.domain.ActiveContextCacheEntry;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.aicampions.poker.context.port.UserContextRepository;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/** Lazy synchronous enrichment with one TTL-managed cache entry per active player. */
public final class JdbcContextEnrichmentFunction
        extends KeyedProcessFunction<ContextKey, String, String> {
    private final String contextSource;
    private final String jdbcUrl;
    private final String snowflakeProxyUrl;
    private final String tableName;
    private final int queryTimeoutSeconds;
    private final int connectTimeoutSeconds;
    private final int validationTimeoutSeconds;
    private final long retryMaximumJitterMs;
    private final long cacheTtlHours;
    private final long refreshAfterMs;
    private final String handTopic;

    private transient UserContextRepository repository;
    private transient ValueState<ActiveContextCacheEntry>
            cachedContext;
    private transient Counter cacheHits;
    private transient Counter cacheMisses;
    private transient Counter cacheRefreshes;
    private transient Counter lookupFailures;
    private transient Counter lookupFound;
    private transient Counter contextNotFound;
    private transient Counter lookupRetries;
    private transient Counter lookupReconnects;
    private transient Counter transientFailures;
    private transient Counter authorizationFailures;
    private transient Counter configurationFailures;
    private transient Counter dataFailures;
    private transient Counter unknownFailures;
    private transient AtomicLong latestLookupLatencyMs;

    public JdbcContextEnrichmentFunction(
            String contextSource,
            String jdbcUrl,
            String snowflakeProxyUrl,
            String tableName,
            int queryTimeoutSeconds,
            int connectTimeoutSeconds,
            int validationTimeoutSeconds,
            long retryMaximumJitterMs,
            long cacheTtlHours,
            long refreshMinutes,
            String handTopic) {
        this.contextSource = contextSource;
        this.jdbcUrl = jdbcUrl;
        this.snowflakeProxyUrl = snowflakeProxyUrl;
        this.tableName = tableName;
        this.queryTimeoutSeconds = queryTimeoutSeconds;
        this.connectTimeoutSeconds = connectTimeoutSeconds;
        this.validationTimeoutSeconds = validationTimeoutSeconds;
        this.retryMaximumJitterMs = retryMaximumJitterMs;
        this.cacheTtlHours = cacheTtlHours;
        this.refreshAfterMs = Duration.ofMinutes(refreshMinutes).toMillis();
        this.handTopic = handTopic;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        cachedContext = getRuntimeContext().getState(
                ActiveContextState.descriptor(cacheTtlHours));
        cacheHits = counter("context_cache_hits");
        cacheMisses = counter("context_cache_misses");
        cacheRefreshes = counter("context_cache_refreshes");
        lookupFailures = counter("context_lookup_failures");
        lookupFound = counter("context_lookup_found");
        contextNotFound = counter("context_lookup_not_found");
        lookupRetries = counter("context_lookup_retries");
        lookupReconnects = counter("context_lookup_reconnects");
        transientFailures = counter("context_lookup_failure_transient");
        authorizationFailures =
                counter("context_lookup_failure_authentication_or_authorization");
        configurationFailures = counter("context_lookup_failure_configuration");
        dataFailures = counter("context_lookup_failure_data");
        unknownFailures = counter("context_lookup_failure_unknown");
        latestLookupLatencyMs = new AtomicLong();
        getRuntimeContext()
                .getMetricGroup()
                .gauge(
                        "context_lookup_latency_ms",
                        latestLookupLatencyMs::get);
        try {
            JdbcRepositoryObserver observer =
                    new JdbcRepositoryObserver() {
                        @Override
                        public void retry(
                                JdbcFailureClassifier.Failure failure) {
                            lookupRetries.inc();
                        }

                        @Override
                        public void reconnect() {
                            lookupReconnects.inc();
                        }
                    };
            if (contextSource.equals("snowflake")) {
                repository = new SnowflakeContextProxyRepository(
                        snowflakeProxyUrl,
                        connectTimeoutSeconds,
                        queryTimeoutSeconds,
                        retryMaximumJitterMs,
                        observer);
            } else {
                JdbcCredentials credentials =
                        JdbcCredentials.fromEnvironment(System.getenv());
                repository = new JdbcUserContextRepository(
                        jdbcUrl,
                        credentials.username(),
                        credentials.password(),
                        tableName,
                        queryTimeoutSeconds,
                        connectTimeoutSeconds,
                        validationTimeoutSeconds,
                        retryMaximumJitterMs,
                        observer);
            }
        } catch (Exception error) {
            lookupFailures.inc();
            JdbcFailureClassifier.Failure failure = JdbcFailureClassifier.classify(error);
            incrementFailureCounter(failure.kind());
            throw new UserContextLookupException(failure);
        }
    }

    @Override
    public void processElement(String expandedHand, Context context, Collector<String> output)
            throws Exception {
        long nowMs = context.timerService().currentProcessingTime();
        long playedAtMs = EventJson.parseInstant(EventJson.requireText(
                EventJson.parse(expandedHand).path("hand").path("payload"), "played_at"));
        ContextKey contextKey = EventJson.contextKeyFromExpandedHand(expandedHand);
        ActiveContextCacheEntry cached = cachedContext.value();
        ActiveContextCacheEntry resolvedContext;

        if (cached != null
                && cached.isFresh(nowMs, refreshAfterMs)
                && cached.isEffectiveFor(playedAtMs)) {
            cacheHits.inc();
            resolvedContext = cached;
        } else {
            if (cached == null) {
                cacheMisses.inc();
            } else {
                cacheRefreshes.inc();
            }
            Optional<UserContextRecord> loaded;
            long lookupStartedNs = System.nanoTime();
            try {
                loaded = repository.findEffective(contextKey, playedAtMs);
            } catch (Exception error) {
                lookupFailures.inc();
                JdbcFailureClassifier.Failure failure =
                        JdbcFailureClassifier.classify(error);
                incrementFailureCounter(failure.kind());
                throw new UserContextLookupException(failure);
            } finally {
                latestLookupLatencyMs.set(Math.max(
                        0L,
                        TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - lookupStartedNs)));
            }
            if (loaded.isEmpty()) {
                contextNotFound.inc();
                context.output(
                        DeadLetters.TAG,
                        EventJson.deadLetter(
                                handTopic,
                                contextSource
                                        + "-user-context-not-found",
                                "context-not-found",
                                expandedHand));
                return;
            }
            lookupFound.inc();
            UserContextRecord record = loaded.orElseThrow();
            ContextKey loadedKey =
                    new ContextKey(record.tenantId(), record.productId(), record.userId());
            if (!contextKey.equals(loadedKey)) {
                throw new IllegalArgumentException("loaded context scope does not match state key");
            }
            resolvedContext =
                    ActiveContextCacheEntry.from(record, nowMs);
            if (cached == null
                    || cached.shouldBeReplacedBy(resolvedContext)) {
                cachedContext.update(resolvedContext);
            }
        }

        output.collect(
                JdbcEnrichedEventV2.create(
                        expandedHand, resolvedContext, contextSource));
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

    private void incrementFailureCounter(JdbcFailureClassifier.Kind kind) {
        switch (kind) {
            case TRANSIENT -> transientFailures.inc();
            case AUTHENTICATION_OR_AUTHORIZATION -> authorizationFailures.inc();
            case CONFIGURATION -> configurationFailures.inc();
            case DATA -> dataFailures.inc();
            case UNKNOWN -> unknownFailures.inc();
        }
    }
}
