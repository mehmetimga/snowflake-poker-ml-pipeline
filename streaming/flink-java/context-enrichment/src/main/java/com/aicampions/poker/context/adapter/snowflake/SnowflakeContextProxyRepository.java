package com.aicampions.poker.context.adapter.snowflake;

import com.aicampions.poker.context.adapter.jdbc.JdbcFailureClassifier;
import com.aicampions.poker.context.adapter.jdbc.JdbcRepositoryObserver;
import com.aicampions.poker.context.adapter.jdbc.JdbcRetryDelay;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.domain.UserContextRecord;
import com.aicampions.poker.context.port.UserContextRepository;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

/** Calls the private Python/Snowflake service-token sidecar over localhost. */
public final class SnowflakeContextProxyRepository
        implements UserContextRepository {
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpClient client;
    private final URI lookupUri;
    private final Duration requestTimeout;
    private final JdbcRetryDelay retryDelay;
    private final JdbcRepositoryObserver observer;

    public SnowflakeContextProxyRepository(
            String baseUrl,
            int connectTimeoutSeconds,
            int queryTimeoutSeconds,
            long retryMaximumJitterMs,
            JdbcRepositoryObserver observer) {
        URI base = URI.create(baseUrl);
        String host = base.getHost();
        if (!"http".equals(base.getScheme())
                || host == null
                || (!host.equals("127.0.0.1") && !host.equals("localhost"))
                || base.getUserInfo() != null
                || base.getQuery() != null
                || base.getFragment() != null) {
            throw new IllegalArgumentException(
                    "Snowflake context proxy must be a private localhost HTTP URL");
        }
        String normalized = base.toString().replaceAll("/+$", "");
        this.lookupUri = URI.create(normalized + "/v1/user-context/lookup");
        this.requestTimeout = Duration.ofSeconds(positive(
                queryTimeoutSeconds, "queryTimeoutSeconds"));
        this.client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(positive(
                        connectTimeoutSeconds, "connectTimeoutSeconds")))
                .build();
        this.retryDelay = JdbcRetryDelay.jittered(retryMaximumJitterMs);
        this.observer = observer;
    }

    @Override
    public Optional<UserContextRecord> findEffective(
            ContextKey key, long playedAtMs)
            throws Exception {
        key.validate();
        try {
            return lookup(key, playedAtMs);
        } catch (SQLException firstFailure) {
            JdbcFailureClassifier.Failure classified =
                    JdbcFailureClassifier.classify(firstFailure);
            if (classified.kind() != JdbcFailureClassifier.Kind.TRANSIENT) {
                throw firstFailure;
            }
            observer.retry(classified);
            retryDelay.pause();
            observer.reconnect();
            return lookup(key, playedAtMs);
        }
    }

    private Optional<UserContextRecord> lookup(
            ContextKey key, long playedAtMs)
            throws Exception {
        ObjectNode payload = JSON.createObjectNode()
                .put("tenant_id", key.getTenantId())
                .put("product_id", key.getProductId())
                .put("user_id", key.getPlayerId())
                .put("played_at_ms", playedAtMs);
        HttpRequest request = HttpRequest.newBuilder(lookupUri)
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(
                        JSON.writeValueAsString(payload)))
                .build();
        HttpResponse<String> response;
        try {
            response = client.send(
                    request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException error) {
            throw new SQLException(
                    "Snowflake context proxy is unavailable", "08006", error);
        }
        return switch (response.statusCode()) {
            case 200 -> Optional.of(record(JSON.readTree(response.body())));
            case 404 -> Optional.empty();
            case 400 -> throw new SQLException(
                    "Snowflake context proxy rejected the lookup", "22000");
            case 401, 403 -> throw new SQLException(
                    "Snowflake context proxy authorization failed", "28000");
            default -> {
                if (response.statusCode() >= 500) {
                    throw new SQLException(
                            "Snowflake context proxy is unavailable", "08006");
                }
                throw new SQLException(
                        "Unexpected Snowflake context proxy response", "58000");
            }
        };
    }

    @Override
    public void close() {
        // java.net.http.HttpClient owns no caller-managed resources.
    }

    private static UserContextRecord record(JsonNode row)
            throws SQLException {
        try {
            return new UserContextRecord(
                    text(row, "tenant_id"),
                    text(row, "product_id"),
                    text(row, "user_id"),
                    row.path("context_version").intValue(),
                    Instant.parse(text(row, "effective_at")),
                    Instant.parse(text(row, "account_created_at")),
                    text(row, "country_bucket"),
                    text(row, "timezone"),
                    text(row, "acquisition_channel"),
                    text(row, "kyc_level"),
                    text(row, "account_status"),
                    text(row, "bankroll_bucket"),
                    text(row, "preferred_stake_bucket"),
                    row.path("skill_rating").doubleValue(),
                    text(row, "device_id"),
                    text(row, "network_cluster_id"));
        } catch (RuntimeException error) {
            throw new SQLException(
                    "Invalid Snowflake context proxy response", "22000", error);
        }
    }

    private static String text(JsonNode row, String field) {
        String value = row.path(field).textValue();
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(
                    "missing context response field " + field);
        }
        return value;
    }

    private static int positive(int value, String name) {
        if (value < 1) {
            throw new IllegalArgumentException(name + " must be positive");
        }
        return value;
    }
}
