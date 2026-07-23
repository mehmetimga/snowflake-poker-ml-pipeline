package com.aicampions.poker.context.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/** Immutable, validated runtime configuration shared by the two explicit entrypoints. */
public record ContextJobConfig(
        String bootstrapServers,
        String handTopic,
        String contextTopic,
        String outputTopic,
        String deadLetterTopic,
        String groupId,
        String contextSource,
        String contextJdbcUrl,
        String contextJdbcTable,
        int contextJdbcQueryTimeoutSeconds,
        int contextJdbcConnectTimeoutSeconds,
        int contextJdbcValidationTimeoutSeconds,
        long contextJdbcRetryMaximumJitterMs,
        long contextCacheTtlHours,
        long contextRefreshMinutes,
        boolean fromBeginning,
        boolean bounded,
        long allowedLatenessMs,
        long correctionWindowMs,
        long idleSourceTimeoutMs,
        long stateTtlHours,
        long contextBootstrapWaitMs,
        long checkpointIntervalMs,
        int restartMaxFailuresPerInterval,
        long restartFailureRateIntervalMs,
        long restartDelayMs,
        int parallelism,
        boolean simulationMode,
        Properties kafkaProperties) {

    public static ContextJobConfig parse(
            String[] arguments, Map<String, String> environment) {
        Map<String, String> args = parseArguments(arguments);
        rejectSecretArgument(args, "context-jdbc-username");
        rejectSecretArgument(args, "context-jdbc-password");
        String bootstrap = value(
                args,
                environment,
                "bootstrap-servers",
                "KAFKA_BOOTSTRAP_SERVERS",
                "localhost:9092");
        String hands = value(
                args,
                environment,
                "hand-topic",
                "KAFKA_WORLD_HANDS_TOPIC",
                "poker.hands.raw.v1");
        String contextSource = value(
                args,
                environment,
                "context-source",
                "FLINK_CONTEXT_SOURCE",
                "kafka")
                .toLowerCase();
        if (!contextSource.equals("kafka") && !contextSource.equals("jdbc")) {
            throw new IllegalArgumentException(
                    "--context-source must be kafka or jdbc");
        }
        String contexts = value(
                args,
                environment,
                "context-topic",
                "KAFKA_USER_CONTEXT_TOPIC",
                "poker.user-context.v1");
        String output = contextSource.equals("jdbc")
                ? value(
                        args,
                        environment,
                        "output-topic",
                        "KAFKA_PLAYER_CONTEXT_V2_TOPIC",
                        "poker.hand-player-context.v2")
                : value(
                        args,
                        environment,
                        "output-topic",
                        "KAFKA_PLAYER_CONTEXT_TOPIC",
                        "poker.hand-player-context.v1");
        String dlq = value(
                args,
                environment,
                "dead-letter-topic",
                "KAFKA_DEAD_LETTER_TOPIC",
                "poker.pipeline.dead-letter.v1");
        String group = contextSource.equals("jdbc")
                ? value(
                        args,
                        environment,
                        "group-id",
                        "FLINK_ACTIVE_CONTEXT_GROUP_ID",
                        "flink-active-context-v2")
                : value(
                        args,
                        environment,
                        "group-id",
                        "FLINK_CONTEXT_GROUP_ID",
                        "flink-legacy-kafka-context-v1");
        String contextJdbcUrl = value(
                args,
                environment,
                "context-jdbc-url",
                "USER_CONTEXT_JDBC_URL",
                "");
        String contextJdbcTable = value(
                args,
                environment,
                "context-jdbc-table",
                "USER_CONTEXT_DB_TABLE",
                "public.poker_user_context");
        int contextJdbcQueryTimeoutSeconds = Math.toIntExact(boundedLongValue(
                args,
                environment,
                "context-jdbc-query-timeout-seconds",
                "FLINK_CONTEXT_JDBC_QUERY_TIMEOUT_SECONDS",
                1L,
                1L,
                60L));
        int contextJdbcConnectTimeoutSeconds = Math.toIntExact(boundedLongValue(
                args,
                environment,
                "context-jdbc-connect-timeout-seconds",
                "FLINK_CONTEXT_JDBC_CONNECT_TIMEOUT_SECONDS",
                3L,
                1L,
                60L));
        int contextJdbcValidationTimeoutSeconds = Math.toIntExact(boundedLongValue(
                args,
                environment,
                "context-jdbc-validation-timeout-seconds",
                "FLINK_CONTEXT_JDBC_VALIDATION_TIMEOUT_SECONDS",
                1L,
                1L,
                60L));
        long contextJdbcRetryMaximumJitterMs = boundedLongValue(
                args,
                environment,
                "context-jdbc-retry-max-jitter-ms",
                "FLINK_CONTEXT_JDBC_RETRY_MAX_JITTER_MS",
                100L,
                0L,
                5_000L);
        long contextCacheTtlHours = longValue(
                args,
                environment,
                "context-cache-ttl-hours",
                "FLINK_CONTEXT_CACHE_TTL_HOURS",
                36L,
                1L);
        long contextRefreshMinutes = longValue(
                args,
                environment,
                "context-refresh-minutes",
                "FLINK_CONTEXT_REFRESH_MINUTES",
                60L,
                1L);
        boolean fromBeginning = args.containsKey("from-beginning");
        boolean bounded = args.containsKey("bounded");
        long lateness = longValue(
                args,
                environment,
                "allowed-lateness-ms",
                "FLINK_CONTEXT_ALLOWED_LATENESS_MS",
                30_000L,
                0L);
        long corrections = longValue(
                args, "correction-window-ms", 300_000L, 0L);
        long idle = longValue(
                args,
                environment,
                "idle-source-timeout-ms",
                "FLINK_CONTEXT_IDLE_SOURCE_TIMEOUT_MS",
                60_000L,
                1L);
        long ttl = longValue(args, "state-ttl-hours", 720L, 1L);
        long bootstrapWait = longValue(
                args,
                "context-bootstrap-wait-ms",
                bounded ? 0L : 30_000L,
                0L);
        long checkpoint = longValue(
                args, "checkpoint-interval-ms", 30_000L, 0L);
        int restartMaxFailuresPerInterval = Math.toIntExact(longValue(
                args,
                environment,
                "restart-max-failures-per-interval",
                "FLINK_CONTEXT_RESTART_MAX_FAILURES_PER_INTERVAL",
                3L,
                1L));
        long restartFailureRateIntervalMs = longValue(
                args,
                environment,
                "restart-failure-rate-interval-ms",
                "FLINK_CONTEXT_RESTART_FAILURE_RATE_INTERVAL_MS",
                600_000L,
                1L);
        long restartDelayMs = longValue(
                args,
                environment,
                "restart-delay-ms",
                "FLINK_CONTEXT_RESTART_DELAY_MS",
                10_000L,
                0L);
        int parallelism = Math.toIntExact(
                longValue(args, "parallelism", 1L, 1L));
        boolean simulation = booleanValue(
                args,
                environment,
                "simulation-mode",
                "FLINK_SIMULATION_MODE",
                false);
        ContextJobConfig config = new ContextJobConfig(
                bootstrap,
                hands,
                contexts,
                output,
                dlq,
                group,
                contextSource,
                contextJdbcUrl,
                contextJdbcTable,
                contextJdbcQueryTimeoutSeconds,
                contextJdbcConnectTimeoutSeconds,
                contextJdbcValidationTimeoutSeconds,
                contextJdbcRetryMaximumJitterMs,
                contextCacheTtlHours,
                contextRefreshMinutes,
                fromBeginning,
                bounded,
                lateness,
                corrections,
                idle,
                ttl,
                bootstrapWait,
                checkpoint,
                restartMaxFailuresPerInterval,
                restartFailureRateIntervalMs,
                restartDelayMs,
                parallelism,
                simulation,
                kafkaProperties(environment));
        config.validateTopicBoundary();
        config.validateContextSource();
        return config;
    }

    public String safeSummary() {
        String contextDescription =
                contextSource.equals("jdbc")
                        ? "postgresql-point-in-time"
                        : contextTopic;
        return String.format(
                "{\"job\":\"context-enrichment\",\"hands\":\"%s\","
                        + "\"context_source\":\"%s\",\"contexts\":\"%s\","
                        + "\"output\":\"%s\","
                        + "\"allowed_lateness_ms\":%d,\"correction_window_ms\":%d,"
                        + "\"context_cache_ttl_hours\":%d,"
                        + "\"context_refresh_minutes\":%d,"
                        + "\"jdbc_connect_timeout_seconds\":%d,"
                        + "\"jdbc_query_timeout_seconds\":%d,"
                        + "\"jdbc_validation_timeout_seconds\":%d,"
                        + "\"jdbc_retry_max_jitter_ms\":%d,"
                        + "\"restart_max_failures_per_interval\":%d,"
                        + "\"restart_failure_rate_interval_ms\":%d,"
                        + "\"restart_delay_ms\":%d,\"simulation\":%s}",
                handTopic,
                contextSource,
                contextDescription,
                outputTopic,
                allowedLatenessMs,
                correctionWindowMs,
                contextCacheTtlHours,
                contextRefreshMinutes,
                contextJdbcConnectTimeoutSeconds,
                contextJdbcQueryTimeoutSeconds,
                contextJdbcValidationTimeoutSeconds,
                contextJdbcRetryMaximumJitterMs,
                restartMaxFailuresPerInterval,
                restartFailureRateIntervalMs,
                restartDelayMs,
                simulationMode);
    }

    public void requireContextSource(String requiredContextSource) {
        if (requiredContextSource != null
                && !requiredContextSource.equals(contextSource)) {
            throw new IllegalArgumentException(
                    "entrypoint requires context source "
                            + requiredContextSource);
        }
    }

    private void validateContextSource() {
        if (!contextSource.equals("jdbc")) {
            return;
        }
        if (contextJdbcUrl.isBlank()) {
            throw new IllegalArgumentException(
                    "USER_CONTEXT_JDBC_URL is required for JDBC context source");
        }
        JdbcTableName.validate(contextJdbcTable);
    }

    private void validateTopicBoundary() {
        if (simulationMode) {
            requireExact("hand", handTopic, "poker.sim.hands.raw.v1");
            if (contextSource.equals("kafka")) {
                requireExact(
                        "context",
                        contextTopic,
                        "poker.sim.user-context.v1");
            }
            requireExact(
                    "output",
                    outputTopic,
                    contextSource.equals("jdbc")
                            ? "poker.sim.hand-player-context.v2"
                            : "poker.sim.hand-player-context.v1");
            requireExact(
                    "dead-letter",
                    deadLetterTopic,
                    "poker.sim.pipeline.dead-letter.v1");
            requireExact(
                    "group",
                    groupId,
                    contextSource.equals("jdbc")
                            ? "flink-active-context-sim-v2"
                            : "flink-legacy-kafka-context-sim-v1");
            return;
        }
        String[] topics = contextSource.equals("kafka")
                ? new String[] {
                    handTopic,
                    contextTopic,
                    outputTopic,
                    deadLetterTopic
                }
                : new String[] {
                    handTopic,
                    outputTopic,
                    deadLetterTopic
                };
        for (String topic : topics) {
            if (topic.startsWith("poker.sim.")) {
                throw new IllegalArgumentException(
                        "production mode rejects simulation topic " + topic);
            }
        }
    }

    private static void requireExact(
            String role, String actual, String expected) {
        if (!expected.equals(actual)) {
            throw new IllegalArgumentException(
                    "simulation topic boundary: "
                            + role
                            + " must be "
                            + expected);
        }
    }

    private static void rejectSecretArgument(
            Map<String, String> args, String key) {
        if (args.containsKey(key)) {
            throw new IllegalArgumentException(
                    "--"
                            + key
                            + " is forbidden; mount TaskManager runtime credentials");
        }
    }

    private static Map<String, String> parseArguments(String[] arguments) {
        Map<String, String> parsed = new HashMap<>();
        for (int index = 0; index < arguments.length; index++) {
            String argument = arguments[index];
            if (!argument.startsWith("--")) {
                throw new IllegalArgumentException(
                        "unexpected argument: " + argument);
            }
            String key = argument.substring(2);
            if (key.equals("from-beginning")
                    || key.equals("bounded")
                    || key.equals("simulation-mode")) {
                parsed.put(key, "true");
                continue;
            }
            if (index + 1 >= arguments.length
                    || arguments[index + 1].startsWith("--")) {
                throw new IllegalArgumentException(
                        "missing value for --" + key);
            }
            parsed.put(key, arguments[++index]);
        }
        return parsed;
    }

    private static String value(
            Map<String, String> args,
            Map<String, String> environment,
            String argument,
            String environmentName,
            String fallback) {
        return args.getOrDefault(
                argument,
                environment.getOrDefault(environmentName, fallback));
    }

    private static long longValue(
            Map<String, String> args,
            String key,
            long fallback,
            long minimum) {
        long parsed = Long.parseLong(
                args.getOrDefault(key, Long.toString(fallback)));
        if (parsed < minimum) {
            throw new IllegalArgumentException(
                    "--" + key + " must be >= " + minimum);
        }
        return parsed;
    }

    private static long boundedLongValue(
            Map<String, String> args,
            Map<String, String> environment,
            String key,
            String environmentName,
            long fallback,
            long minimum,
            long maximum) {
        long parsed = longValue(
                args,
                environment,
                key,
                environmentName,
                fallback,
                minimum);
        if (parsed > maximum) {
            throw new IllegalArgumentException(
                    "--" + key + " must be <= " + maximum);
        }
        return parsed;
    }

    private static long longValue(
            Map<String, String> args,
            Map<String, String> environment,
            String key,
            String environmentName,
            long fallback,
            long minimum) {
        long parsed = Long.parseLong(value(
                args,
                environment,
                key,
                environmentName,
                Long.toString(fallback)));
        if (parsed < minimum) {
            throw new IllegalArgumentException(
                    "--" + key + " must be >= " + minimum);
        }
        return parsed;
    }

    private static boolean booleanValue(
            Map<String, String> args,
            Map<String, String> environment,
            String argument,
            String environmentName,
            boolean fallback) {
        String raw = value(
                args,
                environment,
                argument,
                environmentName,
                Boolean.toString(fallback));
        if (!raw.equalsIgnoreCase("true")
                && !raw.equalsIgnoreCase("false")) {
            throw new IllegalArgumentException(
                    "--" + argument + " must be true or false");
        }
        return Boolean.parseBoolean(raw);
    }

    private static Properties kafkaProperties(
            Map<String, String> environment) {
        Properties properties = new Properties();
        String protocol = environment.getOrDefault(
                "KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
        properties.setProperty("security.protocol", protocol);
        String mechanism = environment.get("KAFKA_SASL_MECHANISM");
        if (mechanism == null || mechanism.isBlank()) {
            return properties;
        }
        properties.setProperty("sasl.mechanism", mechanism);
        if (mechanism.equals("PLAIN")) {
            String username = environment.getOrDefault(
                    "KAFKA_SASL_USERNAME", "");
            String password = environment.getOrDefault(
                    "KAFKA_SASL_PASSWORD", "");
            properties.setProperty(
                    "sasl.jaas.config",
                    "org.apache.kafka.common.security.plain.PlainLoginModule required "
                            + "username=\""
                            + jaasEscape(username)
                            + "\" password=\""
                            + jaasEscape(password)
                            + "\";");
        }
        return properties;
    }

    private static String jaasEscape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
