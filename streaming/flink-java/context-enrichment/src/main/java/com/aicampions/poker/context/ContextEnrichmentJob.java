package com.aicampions.poker.context;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/** Compatibility dispatcher; deployments use one of the source-specific entrypoints. */
public final class ContextEnrichmentJob {
    private ContextEnrichmentJob() {}

    public static void main(String[] arguments) throws Exception {
        run(arguments, null);
    }

    public static void runActive(String[] arguments) throws Exception {
        run(arguments, "jdbc");
    }

    public static void runLegacy(String[] arguments) throws Exception {
        run(arguments, "kafka");
    }

    private static void run(String[] arguments, String requiredContextSource) throws Exception {
        JobConfig config = JobConfig.parse(arguments, System.getenv());
        config.requireContextSource(requiredContextSource);
        boolean active = config.contextSource().equals("jdbc");
        String uidPrefix = active ? "active-context-v2" : "legacy-kafka-context-v1";
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(config.parallelism());
        if (config.checkpointIntervalMs() > 0) {
            environment.enableCheckpointing(config.checkpointIntervalMs());
        }

        KafkaSource<String> handSource = source(
                config, config.handTopic(), config.groupId() + "-hands");

        SingleOutputStreamOperator<String> validHands = environment
                .fromSource(
                        handSource,
                        WatermarkStrategy.noWatermarks(),
                        "canonical-hands-source")
                .uid(uidPrefix + "-hands-source")
                .process(new EnvelopeValidationFunction(
                        config.handTopic(), EventJson.HAND_COMPLETED,
                        config.simulationMode()), Types.STRING)
                .name("validate-hand-envelopes")
                .uid(uidPrefix + "-validate-hands");
        WatermarkStrategy<String> eventTime = WatermarkStrategy
                .<String>forMonotonousTimestamps()
                .withTimestampAssigner(
                        (value, previousTimestamp) -> EventJson.occurredAtMs(value))
                .withIdleness(Duration.ofMillis(config.idleSourceTimeoutMs()));

        DataStream<String> handPlayers = validHands
                .assignTimestampsAndWatermarks(eventTime)
                .name("hand-event-time")
                .uid(uidPrefix + "-hand-event-time")
                .flatMap(new HandPlayerExpander(), Types.STRING)
                .name("expand-hand-players")
                .uid(uidPrefix + "-expand-hand-players");
        SingleOutputStreamOperator<String> enriched;
        DataStream<String> deadLetters;
        if (config.contextSource().equals("jdbc")) {
            enriched = handPlayers
                    .keyBy(
                            EventJson::contextKeyFromExpandedHand,
                            Types.POJO(ContextKey.class))
                    .process(
                            new JdbcContextEnrichmentFunction(
                                    config.contextJdbcUrl(),
                                    config.contextJdbcTable(),
                                    config.contextJdbcQueryTimeoutSeconds(),
                                    config.contextCacheTtlHours(),
                                    config.contextRefreshMinutes(),
                                    config.handTopic()),
                            Types.STRING)
                    .name("jdbc-active-user-context-lookup")
                    .uid("active-context-v2-jdbc-lookup");
            deadLetters = validHands.getSideOutput(DeadLetters.TAG)
                    .union(enriched.getSideOutput(DeadLetters.TAG));
        } else {
            KafkaSource<String> contextSource = source(
                    config, config.contextTopic(), config.groupId() + "-contexts");
            SingleOutputStreamOperator<String> validContexts = environment
                    .fromSource(
                            contextSource,
                            WatermarkStrategy.noWatermarks(),
                            "user-context-source")
                    .uid("legacy-kafka-context-v1-context-source")
                    .process(new EnvelopeValidationFunction(
                            config.contextTopic(), EventJson.USER_CONTEXT_UPDATED,
                            config.simulationMode()), Types.STRING)
                    .name("validate-context-envelopes")
                    .uid("legacy-kafka-context-v1-validate-contexts");
            DataStream<String> contexts = validContexts
                    .assignTimestampsAndWatermarks(eventTime)
                    .name("context-event-time")
                    .uid("legacy-kafka-context-v1-context-event-time");
            enriched = handPlayers
                    .keyBy(
                            EventJson::contextKeyFromExpandedHand,
                            Types.POJO(ContextKey.class))
                    .connect(contexts.keyBy(
                            EventJson::contextKeyFromContextEvent,
                            Types.POJO(ContextKey.class)))
                    .process(
                            new ContextTemporalJoinFunction(
                                    config.allowedLatenessMs(),
                                    config.correctionWindowMs(),
                                    config.stateTtlHours(),
                                    config.contextBootstrapWaitMs(),
                                    config.contextTopic()),
                            Types.STRING)
                    .name("event-time-user-context-join")
                    .uid("legacy-kafka-context-v1-temporal-join");
            deadLetters = validHands.getSideOutput(DeadLetters.TAG)
                    .union(
                            validContexts.getSideOutput(DeadLetters.TAG),
                            enriched.getSideOutput(DeadLetters.TAG));
        }

        KafkaSink<String> enrichedSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(config.outputTopic(), true))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        enriched.sinkTo(enrichedSink)
                .name("enriched-player-hand-sink")
                .uid(uidPrefix + "-enriched-sink");

        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(config.deadLetterTopic(), false))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        deadLetters.sinkTo(deadLetterSink)
                .name("context-enrichment-dead-letter-sink")
                .uid(uidPrefix + "-dead-letter-sink");

        System.out.println(config.safeSummary());
        environment.execute(active
                ? "poker-active-context-enrichment-v2"
                : "poker-legacy-kafka-temporal-context-v1");
    }

    private static KafkaSource<String> source(
            JobConfig config, String topic, String groupId) {
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(
                        config.fromBeginning()
                                ? OffsetsInitializer.earliest()
                                : OffsetsInitializer.latest())
                .setProperties(config.kafkaProperties())
                .setValueOnlyDeserializer(new SimpleStringSchema());
        if (config.bounded()) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        return builder.build();
    }

    record JobConfig(
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
            int parallelism,
            boolean simulationMode,
            Properties kafkaProperties) {

        static JobConfig parse(String[] arguments, Map<String, String> environment) {
            Map<String, String> args = parseArguments(arguments);
            rejectSecretArgument(args, "context-jdbc-username");
            rejectSecretArgument(args, "context-jdbc-password");
            String bootstrap = value(args, environment, "bootstrap-servers",
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            String hands = value(args, environment, "hand-topic",
                    "KAFKA_WORLD_HANDS_TOPIC", "poker.hands.raw.v1");
            String contextSource = value(
                    args, environment, "context-source", "FLINK_CONTEXT_SOURCE", "kafka")
                    .toLowerCase();
            if (!contextSource.equals("kafka") && !contextSource.equals("jdbc")) {
                throw new IllegalArgumentException(
                        "--context-source must be kafka or jdbc");
            }
            String contexts = value(args, environment, "context-topic",
                    "KAFKA_USER_CONTEXT_TOPIC", "poker.user-context.v1");
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
            String dlq = value(args, environment, "dead-letter-topic",
                    "KAFKA_DEAD_LETTER_TOPIC", "poker.pipeline.dead-letter.v1");
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
                    args, environment, "context-jdbc-url", "USER_CONTEXT_JDBC_URL", "");
            String contextJdbcTable = value(
                    args,
                    environment,
                    "context-jdbc-table",
                    "USER_CONTEXT_DB_TABLE",
                    "public.poker_user_context");
            int contextJdbcQueryTimeoutSeconds = Math.toIntExact(longValue(
                    args,
                    environment,
                    "context-jdbc-query-timeout-seconds",
                    "FLINK_CONTEXT_JDBC_QUERY_TIMEOUT_SECONDS",
                    1L,
                    1L));
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
                    args, environment, "allowed-lateness-ms",
                    "FLINK_CONTEXT_ALLOWED_LATENESS_MS", 30_000L, 0L);
            long corrections = longValue(args, "correction-window-ms", 300_000L, 0L);
            long idle = longValue(
                    args, environment, "idle-source-timeout-ms",
                    "FLINK_CONTEXT_IDLE_SOURCE_TIMEOUT_MS", 60_000L, 1L);
            long ttl = longValue(args, "state-ttl-hours", 720L, 1L);
            long bootstrapWait = longValue(
                    args, "context-bootstrap-wait-ms", bounded ? 0L : 30_000L, 0L);
            long checkpoint = longValue(args, "checkpoint-interval-ms", 30_000L, 0L);
            int parallelism = Math.toIntExact(longValue(args, "parallelism", 1L, 1L));
            boolean simulation = booleanValue(
                    args, environment, "simulation-mode", "FLINK_SIMULATION_MODE", false);
            JobConfig config = new JobConfig(
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
                    parallelism,
                    simulation,
                    kafkaProperties(environment));
            config.validateTopicBoundary();
            config.validateContextSource();
            return config;
        }

        String safeSummary() {
            String contextDescription =
                    contextSource.equals("jdbc") ? "postgresql-point-in-time" : contextTopic;
            return String.format(
                    "{\"job\":\"context-enrichment\",\"hands\":\"%s\","
                            + "\"context_source\":\"%s\",\"contexts\":\"%s\","
                            + "\"output\":\"%s\","
                            + "\"allowed_lateness_ms\":%d,\"correction_window_ms\":%d,"
                            + "\"context_cache_ttl_hours\":%d,"
                            + "\"context_refresh_minutes\":%d,\"simulation\":%s}",
                    handTopic, contextSource, contextDescription, outputTopic, allowedLatenessMs,
                    correctionWindowMs, contextCacheTtlHours, contextRefreshMinutes,
                    simulationMode);
        }

        private void requireContextSource(String requiredContextSource) {
            if (requiredContextSource != null
                    && !requiredContextSource.equals(contextSource)) {
                throw new IllegalArgumentException(
                        "entrypoint requires context source " + requiredContextSource);
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
            JdbcUserContextRepository.validateTableName(contextJdbcTable);
        }

        private void validateTopicBoundary() {
            if (simulationMode) {
                requireExact("hand", handTopic, "poker.sim.hands.raw.v1");
                if (contextSource.equals("kafka")) {
                    requireExact("context", contextTopic, "poker.sim.user-context.v1");
                }
                requireExact(
                        "output",
                        outputTopic,
                        contextSource.equals("jdbc")
                                ? "poker.sim.hand-player-context.v2"
                                : "poker.sim.hand-player-context.v1");
                requireExact("dead-letter", deadLetterTopic,
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
                    ? new String[] {handTopic, contextTopic, outputTopic, deadLetterTopic}
                    : new String[] {handTopic, outputTopic, deadLetterTopic};
            for (String topic : topics) {
                if (topic.startsWith("poker.sim.")) {
                    throw new IllegalArgumentException(
                            "production mode rejects simulation topic " + topic);
                }
            }
        }

        private static void requireExact(String role, String actual, String expected) {
            if (!expected.equals(actual)) {
                throw new IllegalArgumentException(
                        "simulation topic boundary: " + role + " must be " + expected);
            }
        }

        private static void rejectSecretArgument(Map<String, String> args, String key) {
            if (args.containsKey(key)) {
                throw new IllegalArgumentException(
                        "--" + key + " is forbidden; mount TaskManager runtime credentials");
            }
        }

        private static Map<String, String> parseArguments(String[] arguments) {
            Map<String, String> parsed = new HashMap<>();
            for (int index = 0; index < arguments.length; index++) {
                String argument = arguments[index];
                if (!argument.startsWith("--")) {
                    throw new IllegalArgumentException("unexpected argument: " + argument);
                }
                String key = argument.substring(2);
                if (key.equals("from-beginning") || key.equals("bounded")
                        || key.equals("simulation-mode")) {
                    parsed.put(key, "true");
                    continue;
                }
                if (index + 1 >= arguments.length || arguments[index + 1].startsWith("--")) {
                    throw new IllegalArgumentException("missing value for --" + key);
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
            return args.getOrDefault(argument, environment.getOrDefault(environmentName, fallback));
        }

        private static long longValue(
                Map<String, String> args, String key, long fallback, long minimum) {
            long value = Long.parseLong(args.getOrDefault(key, Long.toString(fallback)));
            if (value < minimum) {
                throw new IllegalArgumentException("--" + key + " must be >= " + minimum);
            }
            return value;
        }

        private static long longValue(
                Map<String, String> args,
                Map<String, String> environment,
                String key,
                String environmentName,
                long fallback,
                long minimum) {
            long value = Long.parseLong(value(
                    args, environment, key, environmentName, Long.toString(fallback)));
            if (value < minimum) {
                throw new IllegalArgumentException("--" + key + " must be >= " + minimum);
            }
            return value;
        }

        private static boolean booleanValue(
                Map<String, String> args,
                Map<String, String> environment,
                String argument,
                String environmentName,
                boolean fallback) {
            String raw = value(
                    args, environment, argument, environmentName, Boolean.toString(fallback));
            if (!raw.equalsIgnoreCase("true") && !raw.equalsIgnoreCase("false")) {
                throw new IllegalArgumentException("--" + argument + " must be true or false");
            }
            return Boolean.parseBoolean(raw);
        }

        private static Properties kafkaProperties(Map<String, String> environment) {
            Properties properties = new Properties();
            String protocol = environment.getOrDefault("KAFKA_SECURITY_PROTOCOL", "PLAINTEXT");
            properties.setProperty("security.protocol", protocol);
            String mechanism = environment.get("KAFKA_SASL_MECHANISM");
            if (mechanism == null || mechanism.isBlank()) {
                return properties;
            }
            properties.setProperty("sasl.mechanism", mechanism);
            if (mechanism.equals("PLAIN")) {
                String username = environment.getOrDefault("KAFKA_SASL_USERNAME", "");
                String password = environment.getOrDefault("KAFKA_SASL_PASSWORD", "");
                properties.setProperty(
                        "sasl.jaas.config",
                        "org.apache.kafka.common.security.plain.PlainLoginModule required "
                                + "username=\"" + jaasEscape(username) + "\" "
                                + "password=\"" + jaasEscape(password) + "\";");
            }
            return properties;
        }

        private static String jaasEscape(String value) {
            return value.replace("\\", "\\\\").replace("\"", "\\\"");
        }
    }
}
