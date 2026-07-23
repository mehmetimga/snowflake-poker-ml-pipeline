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

/** Native Flink event-time context enrichment job. */
public final class ContextEnrichmentJob {
    private ContextEnrichmentJob() {}

    public static void main(String[] arguments) throws Exception {
        JobConfig config = JobConfig.parse(arguments, System.getenv());
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
                .uid("canonical-hands-source-v1")
                .process(new EnvelopeValidationFunction(
                        config.handTopic(), EventJson.HAND_COMPLETED,
                        config.simulationMode()), Types.STRING)
                .name("validate-hand-envelopes")
                .uid("validate-hand-envelopes-v1");
        WatermarkStrategy<String> eventTime = WatermarkStrategy
                .<String>forMonotonousTimestamps()
                .withTimestampAssigner(
                        (value, previousTimestamp) -> EventJson.occurredAtMs(value))
                .withIdleness(Duration.ofMillis(config.idleSourceTimeoutMs()));

        DataStream<String> handPlayers = validHands
                .assignTimestampsAndWatermarks(eventTime)
                .name("hand-event-time")
                .uid("hand-event-time-v1")
                .flatMap(new HandPlayerExpander(), Types.STRING)
                .name("expand-hand-players")
                .uid("expand-hand-players-v1");
        SingleOutputStreamOperator<String> enriched;
        DataStream<String> deadLetters;
        if (config.contextSource().equals("jdbc")) {
            enriched = handPlayers
                    .keyBy(EventJson::playerIdFromExpandedHand, Types.STRING)
                    .process(
                            new JdbcContextEnrichmentFunction(
                                    config.contextJdbcUrl(),
                                    config.contextJdbcUsername(),
                                    config.contextJdbcPassword(),
                                    config.contextJdbcTable(),
                                    config.contextJdbcQueryTimeoutSeconds(),
                                    config.contextCacheTtlHours(),
                                    config.contextRefreshMinutes(),
                                    config.handTopic()),
                            Types.STRING)
                    .name("jdbc-active-user-context-lookup")
                    .uid("jdbc-active-user-context-lookup-v1");
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
                    .uid("user-context-source-v1")
                    .process(new EnvelopeValidationFunction(
                            config.contextTopic(), EventJson.USER_CONTEXT_UPDATED,
                            config.simulationMode()), Types.STRING)
                    .name("validate-context-envelopes")
                    .uid("validate-context-envelopes-v1");
            DataStream<String> contexts = validContexts
                    .assignTimestampsAndWatermarks(eventTime)
                    .name("context-event-time")
                    .uid("context-event-time-v1");
            enriched = handPlayers
                    .keyBy(EventJson::playerIdFromExpandedHand, Types.STRING)
                    .connect(contexts.keyBy(EventJson::contextUserId, Types.STRING))
                    .process(
                            new ContextTemporalJoinFunction(
                                    config.allowedLatenessMs(),
                                    config.correctionWindowMs(),
                                    config.stateTtlHours(),
                                    config.contextBootstrapWaitMs(),
                                    config.contextTopic()),
                            Types.STRING)
                    .name("event-time-user-context-join")
                    .uid("event-time-user-context-join-v1");
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
                .uid("enriched-player-hand-sink-v1");

        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(config.deadLetterTopic(), false))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        deadLetters.sinkTo(deadLetterSink)
                .name("context-enrichment-dead-letter-sink")
                .uid("context-enrichment-dead-letter-sink-v1");

        System.out.println(config.safeSummary());
        environment.execute("poker-event-time-context-enrichment-v1");
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
            String contextJdbcUsername,
            String contextJdbcPassword,
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
            String bootstrap = value(args, environment, "bootstrap-servers",
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            String hands = value(args, environment, "hand-topic",
                    "KAFKA_WORLD_HANDS_TOPIC", "poker.hands.raw.v1");
            String contexts = value(args, environment, "context-topic",
                    "KAFKA_USER_CONTEXT_TOPIC", "poker.user-context.v1");
            String output = value(args, environment, "output-topic",
                    "KAFKA_PLAYER_CONTEXT_TOPIC", "poker.hand-player-context.v1");
            String dlq = value(args, environment, "dead-letter-topic",
                    "KAFKA_DEAD_LETTER_TOPIC", "poker.pipeline.dead-letter.v1");
            String group = value(args, environment, "group-id",
                    "FLINK_CONTEXT_GROUP_ID", "flink-context-enrichment-v1");
            String contextSource = value(
                    args, environment, "context-source", "FLINK_CONTEXT_SOURCE", "kafka")
                    .toLowerCase();
            if (!contextSource.equals("kafka") && !contextSource.equals("jdbc")) {
                throw new IllegalArgumentException(
                        "--context-source must be kafka or jdbc");
            }
            String contextJdbcUrl = value(
                    args, environment, "context-jdbc-url", "USER_CONTEXT_JDBC_URL", "");
            String contextJdbcUsername = value(
                    args, environment, "context-jdbc-username", "USER_CONTEXT_DB_USER", "");
            String contextJdbcPassword = value(
                    args, environment, "context-jdbc-password", "USER_CONTEXT_DB_PASSWORD", "");
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
                    contextJdbcUsername,
                    contextJdbcPassword,
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
            return String.format(
                    "{\"job\":\"context-enrichment\",\"hands\":\"%s\","
                            + "\"context_source\":\"%s\",\"contexts\":\"%s\","
                            + "\"output\":\"%s\","
                            + "\"allowed_lateness_ms\":%d,\"correction_window_ms\":%d,"
                            + "\"context_cache_ttl_hours\":%d,"
                            + "\"context_refresh_minutes\":%d,\"simulation\":%s}",
                    handTopic, contextSource, contextTopic, outputTopic, allowedLatenessMs,
                    correctionWindowMs, contextCacheTtlHours, contextRefreshMinutes,
                    simulationMode);
        }

        private void validateContextSource() {
            if (!contextSource.equals("jdbc")) {
                return;
            }
            if (contextJdbcUrl.isBlank()) {
                throw new IllegalArgumentException(
                        "USER_CONTEXT_JDBC_URL is required for JDBC context source");
            }
            if (contextJdbcUsername.isBlank() || contextJdbcPassword.isBlank()) {
                throw new IllegalArgumentException(
                        "user-context JDBC username and password are required");
            }
            JdbcUserContextRepository.validateTableName(contextJdbcTable);
        }

        private void validateTopicBoundary() {
            if (simulationMode) {
                requireExact("hand", handTopic, "poker.sim.hands.raw.v1");
                if (contextSource.equals("kafka")) {
                    requireExact("context", contextTopic, "poker.sim.user-context.v1");
                }
                requireExact("output", outputTopic, "poker.sim.hand-player-context.v1");
                requireExact("dead-letter", deadLetterTopic,
                        "poker.sim.pipeline.dead-letter.v1");
                requireExact("group", groupId, "flink-context-enrichment-sim-v1");
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
