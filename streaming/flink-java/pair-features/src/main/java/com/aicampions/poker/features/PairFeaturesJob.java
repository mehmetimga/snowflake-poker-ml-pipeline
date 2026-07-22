package com.aicampions.poker.features;

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

/** Native Flink pair expansion and rolling feature job. */
public final class PairFeaturesJob {
    private PairFeaturesJob() {}

    public static void main(String[] arguments) throws Exception {
        JobConfig config = JobConfig.parse(arguments, System.getenv());
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(config.parallelism());
        if (config.checkpointIntervalMs() > 0) {
            environment.enableCheckpointing(config.checkpointIntervalMs());
        }

        KafkaSource<String> source = source(config);
        SingleOutputStreamOperator<String> valid = environment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "enriched-player-hand-source")
                .uid("enriched-player-hand-source-v1")
                .process(new EnvelopeValidationFunction(
                        config.inputTopic(), config.simulationMode()), Types.STRING)
                .name("validate-enriched-player-hands")
                .uid("validate-enriched-player-hands-v1");

        WatermarkStrategy<String> eventTime = WatermarkStrategy
                .<String>forBoundedOutOfOrderness(
                        Duration.ofMillis(config.outOfOrdernessMs()))
                .withTimestampAssigner(
                        (value, previousTimestamp) -> PairEventJson.occurredAtMs(value))
                .withIdleness(Duration.ofMillis(config.idleSourceTimeoutMs()));

        SingleOutputStreamOperator<String> userHistory = valid
                .assignTimestampsAndWatermarks(eventTime)
                .name("pair-feature-event-time")
                .uid("pair-feature-event-time-v1")
                .keyBy(PairEventJson::playerId, Types.STRING)
                .process(
                        new UserRollingFunction(config.stateTtlHours(), config.inputTopic()),
                        Types.STRING)
                .name("rolling-user-history")
                .uid("rolling-user-history-v1");

        SingleOutputStreamOperator<String> pairObservations = userHistory
                .keyBy(PairEventJson::handId, Types.STRING)
                .process(
                        new HandPairAssemblyFunction(config.stateTtlHours(), config.inputTopic()),
                        Types.STRING)
                .name("assemble-hand-and-expand-pairs")
                .uid("assemble-hand-and-expand-pairs-v1");

        SingleOutputStreamOperator<String> pairFeatures = pairObservations
                .keyBy(PairEventJson::pairKey, Types.STRING)
                .process(
                        new PairRollingFunction(config.stateTtlHours(), config.inputTopic()),
                        Types.STRING)
                .name("rolling-pair-features")
                .uid("rolling-pair-features-v1");

        StatefulFoldRuleEngine.Config statefulRuleConfig = new StatefulFoldRuleEngine.Config(
                Math.multiplyExact(config.statefulRuleWindowHours(), 3_600_000L),
                config.statefulRuleMinimumHands(),
                config.statefulRuleMinimumDirectionalCount(),
                config.statefulRuleRateThreshold(),
                config.statefulRuleAllowedLatenessMs(),
                Math.multiplyExact(config.statefulRuleCorrectionHorizonHours(), 3_600_000L));
        SingleOutputStreamOperator<String> scoringPairs = pairFeatures
                .keyBy(PairEventJson::scopedPairKey, Types.STRING)
                .process(
                        new StatefulPairRuleFunction(
                                config.statefulRuleStateTtlHours(),
                                config.outputTopic(),
                                statefulRuleConfig,
                                config.statefulRuleEnabled()),
                        Types.STRING)
                .name("stateful-pair-rules")
                .uid("stateful-pair-rules-v1");

        KafkaSink<String> outputSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(config.outputTopic(), true))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        scoringPairs.sinkTo(outputSink)
                .name("pair-feature-sink")
                .uid("pair-feature-sink-v1");

        DataStream<String> deadLetters = valid.getSideOutput(DeadLetters.TAG)
                .union(
                        userHistory.getSideOutput(DeadLetters.TAG),
                        pairObservations.getSideOutput(DeadLetters.TAG),
                        pairFeatures.getSideOutput(DeadLetters.TAG),
                        scoringPairs.getSideOutput(DeadLetters.TAG));
        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(config.deadLetterTopic(), false))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        deadLetters.sinkTo(deadLetterSink)
                .name("pair-feature-dead-letter-sink")
                .uid("pair-feature-dead-letter-sink-v1");

        System.out.println(config.safeSummary());
        environment.execute("poker-pair-features-v1");
    }

    private static KafkaSource<String> source(JobConfig config) {
        KafkaSourceBuilder<String> builder = KafkaSource.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setTopics(config.inputTopic())
                .setGroupId(config.groupId())
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
            String inputTopic,
            String outputTopic,
            String deadLetterTopic,
            String groupId,
            boolean fromBeginning,
            boolean bounded,
            long outOfOrdernessMs,
            long idleSourceTimeoutMs,
            long stateTtlHours,
            long statefulRuleWindowHours,
            int statefulRuleMinimumHands,
            int statefulRuleMinimumDirectionalCount,
            double statefulRuleRateThreshold,
            long statefulRuleAllowedLatenessMs,
            long statefulRuleCorrectionHorizonHours,
            long statefulRuleStateTtlHours,
            boolean statefulRuleEnabled,
            long checkpointIntervalMs,
            int parallelism,
            boolean simulationMode,
            Properties kafkaProperties) {

        static JobConfig parse(String[] arguments, Map<String, String> environment) {
            Map<String, String> args = parseArguments(arguments);
            String bootstrap = value(args, environment, "bootstrap-servers",
                    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
            String input = value(args, environment, "input-topic",
                    "KAFKA_PLAYER_CONTEXT_TOPIC", "poker.hand-player-context.v1");
            String output = value(args, environment, "output-topic",
                    "KAFKA_PAIR_FEATURES_TOPIC", "poker.pair-features.v1");
            String dlq = value(args, environment, "dead-letter-topic",
                    "KAFKA_DEAD_LETTER_TOPIC", "poker.pipeline.dead-letter.v1");
            String group = value(args, environment, "group-id",
                    "FLINK_PAIR_FEATURES_GROUP_ID", "flink-pair-features-v1");
            boolean fromBeginning = args.containsKey("from-beginning");
            boolean bounded = args.containsKey("bounded");
            long outOfOrderness = longValue(args, "out-of-orderness-ms", 30_000L, 0L);
            long idle = longValue(
                    args, environment, "idle-source-timeout-ms",
                    "FLINK_PAIR_IDLE_SOURCE_TIMEOUT_MS", 60_000L, 1L);
            long ttl = longValue(args, "state-ttl-hours", 720L, 1L);
            long ruleWindow = longValue(args, "stateful-rule-window-hours", 24L, 1L);
            int ruleMinimumHands = Math.toIntExact(
                    longValue(args, "stateful-rule-minimum-hands", 5L, 1L));
            int ruleMinimumDirectional = Math.toIntExact(
                    longValue(args, "stateful-rule-minimum-directional-count", 3L, 1L));
            double ruleRate = doubleValue(
                    args, "stateful-rule-rate-threshold", 0.6, 0.0, 1.0);
            long ruleLateness = longValue(
                    args, "stateful-rule-allowed-lateness-ms", 120_000L, 0L);
            long correctionHorizon = longValue(
                    args, "stateful-rule-correction-horizon-hours", 48L, ruleWindow);
            long ruleTtl = longValue(
                    args, "stateful-rule-state-ttl-hours", 72L, correctionHorizon);
            boolean ruleEnabled = booleanValue(
                    args, environment, "stateful-rule-enabled", "FLINK_STATEFUL_RULE_ENABLED", true);
            long checkpoint = longValue(args, "checkpoint-interval-ms", 30_000L, 0L);
            int parallelism = Math.toIntExact(longValue(args, "parallelism", 1L, 1L));
            boolean simulation = booleanValue(
                    args, environment, "simulation-mode", "FLINK_SIMULATION_MODE", false);
            JobConfig config = new JobConfig(
                    bootstrap, input, output, dlq, group, fromBeginning, bounded,
                    outOfOrderness, idle, ttl,
                    ruleWindow, ruleMinimumHands, ruleMinimumDirectional, ruleRate,
                    ruleLateness, correctionHorizon, ruleTtl,
                    ruleEnabled,
                    checkpoint, parallelism,
                    simulation,
                    kafkaProperties(environment));
            config.validateTopicBoundary();
            return config;
        }

        String safeSummary() {
            return String.format(
                    "{\"job\":\"pair-features\",\"input\":\"%s\","
                            + "\"output\":\"%s\",\"feature_version\":\"%s\","
                            + "\"stateful_rule\":\"%s\",\"stateful_rule_enabled\":%s,"
                            + "\"simulation\":%s}",
                    inputTopic, outputTopic, PairEventJson.FEATURE_VERSION,
                    StatefulFoldRuleEngine.RULE_ID, statefulRuleEnabled, simulationMode);
        }

        private void validateTopicBoundary() {
            if (simulationMode) {
                requireExact("input", inputTopic, "poker.sim.hand-player-context.v1");
                requireExact("output", outputTopic, "poker.sim.pair-features.v1");
                requireExact("dead-letter", deadLetterTopic,
                        "poker.sim.pipeline.dead-letter.v1");
                requireExact("group", groupId, "flink-pair-features-sim-v1");
                return;
            }
            for (String topic : new String[] {inputTopic, outputTopic, deadLetterTopic}) {
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

        private static double doubleValue(
                Map<String, String> args,
                String key,
                double fallback,
                double minimum,
                double maximum) {
            double value = Double.parseDouble(
                    args.getOrDefault(key, Double.toString(fallback)));
            if (!Double.isFinite(value) || value < minimum || value > maximum) {
                throw new IllegalArgumentException(
                        "--" + key + " must be in [" + minimum + ", " + maximum + "]");
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
