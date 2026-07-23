package com.aicampions.poker.context;

import com.aicampions.poker.context.config.ContextJobConfig;
import com.aicampions.poker.context.domain.ContextKey;
import com.aicampions.poker.context.flink.JdbcContextEnrichmentFunction;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
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
        run(arguments, "snowflake");
    }

    public static void runLegacy(String[] arguments) throws Exception {
        run(arguments, "kafka");
    }

    private static void run(
            String[] arguments, String requiredContextSource)
            throws Exception {
        ContextJobConfig config =
                ContextJobConfig.parse(arguments, System.getenv());
        config.requireContextSource(requiredContextSource);
        boolean active = !config.contextSource().equals("kafka");
        String uidPrefix =
                active
                        ? "active-context-v2"
                        : "legacy-kafka-context-v1";
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(config.parallelism());
        if (active) {
            environment.setRestartStrategy(
                    RestartStrategies.failureRateRestart(
                            config.restartMaxFailuresPerInterval(),
                            Time.milliseconds(
                                    config.restartFailureRateIntervalMs()),
                            Time.milliseconds(
                                    config.restartDelayMs())));
        }
        if (config.checkpointIntervalMs() > 0) {
            environment.enableCheckpointing(
                    config.checkpointIntervalMs());
        }

        KafkaSource<String> handSource = source(
                config,
                config.handTopic(),
                config.groupId() + "-hands");
        SingleOutputStreamOperator<String> validHands = environment
                .fromSource(
                        handSource,
                        WatermarkStrategy.noWatermarks(),
                        "canonical-hands-source")
                .uid(uidPrefix + "-hands-source")
                .process(
                        new EnvelopeValidationFunction(
                                config.handTopic(),
                                EventJson.HAND_COMPLETED,
                                config.simulationMode()),
                        Types.STRING)
                .name("validate-hand-envelopes")
                .uid(uidPrefix + "-validate-hands");
        WatermarkStrategy<String> eventTime = WatermarkStrategy
                .<String>forMonotonousTimestamps()
                .withTimestampAssigner(
                        (value, previousTimestamp) ->
                                EventJson.occurredAtMs(value))
                .withIdleness(Duration.ofMillis(
                        config.idleSourceTimeoutMs()));

        DataStream<String> handPlayers = validHands
                .assignTimestampsAndWatermarks(eventTime)
                .name("hand-event-time")
                .uid(uidPrefix + "-hand-event-time")
                .flatMap(new HandPlayerExpander(), Types.STRING)
                .name("expand-hand-players")
                .uid(uidPrefix + "-expand-hand-players");

        SingleOutputStreamOperator<String> enriched;
        DataStream<String> deadLetters;
        if (active) {
            enriched = handPlayers
                    .keyBy(
                            EventJson::contextKeyFromExpandedHand,
                            Types.POJO(ContextKey.class))
                    .process(
                            new JdbcContextEnrichmentFunction(
                                    config.contextSource(),
                                    config.contextJdbcUrl(),
                                    config.contextSnowflakeProxyUrl(),
                                    config.contextJdbcTable(),
                                    config.contextJdbcQueryTimeoutSeconds(),
                                    config.contextJdbcConnectTimeoutSeconds(),
                                    config.contextJdbcValidationTimeoutSeconds(),
                                    config.contextJdbcRetryMaximumJitterMs(),
                                    config.contextCacheTtlHours(),
                                    config.contextRefreshMinutes(),
                                    config.handTopic()),
                            Types.STRING)
                    .name(
                            config.contextSource()
                                    + "-active-user-context-lookup")
                    .uid("active-context-v2-jdbc-lookup");
            deadLetters = validHands
                    .getSideOutput(DeadLetters.TAG)
                    .union(enriched.getSideOutput(DeadLetters.TAG));
        } else {
            KafkaSource<String> contextSource = source(
                    config,
                    config.contextTopic(),
                    config.groupId() + "-contexts");
            SingleOutputStreamOperator<String> validContexts =
                    environment
                            .fromSource(
                                    contextSource,
                                    WatermarkStrategy.noWatermarks(),
                                    "user-context-source")
                            .uid(
                                    "legacy-kafka-context-v1-context-source")
                            .process(
                                    new EnvelopeValidationFunction(
                                            config.contextTopic(),
                                            EventJson.USER_CONTEXT_UPDATED,
                                            config.simulationMode()),
                                    Types.STRING)
                            .name("validate-context-envelopes")
                            .uid(
                                    "legacy-kafka-context-v1-validate-contexts");
            DataStream<String> contexts = validContexts
                    .assignTimestampsAndWatermarks(eventTime)
                    .name("context-event-time")
                    .uid(
                            "legacy-kafka-context-v1-context-event-time");
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
                    .uid(
                            "legacy-kafka-context-v1-temporal-join");
            deadLetters = validHands
                    .getSideOutput(DeadLetters.TAG)
                    .union(
                            validContexts.getSideOutput(
                                    DeadLetters.TAG),
                            enriched.getSideOutput(
                                    DeadLetters.TAG));
        }

        KafkaSink<String> enrichedSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(
                        config.outputTopic(), true))
                .setDeliveryGuarantee(
                        DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        enriched
                .sinkTo(enrichedSink)
                .name("enriched-player-hand-sink")
                .uid(uidPrefix + "-enriched-sink");

        KafkaSink<String> deadLetterSink = KafkaSink.<String>builder()
                .setBootstrapServers(config.bootstrapServers())
                .setKafkaProducerConfig(config.kafkaProperties())
                .setRecordSerializer(new KeyedJsonKafkaSerializer(
                        config.deadLetterTopic(), false))
                .setDeliveryGuarantee(
                        DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        deadLetters
                .sinkTo(deadLetterSink)
                .name("context-enrichment-dead-letter-sink")
                .uid(uidPrefix + "-dead-letter-sink");

        System.out.println(config.safeSummary());
        environment.execute(
                active
                        ? "poker-active-context-enrichment-v2"
                        : "poker-legacy-kafka-temporal-context-v1");
    }

    private static KafkaSource<String> source(
            ContextJobConfig config,
            String topic,
            String groupId) {
        KafkaSourceBuilder<String> builder =
                KafkaSource.<String>builder()
                        .setBootstrapServers(
                                config.bootstrapServers())
                        .setTopics(topic)
                        .setGroupId(groupId)
                        .setStartingOffsets(
                                config.fromBeginning()
                                        ? OffsetsInitializer.earliest()
                                        : OffsetsInitializer.latest())
                        .setProperties(config.kafkaProperties())
                        .setValueOnlyDeserializer(
                                new SimpleStringSchema());
        if (config.bounded()) {
            builder.setBounded(OffsetsInitializer.latest());
        }
        return builder.build();
    }
}
