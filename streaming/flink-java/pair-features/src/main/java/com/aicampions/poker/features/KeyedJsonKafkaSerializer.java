package com.aicampions.poker.features;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KeyedJsonKafkaSerializer implements KafkaRecordSerializationSchema<String> {
    private final String topic;
    private final boolean pairKey;

    KeyedJsonKafkaSerializer(String topic, boolean pairKey) {
        this.topic = topic;
        this.pairKey = pairKey;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            String value, KafkaSinkContext context, Long timestamp) {
        JsonNode root = PairEventJson.parse(value);
        String key;
        if (pairKey) {
            key = PairEventJson.requireText(root.path("payload"), "pair_key");
        } else {
            key = root.path("event_id").asText(root.path("stage").asText("dead-letter"));
        }
        return new ProducerRecord<>(topic, PairEventJson.utf8(key), PairEventJson.utf8(value));
    }
}
