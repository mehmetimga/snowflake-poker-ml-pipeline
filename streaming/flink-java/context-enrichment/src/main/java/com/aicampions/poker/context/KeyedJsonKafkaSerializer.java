package com.aicampions.poker.context;

import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

final class KeyedJsonKafkaSerializer implements KafkaRecordSerializationSchema<String> {
    private final String topic;
    private final boolean enrichedEvent;

    KeyedJsonKafkaSerializer(String topic, boolean enrichedEvent) {
        this.topic = topic;
        this.enrichedEvent = enrichedEvent;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(
            String element, KafkaSinkContext context, Long timestamp) {
        String key;
        try {
            key = enrichedEvent
                    ? EventJson.playerIdFromEnriched(element)
                    : EventJson.parse(element).path("source_topic").asText("unknown");
        } catch (RuntimeException error) {
            key = "unknown";
        }
        return new ProducerRecord<>(topic, EventJson.utf8(key), EventJson.utf8(element));
    }
}
