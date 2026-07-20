"""At-least-once Kafka consumer with idempotent warehouse writes."""

from __future__ import annotations

import json
from typing import Optional

from pipeline.config import get_settings
from pipeline.events import event_partition_key, validate_event
from pipeline.warehouse.events import CanonicalLoadResult, IngestRecord, load_canonical_events
from pipeline.warehouse.factory import Warehouse, get_warehouse

from .config import kafka_client_kwargs
from .event_producer import WorldTopics


class WorldWarehouseSink:
    """Consume canonical topics and commit offsets only after warehouse success."""

    def __init__(
        self,
        *,
        warehouse: Warehouse | None = None,
        bootstrap_servers: str | None = None,
        topics: WorldTopics | None = None,
        batch_size: int = 200,
        group_id: str = "poker-world-warehouse-sink-v1",
        auto_offset_reset: str = "latest",
        consumer_timeout_ms: int = -1,
        manual_assign_from_beginning: bool = False,
        consumer: object | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        self.warehouse = warehouse or get_warehouse()
        self.batch_size = batch_size
        self._commit_offsets = not manual_assign_from_beginning
        self.topics = topics or WorldTopics.from_settings()
        self._topic_by_event_type = self.topics.by_event_type()
        if consumer is None:
            from kafka import KafkaConsumer

            settings = get_settings()
            subscribed_topics = sorted(set(self._topic_by_event_type.values()))
            consumer = KafkaConsumer(
                *(() if manual_assign_from_beginning else subscribed_topics),
                bootstrap_servers=(
                    bootstrap_servers or settings.kafka_bootstrap_servers
                ).split(","),
                key_deserializer=lambda value: value.decode("utf-8") if value else None,
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
                group_id=None if manual_assign_from_beginning else group_id,
                auto_offset_reset=auto_offset_reset,
                enable_auto_commit=False,
                consumer_timeout_ms=consumer_timeout_ms,
                **kafka_client_kwargs(),
            )
            if manual_assign_from_beginning:
                from kafka import TopicPartition

                assignments = []
                for topic in subscribed_topics:
                    partitions = consumer.partitions_for_topic(topic)
                    if not partitions:
                        raise RuntimeError(f"Kafka topic has no readable partitions: {topic}")
                    assignments.extend(
                        TopicPartition(topic, partition) for partition in partitions
                    )
                consumer.assign(assignments)
                consumer.seek_to_beginning(*assignments)
        self._consumer = consumer

    def _record(self, message: object) -> IngestRecord:
        envelope = validate_event(message.value)
        expected_topic = self._topic_by_event_type[envelope.event_type]
        if message.topic != expected_topic:
            raise ValueError(
                f"event {envelope.event_id} arrived on {message.topic}, "
                f"expected {expected_topic}"
            )
        expected_key = event_partition_key(envelope)
        if message.key != expected_key:
            raise ValueError(
                f"event {envelope.event_id} key={message.key!r}, expected {expected_key!r}"
            )
        return IngestRecord(
            envelope=envelope.model_dump(mode="json"),
            topic=message.topic,
            partition=int(message.partition),
            offset=int(message.offset),
            kafka_timestamp_ms=int(message.timestamp) if message.timestamp is not None else None,
        )

    def _flush(self, records: list[IngestRecord]) -> CanonicalLoadResult:
        result = load_canonical_events(self.warehouse, records)
        # Offset commits deliberately follow the warehouse transaction. A crash
        # before this call replays the batch, and event IDs make that harmless.
        if self._commit_offsets:
            self._consumer.commit()
        records.clear()
        return result

    def run(self, max_messages: Optional[int] = None) -> CanonicalLoadResult:
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive")
        total = CanonicalLoadResult()
        records: list[IngestRecord] = []
        consumed = 0
        for message in self._consumer:
            records.append(self._record(message))
            consumed += 1
            if len(records) >= self.batch_size:
                total += self._flush(records)
            if max_messages is not None and consumed >= max_messages:
                break
        if records:
            total += self._flush(records)
        return total

    def close(self) -> None:
        self._consumer.close()
