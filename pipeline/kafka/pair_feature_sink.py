"""At-least-once Kafka to warehouse sink for pair-feature snapshots."""

from __future__ import annotations

import json
from typing import Optional

from pipeline.config import get_settings
from pipeline.events import PairFeatureEvent
from pipeline.warehouse.factory import Warehouse, get_warehouse
from pipeline.warehouse.pair_features import (
    PairFeatureIngestRecord,
    PairFeatureLoadResult,
    load_pair_feature_events,
)

from .config import kafka_client_kwargs


class PairFeatureWarehouseSink:
    def __init__(
        self,
        *,
        warehouse: Warehouse | None = None,
        bootstrap_servers: str | None = None,
        topic: str | None = None,
        batch_size: int = 500,
        group_id: str = "poker-pair-feature-warehouse-sink-v1",
        auto_offset_reset: str = "latest",
        consumer_timeout_ms: int = -1,
        manual_assign_from_beginning: bool = False,
        consumer: object | None = None,
    ) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        settings = get_settings()
        self.warehouse = warehouse or get_warehouse()
        self.topic = topic or settings.kafka_pair_features_topic
        self.batch_size = batch_size
        self._commit_offsets = not manual_assign_from_beginning
        if consumer is None:
            from kafka import KafkaConsumer

            consumer = KafkaConsumer(
                *(() if manual_assign_from_beginning else (self.topic,)),
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

                partitions = consumer.partitions_for_topic(self.topic)
                if not partitions:
                    raise RuntimeError(f"Kafka topic has no readable partitions: {self.topic}")
                assignments = [
                    TopicPartition(self.topic, partition) for partition in partitions
                ]
                consumer.assign(assignments)
                consumer.seek_to_beginning(*assignments)
        self._consumer = consumer

    def _record(self, message: object) -> PairFeatureIngestRecord:
        event = PairFeatureEvent.model_validate(message.value)
        if message.topic != self.topic:
            raise ValueError(f"pair feature arrived on {message.topic}, expected {self.topic}")
        if message.key != event.payload.pair_key:
            raise ValueError(
                f"event {event.event_id} key={message.key!r}, "
                f"expected {event.payload.pair_key!r}"
            )
        return PairFeatureIngestRecord(
            envelope=event.model_dump(mode="json"),
            topic=message.topic,
            partition=int(message.partition),
            offset=int(message.offset),
            kafka_timestamp_ms=(
                int(message.timestamp) if message.timestamp is not None else None
            ),
        )

    def _flush(self, records: list[PairFeatureIngestRecord]) -> PairFeatureLoadResult:
        result = load_pair_feature_events(self.warehouse, records)
        if self._commit_offsets:
            self._consumer.commit()
        records.clear()
        return result

    def run(self, max_messages: Optional[int] = None) -> PairFeatureLoadResult:
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive")
        total = PairFeatureLoadResult()
        records: list[PairFeatureIngestRecord] = []
        seen_hands: set[str] = set()
        seen_pairs: set[str] = set()
        consumed = 0
        for message in self._consumer:
            record = self._record(message)
            event = PairFeatureEvent.model_validate(record.envelope)
            seen_hands.add(event.payload.hand_id)
            seen_pairs.add(event.payload.pair_key)
            records.append(record)
            consumed += 1
            if len(records) >= self.batch_size:
                total += self._flush(records)
            if max_messages is not None and consumed >= max_messages:
                break
        if records:
            total += self._flush(records)
        return PairFeatureLoadResult(
            events=total.events,
            hands=len(seen_hands),
            pairs=len(seen_pairs),
        )

    def close(self) -> None:
        self._consumer.close()
