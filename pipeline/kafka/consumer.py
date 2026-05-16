from __future__ import annotations

import json
from typing import Optional

from kafka import KafkaConsumer

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse
from pipeline.warehouse.loader import load_hands


class WarehouseSink:
    """Consume hand events from Kafka and batch-write to the warehouse."""

    def __init__(
        self,
        warehouse: Optional[Warehouse] = None,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        batch_size: int = 200,
        group_id: str = "warehouse-sink",
    ) -> None:
        s = get_settings()
        self.warehouse = warehouse or get_warehouse()
        self.topic = topic or s.kafka_hands_topic
        self.batch_size = batch_size
        self._consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=(bootstrap_servers or s.kafka_bootstrap_servers).split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            consumer_timeout_ms=10000,
        )

    def run(self, max_messages: Optional[int] = None) -> int:
        total = 0
        buffer: list[dict] = []
        for msg in self._consumer:
            buffer.append(msg.value)
            if len(buffer) >= self.batch_size:
                total += load_hands(self.warehouse, buffer)
                print(f"[consumer] flushed {len(buffer)} hands (total {total})")
                buffer.clear()
            if max_messages is not None and total + len(buffer) >= max_messages:
                break
        if buffer:
            total += load_hands(self.warehouse, buffer)
            print(f"[consumer] flushed {len(buffer)} hands (total {total})")
        return total

    def close(self) -> None:
        self._consumer.close()
