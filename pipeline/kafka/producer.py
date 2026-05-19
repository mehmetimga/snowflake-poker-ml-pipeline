from __future__ import annotations

import json
from typing import Iterable

from kafka import KafkaProducer

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs


class HandProducer:
    def __init__(self, bootstrap_servers: str | None = None, topic: str | None = None) -> None:
        s = get_settings()
        self._producer = KafkaProducer(
            bootstrap_servers=(bootstrap_servers or s.kafka_bootstrap_servers).split(","),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: k.encode("utf-8") if isinstance(k, str) else None,
            acks="all",
            **kafka_client_kwargs(),
        )
        self.topic = topic or s.kafka_hands_topic

    def publish(self, hand: dict) -> None:
        self._producer.send(self.topic, key=hand["hand_id"], value=hand)

    def publish_many(self, hands: Iterable[dict]) -> int:
        count = 0
        for h in hands:
            self.publish(h)
            count += 1
            if count % 500 == 0:
                self._producer.flush()
        self._producer.flush()
        return count

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
