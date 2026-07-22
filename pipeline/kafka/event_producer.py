"""Kafka publisher for canonical multi-stream world events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Mapping

from pipeline.config import get_settings
from pipeline.events import validate_event
from pipeline.events.contracts import TOPIC_BY_EVENT_TYPE
from pipeline.replay import PendingPublish, PublishAck, ReplayEvent

from .config import kafka_client_kwargs
from .headers import canonical_event_headers


@dataclass(frozen=True)
class WorldTopics:
    hands: str = "poker.hands.raw.v1"
    user_context: str = "poker.user-context.v1"
    sessions: str = "poker.session-context.v1"
    account_links: str = "poker.account-links.v1"

    def by_event_type(self) -> dict[str, str]:
        return {
            "poker.hand.completed": self.hands,
            "poker.user-context.updated": self.user_context,
            "poker.session.started": self.sessions,
            "poker.account-link.updated": self.account_links,
        }

    @classmethod
    def from_settings(cls) -> "WorldTopics":
        settings = get_settings()
        return cls(
            hands=settings.kafka_world_hands_topic,
            user_context=settings.kafka_user_context_topic,
            sessions=settings.kafka_session_context_topic,
            account_links=settings.kafka_account_links_topic,
        )


def _canonical_json(value: dict) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


class WorldEventProducer:
    """Route validated envelopes and wait for broker acknowledgements."""

    def __init__(
        self,
        *,
        bootstrap_servers: str | None = None,
        topics: WorldTopics | None = None,
        producer: object | None = None,
    ) -> None:
        self._topics = (topics or WorldTopics.from_settings()).by_event_type()
        unknown = set(self._topics) - set(TOPIC_BY_EVENT_TYPE)
        if unknown:
            raise ValueError(f"unsupported topic routes: {sorted(unknown)}")
        if producer is None:
            from kafka import KafkaProducer

            settings = get_settings()
            producer = KafkaProducer(
                bootstrap_servers=(
                    bootstrap_servers or settings.kafka_bootstrap_servers
                ).split(","),
                value_serializer=_canonical_json,
                key_serializer=lambda key: key.encode("utf-8"),
                acks="all",
                retries=10,
                max_in_flight_requests_per_connection=1,
                linger_ms=5,
                compression_type="gzip",
                client_id="poker-world-replayer-v1",
                **kafka_client_kwargs(),
            )
        self._producer = producer

    def publish(self, event: ReplayEvent) -> PendingPublish:
        envelope = validate_event(event.envelope)
        topic = self._topics[envelope.event_type]
        key = event.partition_key
        headers = canonical_event_headers(envelope)
        handle = self._producer.send(
            topic,
            key=key,
            value=envelope.model_dump(mode="json"),
            headers=headers,
        )
        return PendingPublish(
            event_id=str(envelope.event_id),
            topic=topic,
            key=key,
            handle=handle,
        )

    def acknowledge(self, pending: PendingPublish, timeout_seconds: float) -> PublishAck:
        if pending.handle is None:
            raise RuntimeError("Kafka publish did not return a future")
        metadata = pending.handle.get(timeout=timeout_seconds)
        return PublishAck(
            event_id=pending.event_id,
            topic=str(metadata.topic),
            key=pending.key,
            partition=int(metadata.partition),
            offset=int(metadata.offset),
        )

    def flush(self) -> None:
        self._producer.flush()

    def close(self) -> None:
        self._producer.flush()
        self._producer.close()
