"""Managed definitions for canonical world-input Kafka topics."""

from __future__ import annotations

import time
from dataclasses import dataclass

from pipeline.config import get_settings

from .config import kafka_client_kwargs
from .event_producer import WorldTopics


@dataclass(frozen=True)
class WorldTopicSpec:
    name: str
    partitions: int
    replication_factor: int
    configs: dict[str, str]


@dataclass(frozen=True)
class EnrichmentTopics:
    player_context: str = "poker.hand-player-context.v1"
    pair_features: str = "poker.pair-features.v1"
    dead_letters: str = "poker.pipeline.dead-letter.v1"

    @classmethod
    def from_settings(cls) -> "EnrichmentTopics":
        settings = get_settings()
        return cls(
            player_context=settings.kafka_player_context_topic,
            pair_features=settings.kafka_pair_features_topic,
            dead_letters=settings.kafka_dead_letter_topic,
        )


@dataclass(frozen=True)
class ScoringTopics:
    risk_scores: str = "poker.risk-scores.v1"
    risk_alerts: str = "poker.risk-alerts.v1"
    dead_letters: str = "poker.pipeline.dead-letter.v1"

    @classmethod
    def from_settings(cls) -> "ScoringTopics":
        settings = get_settings()
        return cls(
            risk_scores=settings.kafka_risk_scores_topic,
            risk_alerts=settings.kafka_risk_alerts_topic,
            dead_letters=settings.kafka_dead_letter_topic,
        )


def world_topic_specs(
    topics: WorldTopics | None = None,
    *,
    partitions: int = 6,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or WorldTopics.from_settings()
    seven_days_ms = str(7 * 24 * 60 * 60 * 1_000)
    return (
        WorldTopicSpec(
            name=names.hands,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": seven_days_ms},
        ),
        WorldTopicSpec(
            name=names.user_context,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "compact"},
        ),
        WorldTopicSpec(
            name=names.sessions,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": seven_days_ms},
        ),
        WorldTopicSpec(
            name=names.account_links,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "compact"},
        ),
    )


def enrichment_topic_specs(
    topics: EnrichmentTopics | None = None,
    *,
    partitions: int = 6,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Derived topics kept separate so resetting world inputs cannot delete outputs."""
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or EnrichmentTopics.from_settings()
    seven_days_ms = str(7 * 24 * 60 * 60 * 1_000)
    thirty_days_ms = str(30 * 24 * 60 * 60 * 1_000)
    return (
        WorldTopicSpec(
            name=names.player_context,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": seven_days_ms},
        ),
        WorldTopicSpec(
            name=names.pair_features,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": seven_days_ms},
        ),
        WorldTopicSpec(
            name=names.dead_letters,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": thirty_days_ms},
        ),
    )


def scoring_topic_specs(
    topics: ScoringTopics | None = None,
    *,
    partitions: int = 6,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Online-scoring outputs and the shared poison-message destination."""
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or ScoringTopics.from_settings()
    thirty_days_ms = str(30 * 24 * 60 * 60 * 1_000)
    return (
        WorldTopicSpec(
            name=names.risk_scores,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": thirty_days_ms},
        ),
        WorldTopicSpec(
            name=names.risk_alerts,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": thirty_days_ms},
        ),
        WorldTopicSpec(
            name=names.dead_letters,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": thirty_days_ms},
        ),
    )


def ensure_scoring_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: ScoringTopics | None = None,
    partitions: int = 6,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create missing scoring topics without changing existing configurations."""
    specs = scoring_topic_specs(
        topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    owns_client = admin_client is None
    if admin_client is None:
        from kafka.admin import KafkaAdminClient

        settings = get_settings()
        admin_client = KafkaAdminClient(
            bootstrap_servers=(
                bootstrap_servers or settings.kafka_bootstrap_servers
            ).split(","),
            client_id="poker-risk-topic-manager-v1",
            **kafka_client_kwargs(),
        )
    try:
        existing = set(admin_client.list_topics())
        missing = [spec for spec in specs if spec.name not in existing]
        if missing:
            from kafka.admin import NewTopic

            admin_client.create_topics(
                [
                    NewTopic(
                        name=spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor,
                        topic_configs=spec.configs,
                    )
                    for spec in missing
                ],
                validate_only=False,
            )
        return {
            "created": [spec.name for spec in missing],
            "existing": [spec.name for spec in specs if spec.name in existing],
        }
    finally:
        if owns_client:
            admin_client.close()


def ensure_enrichment_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: EnrichmentTopics | None = None,
    partitions: int = 6,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create only the derived context, pair-feature, and dead-letter topics."""
    specs = enrichment_topic_specs(
        topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    owns_client = admin_client is None
    if admin_client is None:
        from kafka.admin import KafkaAdminClient

        settings = get_settings()
        admin_client = KafkaAdminClient(
            bootstrap_servers=(
                bootstrap_servers or settings.kafka_bootstrap_servers
            ).split(","),
            client_id="poker-context-topic-manager-v1",
            **kafka_client_kwargs(),
        )
    try:
        existing = set(admin_client.list_topics())
        missing = [spec for spec in specs if spec.name not in existing]
        if missing:
            from kafka.admin import NewTopic

            admin_client.create_topics(
                [
                    NewTopic(
                        name=spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor,
                        topic_configs=spec.configs,
                    )
                    for spec in missing
                ],
                validate_only=False,
            )
        return {
            "created": [spec.name for spec in missing],
            "existing": [spec.name for spec in specs if spec.name in existing],
        }
    finally:
        if owns_client:
            admin_client.close()


def ensure_world_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: WorldTopics | None = None,
    partitions: int = 6,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create missing canonical topics without modifying existing topics."""
    specs = world_topic_specs(
        topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    owns_client = admin_client is None
    if admin_client is None:
        from kafka.admin import KafkaAdminClient

        settings = get_settings()
        admin_client = KafkaAdminClient(
            bootstrap_servers=(
                bootstrap_servers or settings.kafka_bootstrap_servers
            ).split(","),
            client_id="poker-world-topic-manager-v1",
            **kafka_client_kwargs(),
        )
    try:
        existing = set(admin_client.list_topics())
        missing = [spec for spec in specs if spec.name not in existing]
        if missing:
            from kafka.admin import NewTopic

            admin_client.create_topics(
                [
                    NewTopic(
                        name=spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor,
                        topic_configs=spec.configs,
                    )
                    for spec in missing
                ],
                validate_only=False,
            )
        return {
            "created": [spec.name for spec in missing],
            "existing": [spec.name for spec in specs if spec.name in existing],
        }
    finally:
        if owns_client:
            admin_client.close()


def reset_world_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: WorldTopics | None = None,
    partitions: int = 6,
    replication_factor: int = 3,
    wait_seconds: float = 30.0,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Delete and recreate exactly the four managed world topics."""
    if wait_seconds <= 0:
        raise ValueError("wait_seconds must be positive")
    specs = world_topic_specs(
        topics,
        partitions=partitions,
        replication_factor=replication_factor,
    )
    names = [spec.name for spec in specs]
    owns_client = admin_client is None
    if admin_client is None:
        from kafka.admin import KafkaAdminClient

        settings = get_settings()
        admin_client = KafkaAdminClient(
            bootstrap_servers=(
                bootstrap_servers or settings.kafka_bootstrap_servers
            ).split(","),
            client_id="poker-world-topic-reset-v1",
            **kafka_client_kwargs(),
        )
    try:
        existing = set(admin_client.list_topics())
        deleted = [name for name in names if name in existing]
        if deleted:
            admin_client.delete_topics(deleted)
            deadline = time.monotonic() + wait_seconds
            while set(deleted) & set(admin_client.list_topics()):
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"timed out deleting topics: {deleted}")
                time.sleep(0.25)
        created = ensure_world_topics(
            topics=topics,
            partitions=partitions,
            replication_factor=replication_factor,
            admin_client=admin_client,
        )["created"]
        return {"deleted": deleted, "created": created}
    finally:
        if owns_client:
            admin_client.close()
