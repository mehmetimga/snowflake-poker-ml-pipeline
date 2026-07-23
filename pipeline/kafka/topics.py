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
    rule_evidence: str = "poker.rule-evidence.v1"
    review_decisions: str = "poker.review-decisions.v1"
    risk_alerts: str = "poker.risk-alerts.v1"
    dead_letters: str = "poker.pipeline.dead-letter.v1"

    @classmethod
    def from_settings(cls) -> "ScoringTopics":
        settings = get_settings()
        return cls(
            risk_scores=settings.kafka_risk_scores_topic,
            rule_evidence=settings.kafka_rule_evidence_topic,
            review_decisions=settings.kafka_review_decisions_topic,
            risk_alerts=settings.kafka_risk_alerts_topic,
            dead_letters=settings.kafka_dead_letter_topic,
        )


@dataclass(frozen=True)
class CdcSimulationTopics:
    """Strictly isolated topics used by the synthetic C2 adapter."""

    source: str = "poker.sim.cdc-hand-outbox.v1"
    canonical: str = "poker.sim.hands.raw.v1"
    dead_letters: str = "poker.sim.pipeline.dead-letter.v1"


@dataclass(frozen=True)
class ShadowSimulationTopics:
    """Complete isolated topic boundary for the synthetic shadow pipeline."""

    user_context: str = "poker.sim.user-context.v1"
    player_context: str = "poker.sim.hand-player-context.v1"
    pair_features: str = "poker.sim.pair-features.v1"
    risk_scores: str = "poker.sim.risk-scores.v1"
    rule_evidence: str = "poker.sim.rule-evidence.v1"
    review_decisions: str = "poker.sim.review-decisions.v1"
    risk_alerts: str = "poker.sim.risk-alerts.v1"
    dead_letters: str = "poker.sim.pipeline.dead-letter.v1"


@dataclass(frozen=True)
class CanonicalFlinkTopics:
    """Hands-only synthetic boundary for the canonical SPCS Flink service."""

    hands: str = "poker.synthetic.hands.raw.v1"
    player_context: str = "poker.synthetic.hand-player-context.v2"
    pair_features: str = "poker.synthetic.pair-features.context-v2.v1"
    dead_letters: str = "poker.synthetic.pipeline.dead-letter.v1"


@dataclass(frozen=True)
class CanonicalScoringTopics:
    """Synthetic outputs produced by the canonical SPCS risk service."""

    risk_scores: str = "poker.synthetic.risk-scores.v1"
    rule_evidence: str = "poker.synthetic.rule-evidence.v1"
    review_decisions: str = "poker.synthetic.review-decisions.v1"
    risk_alerts: str = "poker.synthetic.risk-alerts.v1"
    dead_letters: str = "poker.synthetic.pipeline.dead-letter.v1"


def canonical_spcs_topics() -> dict[str, str]:
    """Return the complete canonical hands -> Flink -> risk topic boundary."""
    flink = CanonicalFlinkTopics()
    scoring = CanonicalScoringTopics()
    return {
        "hands": flink.hands,
        "player_context": flink.player_context,
        "pair_features": flink.pair_features,
        "risk_scores": scoring.risk_scores,
        "rule_evidence": scoring.rule_evidence,
        "review_decisions": scoring.review_decisions,
        "risk_alerts": scoring.risk_alerts,
        "dead_letters": flink.dead_letters,
    }


def canonical_flink_topic_specs(
    topics: CanonicalFlinkTopics | None = None,
    *,
    partitions: int = 3,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Define exactly the canonical Flink input, outputs, and DLQ."""
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or CanonicalFlinkTopics()
    topic_names = (
        names.hands,
        names.player_context,
        names.pair_features,
        names.dead_letters,
    )
    if len(set(topic_names)) != len(topic_names) or any(
        not name.startswith("poker.synthetic.") for name in topic_names
    ):
        raise ValueError(
            "canonical Flink topics must be unique poker.synthetic.* names"
        )
    seven_days_ms = str(7 * 24 * 60 * 60 * 1_000)
    thirty_days_ms = str(30 * 24 * 60 * 60 * 1_000)
    return tuple(
        WorldTopicSpec(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={
                "cleanup.policy": "delete",
                "retention.ms": (
                    thirty_days_ms if name == names.dead_letters else seven_days_ms
                ),
            },
        )
        for name in topic_names
    )


def ensure_canonical_flink_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: CanonicalFlinkTopics | None = None,
    partitions: int = 3,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create only missing canonical Flink topics; never alter existing topics."""
    specs = canonical_flink_topic_specs(
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
            client_id="poker-canonical-flink-topic-manager-v1",
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


def canonical_scoring_topic_specs(
    topics: CanonicalScoringTopics | None = None,
    *,
    partitions: int = 3,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Define the canonical synthetic risk outputs and shared DLQ."""
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or CanonicalScoringTopics()
    topic_names = (
        names.risk_scores,
        names.rule_evidence,
        names.review_decisions,
        names.risk_alerts,
        names.dead_letters,
    )
    if len(set(topic_names)) != len(topic_names) or any(
        not name.startswith("poker.synthetic.") for name in topic_names
    ):
        raise ValueError(
            "canonical scoring topics must be unique poker.synthetic.* names"
        )
    thirty_days_ms = str(30 * 24 * 60 * 60 * 1_000)
    return tuple(
        WorldTopicSpec(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={
                "cleanup.policy": "delete",
                "retention.ms": thirty_days_ms,
            },
        )
        for name in topic_names
    )


def ensure_canonical_scoring_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: CanonicalScoringTopics | None = None,
    partitions: int = 3,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create only missing canonical scoring topics; never alter existing ones."""
    specs = canonical_scoring_topic_specs(
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
            client_id="poker-canonical-scoring-topic-manager-v1",
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


def shadow_simulation_topic_specs(
    topics: ShadowSimulationTopics | None = None,
    *,
    partitions: int = 3,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Return only the derived topics used by isolated Flink/risk services."""
    if partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or ShadowSimulationTopics()
    topic_names = (
        names.user_context,
        names.player_context,
        names.pair_features,
        names.risk_scores,
        names.rule_evidence,
        names.review_decisions,
        names.risk_alerts,
        names.dead_letters,
    )
    if len(set(topic_names)) != len(topic_names) or any(
        not name.startswith("poker.sim.") for name in topic_names
    ):
        raise ValueError("shadow simulation topics must be unique poker.sim.* names")
    seven_days_ms = str(7 * 24 * 60 * 60 * 1_000)
    compact = {"cleanup.policy": "compact", "retention.ms": seven_days_ms}
    delete = {"cleanup.policy": "delete", "retention.ms": seven_days_ms}
    return tuple(
        WorldTopicSpec(
            name=name,
            partitions=partitions,
            replication_factor=replication_factor,
            configs=dict(compact if name == names.user_context else delete),
        )
        for name in topic_names
    )


def ensure_shadow_simulation_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: ShadowSimulationTopics | None = None,
    partitions: int = 3,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create only missing full-shadow topics; never alter existing topics."""
    specs = shadow_simulation_topic_specs(
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
            client_id="poker-shadow-simulation-topic-manager-v1",
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


def cdc_simulation_topic_specs(
    topics: CdcSimulationTopics | None = None,
    *,
    output_partitions: int = 3,
    replication_factor: int = 3,
) -> tuple[WorldTopicSpec, ...]:
    """Return the fixed C2 simulation boundary and retention policy.

    The source stays single-partition so deterministic fault and recovery
    replays preserve their input order. Derived outputs can fan out.
    """
    if output_partitions < 1 or replication_factor < 1:
        raise ValueError("partitions and replication_factor must be positive")
    names = topics or CdcSimulationTopics()
    topic_names = (names.source, names.canonical, names.dead_letters)
    if len(set(topic_names)) != len(topic_names) or any(
        not name.startswith("poker.sim.") for name in topic_names
    ):
        raise ValueError("CDC simulation topics must be unique poker.sim.* names")
    seven_days_ms = str(7 * 24 * 60 * 60 * 1_000)
    configs = {"cleanup.policy": "delete", "retention.ms": seven_days_ms}
    return (
        WorldTopicSpec(
            name=names.source,
            partitions=1,
            replication_factor=replication_factor,
            configs=dict(configs),
        ),
        WorldTopicSpec(
            name=names.canonical,
            partitions=output_partitions,
            replication_factor=replication_factor,
            configs=dict(configs),
        ),
        WorldTopicSpec(
            name=names.dead_letters,
            partitions=output_partitions,
            replication_factor=replication_factor,
            configs=dict(configs),
        ),
    )


def ensure_cdc_simulation_topics(
    *,
    bootstrap_servers: str | None = None,
    topics: CdcSimulationTopics | None = None,
    output_partitions: int = 3,
    replication_factor: int = 3,
    admin_client: object | None = None,
) -> dict[str, list[str]]:
    """Create only missing isolated C2 topics; never alter existing topics."""
    specs = cdc_simulation_topic_specs(
        topics,
        output_partitions=output_partitions,
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
            client_id="poker-cdc-simulation-topic-manager-v1",
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
            name=names.rule_evidence,
            partitions=partitions,
            replication_factor=replication_factor,
            configs={"cleanup.policy": "delete", "retention.ms": thirty_days_ms},
        ),
        WorldTopicSpec(
            name=names.review_decisions,
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
