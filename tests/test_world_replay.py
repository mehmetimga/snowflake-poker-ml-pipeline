from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from pipeline.generator import (
    FrozenDatasetConfig,
    RealtimeWorldConfig,
    build_realtime_world_dataset,
)
from pipeline.kafka.event_producer import WorldEventProducer, WorldTopics
from pipeline.kafka.topics import (
    EnrichmentTopics,
    enrichment_topic_specs,
    ensure_enrichment_topics,
    ensure_world_topics,
    reset_world_topics,
    world_topic_specs,
)
from pipeline.replay import (
    DryRunPublisher,
    EventPacer,
    ReplayConfig,
    iter_delivery_events,
    iter_world_events,
    replay_world,
)


def _dataset(tmp_path: Path) -> tuple[Path, dict]:
    dataset_dir = tmp_path / "world"
    manifest = build_realtime_world_dataset(
        dataset_dir,
        RealtimeWorldConfig(
            dataset_id="replay-test-v1",
            frozen=FrozenDatasetConfig(
                train_hands=3,
                validation_hands=2,
                test_hands=2,
                challenge_hands=1,
                n_players=12,
                n_tables=2,
                n_colluding_pairs=3,
                seed=727,
            ),
        ),
    )
    return dataset_dir, manifest


def test_world_stream_merges_by_event_time_and_matches_manifest(tmp_path: Path):
    dataset_dir, manifest = _dataset(tmp_path)
    events = list(iter_world_events(dataset_dir))

    assert [event.occurred_at for event in events] == sorted(
        event.occurred_at for event in events
    )
    assert len(events) == sum(manifest["expected_kafka_counts"].values())
    assert len({event.event_id for event in events}) == len(events)

    actual: dict[str, int] = {}
    for event in events:
        actual[event.canonical_topic] = actual.get(event.canonical_topic, 0) + 1
        if event.event_type == "poker.hand.completed":
            assert event.partition_key == event.envelope["payload"]["table_id"]
    assert actual == manifest["expected_kafka_counts"]


def test_replay_is_stable_and_chaos_schedule_is_seeded(tmp_path: Path):
    dataset_dir, _ = _dataset(tmp_path)
    first = [event.event_id for event in iter_world_events(dataset_dir)]
    second = [event.event_id for event in iter_world_events(dataset_dir)]
    assert first == second

    config = ReplayConfig(
        mode="chaos",
        duplicate_rate=1.0,
        late_rate=0.5,
        reorder_window=3,
        chaos_seed=812,
    )
    first_chaos = [
        event.event_id
        for event in iter_delivery_events(iter_world_events(dataset_dir), config)
    ]
    second_chaos = [
        event.event_id
        for event in iter_delivery_events(iter_world_events(dataset_dir), config)
    ]
    assert first_chaos == second_chaos
    assert len(first_chaos) == len(first) * 2
    assert set(first_chaos) == set(first)


def test_dry_replay_reports_acknowledged_counts(tmp_path: Path):
    dataset_dir, manifest = _dataset(tmp_path)
    clock = [100.0]

    def monotonic() -> float:
        value = clock[0]
        clock[0] += 0.001
        return value

    report = replay_world(
        dataset_dir,
        DryRunPublisher(),
        ReplayConfig(mode="replay", publish_batch_size=7),
        monotonic=monotonic,
        sleep=lambda _: None,
    )

    expected = sum(manifest["expected_kafka_counts"].values())
    assert report.source_events == expected
    assert report.attempted == expected
    assert report.acknowledged == expected
    assert report.duplicate_attempts == 0
    assert report.acknowledged_by_topic == manifest["expected_kafka_counts"]


def test_accelerated_pacer_uses_configured_event_rate(tmp_path: Path):
    dataset_dir, _ = _dataset(tmp_path)
    events = list(iter_world_events(dataset_dir, max_events=3))
    now = [50.0]
    delays: list[float] = []

    def monotonic() -> float:
        return now[0]

    def sleep(delay: float) -> None:
        delays.append(delay)
        now[0] += delay

    pacer = EventPacer(
        ReplayConfig(mode="accelerated", rate=2.0),
        monotonic=monotonic,
        sleep=sleep,
    )
    for index, event in enumerate(events):
        pacer.wait(index, event)

    assert delays == [0.5, 0.5]


class _Future:
    def __init__(self, metadata) -> None:
        self.metadata = metadata
        self.timeout = None

    def get(self, timeout):
        self.timeout = timeout
        return self.metadata


class _Backend:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict]] = []
        self.flushed = 0
        self.closed = 0

    def send(self, topic: str, **kwargs):
        self.sent.append((topic, kwargs))
        return _Future(
            SimpleNamespace(topic=topic, partition=2, offset=len(self.sent) - 1)
        )

    def flush(self):
        self.flushed += 1

    def close(self):
        self.closed += 1


def test_kafka_publisher_routes_envelope_with_key_and_event_time_header(tmp_path: Path):
    dataset_dir, _ = _dataset(tmp_path)
    hand = next(
        event
        for event in iter_world_events(dataset_dir)
        if event.event_type == "poker.hand.completed"
    )
    backend = _Backend()
    producer = WorldEventProducer(
        producer=backend,
        topics=WorldTopics(hands="hands-test"),
    )

    pending = producer.publish(hand)
    ack = producer.acknowledge(pending, 12.0)
    producer.close()

    assert pending.topic == "hands-test"
    assert pending.key == hand.envelope["payload"]["table_id"]
    assert ack.partition == 2
    assert ack.offset == 0
    assert backend.sent[0][0] == "hands-test"
    kwargs = backend.sent[0][1]
    assert kwargs["key"] == pending.key
    assert kwargs["value"]["event_id"] == hand.event_id
    assert "timestamp_ms" not in kwargs
    headers = dict(kwargs["headers"])
    assert headers["event_type"] == b"poker.hand.completed"
    assert headers["occurred_at"] == hand.occurred_at.isoformat().encode("utf-8")
    assert backend.flushed == 1
    assert backend.closed == 1


class _Admin:
    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.created = []

    def list_topics(self):
        return self.existing

    def create_topics(self, topics, validate_only):
        assert validate_only is False
        self.created.extend(topics)
        self.existing.update(topic.name for topic in topics)

    def delete_topics(self, topics):
        self.existing.difference_update(topics)


def test_topic_manager_creates_only_missing_topics_with_cleanup_policies():
    topics = WorldTopics(
        hands="hands-v1",
        user_context="users-v1",
        sessions="sessions-v1",
        account_links="links-v1",
    )
    specs = world_topic_specs(topics, partitions=4, replication_factor=2)
    assert {spec.name: spec.configs["cleanup.policy"] for spec in specs} == {
        "hands-v1": "delete",
        "users-v1": "compact",
        "sessions-v1": "delete",
        "links-v1": "compact",
    }
    admin = _Admin({"hands-v1"})

    result = ensure_world_topics(
        topics=topics,
        partitions=4,
        replication_factor=2,
        admin_client=admin,
    )

    assert result == {
        "created": ["users-v1", "sessions-v1", "links-v1"],
        "existing": ["hands-v1"],
    }
    assert [topic.name for topic in admin.created] == result["created"]
    assert all(topic.num_partitions == 4 for topic in admin.created)
    assert all(topic.replication_factor == 2 for topic in admin.created)


def test_topic_reset_is_limited_to_managed_world_topics():
    topics = WorldTopics(
        hands="hands-v1",
        user_context="users-v1",
        sessions="sessions-v1",
        account_links="links-v1",
    )
    admin = _Admin({"hands-v1", "users-v1", "unrelated-topic"})

    result = reset_world_topics(
        topics=topics,
        partitions=3,
        replication_factor=1,
        admin_client=admin,
    )

    assert result["deleted"] == ["hands-v1", "users-v1"]
    assert set(result["created"]) == {"hands-v1", "users-v1", "sessions-v1", "links-v1"}
    assert "unrelated-topic" in admin.existing


def test_enrichment_topics_are_managed_separately_from_world_inputs():
    topics = EnrichmentTopics(
        player_context="player-context-v1",
        pair_features="pair-features-v1",
        dead_letters="dead-letter-v1",
    )
    specs = enrichment_topic_specs(topics, partitions=2, replication_factor=1)
    assert {spec.name: spec.configs["cleanup.policy"] for spec in specs} == {
        "player-context-v1": "delete",
        "pair-features-v1": "delete",
        "dead-letter-v1": "delete",
    }
    admin = _Admin({"player-context-v1", "hands-v1"})

    result = ensure_enrichment_topics(
        topics=topics,
        partitions=2,
        replication_factor=1,
        admin_client=admin,
    )

    assert result == {
        "created": ["pair-features-v1", "dead-letter-v1"],
        "existing": ["player-context-v1"],
    }
    assert "hands-v1" in admin.existing
