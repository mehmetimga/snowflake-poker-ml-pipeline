from __future__ import annotations

import pytest

from pipeline.kafka.topics import (
    CanonicalFlinkTopics,
    canonical_flink_topic_specs,
    ensure_canonical_flink_topics,
)


class FakeAdmin:
    def __init__(self, existing: set[str] | None = None) -> None:
        self.existing = existing or set()
        self.created: list[object] = []
        self.closed = False

    def list_topics(self) -> set[str]:
        return set(self.existing)

    def create_topics(
        self, topics: list[object], validate_only: bool
    ) -> None:
        assert validate_only is False
        self.created.extend(topics)

    def close(self) -> None:
        self.closed = True


def test_specs_are_hands_only_and_keep_dlq_longer() -> None:
    specs = canonical_flink_topic_specs()
    assert [spec.name for spec in specs] == [
        "poker.synthetic.hands.raw.v1",
        "poker.synthetic.hand-player-context.v2",
        "poker.synthetic.pair-features.context-v2.v1",
        "poker.synthetic.pipeline.dead-letter.v1",
    ]
    assert all(spec.partitions == 3 for spec in specs)
    assert specs[-1].configs["retention.ms"] == str(
        30 * 24 * 60 * 60 * 1_000
    )
    assert specs[0].configs["retention.ms"] == str(
        7 * 24 * 60 * 60 * 1_000
    )


def test_ensure_creates_only_missing_topics() -> None:
    topics = CanonicalFlinkTopics()
    admin = FakeAdmin(existing={topics.hands})
    result = ensure_canonical_flink_topics(
        topics=topics,
        admin_client=admin,
    )

    assert result["existing"] == [topics.hands]
    assert result["created"] == [
        topics.player_context,
        topics.pair_features,
        topics.dead_letters,
    ]
    assert [topic.name for topic in admin.created] == result["created"]
    assert admin.closed is False


def test_rejects_topics_outside_synthetic_boundary() -> None:
    with pytest.raises(ValueError, match="poker.synthetic"):
        canonical_flink_topic_specs(
            CanonicalFlinkTopics(hands="poker.hands.raw.v1")
        )
