from __future__ import annotations

import pytest

from pipeline.kafka.topics import (
    CanonicalScoringTopics,
    canonical_scoring_topic_specs,
    ensure_canonical_scoring_topics,
)


class FakeAdmin:
    def __init__(self, existing: set[str]) -> None:
        self.existing = existing
        self.created: list[object] = []

    def list_topics(self) -> set[str]:
        return self.existing

    def create_topics(self, topics: list[object], *, validate_only: bool) -> None:
        assert validate_only is False
        self.created.extend(topics)


def test_canonical_scoring_topics_are_synthetic_and_managed() -> None:
    specs = canonical_scoring_topic_specs(partitions=3, replication_factor=3)

    assert len(specs) == 5
    assert len({spec.name for spec in specs}) == 5
    assert all(spec.name.startswith("poker.synthetic.") for spec in specs)
    assert all(spec.partitions == 3 for spec in specs)
    assert all(spec.replication_factor == 3 for spec in specs)
    assert all(spec.configs["cleanup.policy"] == "delete" for spec in specs)


def test_canonical_scoring_topic_creation_is_additive() -> None:
    names = CanonicalScoringTopics()
    admin = FakeAdmin({names.risk_scores, names.dead_letters})

    result = ensure_canonical_scoring_topics(admin_client=admin)

    assert result["existing"] == [names.risk_scores, names.dead_letters]
    assert result["created"] == [
        names.rule_evidence,
        names.review_decisions,
        names.risk_alerts,
    ]
    assert [topic.name for topic in admin.created] == result["created"]


def test_canonical_scoring_topics_reject_non_synthetic_name() -> None:
    with pytest.raises(ValueError, match=r"poker\.synthetic"):
        canonical_scoring_topic_specs(
            CanonicalScoringTopics(risk_alerts="poker.risk-alerts.v1")
        )
