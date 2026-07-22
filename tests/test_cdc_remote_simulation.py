from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.kafka.topics import (
    CdcSimulationTopics,
    ShadowSimulationTopics,
    cdc_simulation_topic_specs,
    ensure_cdc_simulation_topics,
    ensure_shadow_simulation_topics,
    shadow_simulation_topic_specs,
)
from scripts.verify_cdc_simulation_remote import load_manifest


class _Admin:
    def __init__(self, existing: set[str]) -> None:
        self.existing = set(existing)
        self.created = []
        self.closed = False

    def list_topics(self):
        return list(self.existing)

    def create_topics(self, topics, validate_only=False):
        assert validate_only is False
        self.created.extend(topics)
        self.existing.update(topic.name for topic in topics)

    def close(self):
        self.closed = True


def test_cdc_simulation_topics_are_isolated_and_source_is_ordered():
    topics = CdcSimulationTopics()
    specs = cdc_simulation_topic_specs(
        topics,
        output_partitions=3,
        replication_factor=2,
    )
    assert [spec.name for spec in specs] == [
        "poker.sim.cdc-hand-outbox.v1",
        "poker.sim.hands.raw.v1",
        "poker.sim.pipeline.dead-letter.v1",
    ]
    assert [spec.partitions for spec in specs] == [1, 3, 3]
    assert all(spec.replication_factor == 2 for spec in specs)
    assert all(spec.configs["cleanup.policy"] == "delete" for spec in specs)

    admin = _Admin({topics.source, "poker.hands.raw.v1"})
    result = ensure_cdc_simulation_topics(
        topics=topics,
        output_partitions=3,
        replication_factor=2,
        admin_client=admin,
    )

    assert result == {
        "created": [topics.canonical, topics.dead_letters],
        "existing": [topics.source],
    }
    assert "poker.hands.raw.v1" in admin.existing


def test_cdc_simulation_topics_reject_production_names():
    with pytest.raises(ValueError, match=r"poker\.sim"):
        cdc_simulation_topic_specs(
            CdcSimulationTopics(canonical="poker.hands.raw.v1")
        )


def test_shadow_simulation_topics_are_complete_and_isolated():
    topics = ShadowSimulationTopics()
    specs = shadow_simulation_topic_specs(
        topics,
        partitions=3,
        replication_factor=2,
    )

    assert [spec.name for spec in specs] == [
        topics.user_context,
        topics.player_context,
        topics.pair_features,
        topics.risk_scores,
        topics.rule_evidence,
        topics.review_decisions,
        topics.risk_alerts,
        topics.dead_letters,
    ]
    assert all(spec.partitions == 3 for spec in specs)
    assert specs[0].configs["cleanup.policy"] == "compact"
    assert all(spec.configs["cleanup.policy"] == "delete" for spec in specs[1:])

    admin = _Admin({topics.dead_letters})
    result = ensure_shadow_simulation_topics(
        topics=topics,
        partitions=3,
        replication_factor=2,
        admin_client=admin,
    )
    assert result["existing"] == [topics.dead_letters]
    assert set(result["created"]) == set(spec.name for spec in specs[:-1])


def test_shadow_simulation_topics_reject_production_names_and_duplicates():
    with pytest.raises(ValueError, match=r"poker\.sim"):
        shadow_simulation_topic_specs(
            ShadowSimulationTopics(pair_features="poker.pair-features.v1")
        )
    with pytest.raises(ValueError, match="unique"):
        shadow_simulation_topic_specs(
            ShadowSimulationTopics(risk_scores="poker.sim.risk-alerts.v1")
        )


def test_remote_manifest_is_exactly_scoped(tmp_path: Path):
    topics = CdcSimulationTopics()
    manifest = {
        "schema_version": 1,
        "topics": {
            "source": topics.source,
            "canonical": topics.canonical,
            "dead_letters": topics.dead_letters,
        },
        "records": [
            {
                "scenario": f"scenario_{index}",
                "published_to_managed_kafka": index < 5,
            }
            for index in range(6)
        ],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    assert load_manifest(path) == manifest

    manifest["topics"]["canonical"] = "poker.hands.raw.v1"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="contract"):
        load_manifest(path)
