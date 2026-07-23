from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.events import HandCompletedPayload, build_event
from pipeline.kafka.topics import canonical_spcs_topics
from scripts.replay_alert_acceptance_spcs import (
    build_watermark_events,
    load_public_pack,
)
from scripts.verify_alert_acceptance_spcs import (
    _semantic_evidence,
    _semantic_pair,
    wait_for_consumer_commits,
)


def _hand_document() -> dict:
    payload = HandCompletedPayload(
        hand_id="HAND-1",
        table_id="TABLE-1",
        played_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
        dataset_split="acceptance",
        generator="pokerkit",
        small_blind=0.5,
        big_blind=1.0,
        num_players=2,
        pot_size=1.5,
        board=[],
        actions=[],
        players=[
            {
                "player_id": "A",
                "name": "A",
                "position": "SB",
                "stack_start": 100,
                "hole_cards": "As Ks",
                "won_amount": 1.5,
            },
            {
                "player_id": "B",
                "name": "B",
                "position": "BB",
                "stack_start": 100,
                "hole_cards": "2c 3c",
                "won_amount": 0,
            },
        ],
    )
    return build_event(
        event_type="poker.hand.completed",
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id="acceptance-v1",
        dataset_split="acceptance",
        occurred_at=payload.played_at,
    ).model_dump(mode="json")


def test_public_pack_replay_opens_hands_but_not_private_oracle(
    tmp_path: Path,
) -> None:
    hand_path = tmp_path / "events/hands.jsonl"
    hand_path.parent.mkdir(parents=True)
    hand_path.write_text(json.dumps(_hand_document()) + "\n")
    private_path = tmp_path / "private_oracle/sentinel.json"
    private_path.parent.mkdir(parents=True)
    private_path.write_text("{not-json")
    artifacts = {
        "events/hands.jsonl": hashlib.sha256(hand_path.read_bytes()).hexdigest(),
        "private_oracle/sentinel.json": hashlib.sha256(
            private_path.read_bytes()
        ).hexdigest(),
    }
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "product_type": "alert_acceptance",
                "training_allowed": False,
                "private_oracle_after_scoring_only": True,
                "dataset_id": "acceptance-v1",
                "dataset_split": "acceptance",
                "counts": {"hands": 1},
                "artifacts": artifacts,
            }
        )
    )

    manifest, hands = load_public_pack(tmp_path)

    assert manifest["training_allowed"] is False
    assert len(hands) == 1


def test_watermarks_cover_every_partition_twice() -> None:
    rows = build_watermark_events(
        _hand_document(),
        partitions=[0, 1, 2],
        marker_dataset_id="acceptance-v1-watermark-v1",
    )

    assert len(rows) == 6
    assert {(role, partition) for role, partition, _event in rows} == {
        (role, partition)
        for role in ("context-flush", "pair-flush")
        for partition in (0, 1, 2)
    }
    assert all(
        event.dataset_id == "acceptance-v1-watermark-v1"
        for _role, _partition, event in rows
    )


def test_v2_semantic_comparison_ignores_only_transport_lineage() -> None:
    pair = {
        "event_id": "pair-v1",
        "emitted_at": "old",
        "payload": {
            "hand_id": "H",
            "source_player_context_event_id_a": "context-v1-a",
            "source_player_context_event_id_b": "context-v1-b",
            "current_hand": {"position_gap": 1},
        },
        "upstream_rule_evidence": [
            {
                "payload": {
                    "evidence": {
                        "source_pair_feature_event_id": "pair-v1",
                        "observed_value": 1,
                    }
                }
            }
        ],
    }
    deployed = json.loads(json.dumps(pair))
    deployed["event_id"] = "pair-v2"
    deployed["emitted_at"] = "new"
    deployed["payload"]["source_player_context_event_id_a"] = "context-v2-a"
    deployed["payload"]["source_player_context_event_id_b"] = "context-v2-b"
    deployed["upstream_rule_evidence"][0]["payload"]["evidence"][
        "source_pair_feature_event_id"
    ] = "pair-v2"

    assert _semantic_pair(pair) == _semantic_pair(deployed)
    deployed["payload"]["current_hand"]["position_gap"] = 2
    assert _semantic_pair(pair) != _semantic_pair(deployed)

    evidence = {
        "emitted_at": "old",
        "payload": {
            "evidence": {
                "source_pair_feature_event_id": "pair-v1",
                "observed_value": 1,
            }
        },
    }
    changed = json.loads(json.dumps(evidence))
    changed["payload"]["evidence"]["source_pair_feature_event_id"] = "pair-v2"
    assert _semantic_evidence(evidence) == _semantic_evidence(changed)


def test_canonical_spcs_boundary_is_unique_and_synthetic() -> None:
    topics = canonical_spcs_topics()

    assert set(topics) == {
        "hands",
        "player_context",
        "pair_features",
        "risk_scores",
        "rule_evidence",
        "review_decisions",
        "risk_alerts",
        "dead_letters",
    }
    assert all(value.startswith("poker.synthetic.") for value in topics.values())
    assert len(set(topics.values())) == 8


def test_commit_audit_allows_checkpoint_managed_context(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    topics = canonical_spcs_topics()

    @dataclass(frozen=True)
    class Partition:
        topic: str
        partition: int

    class Admin:
        def __init__(self, **_kwargs: object) -> None:
            pass

        def list_consumer_group_offsets(
            self, group_id: str
        ) -> dict[Partition, SimpleNamespace]:
            if group_id == "context":
                return {}
            topic = (
                topics["player_context"]
                if group_id == "pairs"
                else topics["pair_features"]
            )
            return {Partition(topic, 0): SimpleNamespace(offset=11)}

        def close(self) -> None:
            pass

    monkeypatch.setattr("kafka.KafkaAdminClient", Admin)
    document = json.dumps(
        {"dataset_id": "acceptance-v1", "payload": {"hand_id": "H"}}
    ).encode()
    outputs = {
        "player_context": [SimpleNamespace(partition=0, offset=4, value=document)],
        "pair_features": [SimpleNamespace(partition=0, offset=7, value=document)],
    }
    manifest = {
        "topics": topics,
        "dataset_id": "acceptance-v1",
        "target_hand_ids": ["H"],
        "published_hands": [{"partition": 0, "offset": 2}],
        "watermarks": {"records": [{"partition": 0, "offset": 3}]},
        "consumer_groups": {
            "context": "context",
            "pair_features": "pairs",
            "risk": "risk",
        },
    }

    result = wait_for_consumer_commits(
        manifest, outputs, client_kwargs={}, timeout_seconds=0.1
    )

    assert result["context"]["required_broker_commit"] is False
    assert result["context"]["offsets"][f"{topics['hands']}[0]"] == -1
    assert result["pairs"]["required_broker_commit"] is True
    assert result["risk"]["required_broker_commit"] is True


def test_public_pack_rejects_training_product(tmp_path: Path) -> None:
    (tmp_path / "manifest.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "product_type": "alert_acceptance",
                "training_allowed": True,
            }
        )
    )

    with pytest.raises(ValueError, match="training-excluded"):
        load_public_pack(tmp_path)
