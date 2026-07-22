from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.events import HandCompletedPayload
from pipeline.kafka.topics import CdcSimulationTopics, ShadowSimulationTopics
from scripts.replay_shadow_simulation import build_context_events
from scripts.verify_shadow_simulation import load_manifest


def _hand() -> HandCompletedPayload:
    value = json.loads(
        Path("schemas/examples/cdc-canonical-hand-payload-v1.json").read_text()
    )
    return HandCompletedPayload.model_validate(value)


def test_shadow_context_is_deterministic_and_has_no_private_truth() -> None:
    first = build_context_events(_hand(), dataset_id="sim-cdc-v1")
    second = build_context_events(_hand(), dataset_id="sim-cdc-v1")

    assert [event.model_dump(mode="json") for event in first] == [
        event.model_dump(mode="json") for event in second
    ]
    assert len(first) == 6
    assert len({event.payload["user_id"] for event in first}) == 6
    assert all(event.dataset_id == "sim-cdc-v1" for event in first)
    serialized = json.dumps([event.model_dump(mode="json") for event in first])
    assert "collusion" not in serialized and "is_suspicious" not in serialized


def test_shadow_manifest_rejects_production_topic(tmp_path: Path) -> None:
    cdc = CdcSimulationTopics()
    shadow = ShadowSimulationTopics()
    manifest = {
        "schema_version": 1,
        "adapter_dataset_id": "sim-cdc-v1",
        "target_player_ids": [str(index) for index in range(6)],
        "build_versions": {"adapter": "a", "flink": "f", "risk": "r"},
        "model_run_id": "pair_test",
        "topics": {"source": cdc.source, "canonical": cdc.canonical, **shadow.__dict__},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))
    assert load_manifest(path) == manifest

    manifest["topics"]["risk_scores"] = "poker.risk-scores.v1"
    path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="contract"):
        load_manifest(path)
