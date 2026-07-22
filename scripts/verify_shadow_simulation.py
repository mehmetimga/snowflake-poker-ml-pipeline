#!/usr/bin/env python3
"""Verify one offset-bounded CDC -> Flink -> Go/Triton shadow replay."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from pipeline.config import get_settings
from pipeline.events import (
    PairFeatureEvent,
    PlayerHandContextEvent,
    ReviewDecisionEvent,
    RiskAlertEvent,
    RiskScoreEvent,
    RuleEvidenceEvent,
    validate_event,
)
from pipeline.features import PairFeatureCore
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import CdcSimulationTopics, ShadowSimulationTopics
from scripts.verify_cdc_simulation_remote import read_bounded_topic


ModelT = TypeVar("ModelT", bound=BaseModel)


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    cdc = CdcSimulationTopics()
    shadow = ShadowSimulationTopics()
    expected_topics = {
        "source": cdc.source,
        "canonical": cdc.canonical,
        **shadow.__dict__,
    }
    if manifest.get("schema_version") != 1 or manifest.get("topics") != expected_topics:
        raise ValueError("shadow simulation manifest topic contract is invalid")
    if not str(manifest.get("adapter_dataset_id", "")).startswith("sim-"):
        raise ValueError("shadow simulation dataset must start with sim-")
    if len(manifest.get("target_player_ids", [])) != 6:
        raise ValueError("shadow simulation requires exactly six target players")
    if len(set(manifest["target_player_ids"])) != 6:
        raise ValueError("target player IDs must be unique")
    if any(not topic.startswith("poker.sim.") for topic in expected_topics.values()):
        raise ValueError("shadow manifest contains a production topic")
    required_builds = {"adapter", "flink", "risk"}
    builds = manifest.get("build_versions", {})
    if set(builds) != required_builds or any(not str(value) for value in builds.values()):
        raise ValueError("shadow manifest build lineage is incomplete")
    if not str(manifest.get("model_run_id", "")):
        raise ValueError("shadow manifest model lineage is incomplete")
    return manifest


def _target_documents(messages: list[Any], hand_id: str) -> list[tuple[Any, dict]]:
    output = []
    for message in messages:
        try:
            value = json.loads(message.value)
        except (TypeError, json.JSONDecodeError):
            continue
        payload = value.get("payload") if isinstance(value, dict) else None
        if isinstance(payload, dict) and payload.get("hand_id") == hand_id:
            output.append((message, value))
    return output


def _models(
    messages: list[Any], hand_id: str, model: type[ModelT]
) -> list[tuple[Any, ModelT]]:
    return [
        (message, model.model_validate(document))
        for message, document in _target_documents(messages, hand_id)
    ]


def _read_outputs(
    manifest: dict[str, Any], client_kwargs: dict[str, Any], timeout_seconds: float
) -> dict[str, list[Any]]:
    topics = manifest["topics"]
    starts = manifest["output_start_offsets"]
    observed = (
        topics["canonical"],
        topics["player_context"],
        topics["pair_features"],
        topics["risk_scores"],
        topics["rule_evidence"],
        topics["review_decisions"],
        topics["risk_alerts"],
        topics["dead_letters"],
    )
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, list[Any]] = {}
    while time.monotonic() < deadline:
        latest = {
            topic: read_bounded_topic(
                topic, starts[topic], client_kwargs=client_kwargs, timeout_seconds=5
            )
            for topic in observed
        }
        hand_id = manifest["target_hand_id"]
        if (
            len(_target_documents(latest[topics["player_context"]], hand_id)) >= 6
            and len(_target_documents(latest[topics["pair_features"]], hand_id)) >= 15
            and len(_target_documents(latest[topics["risk_scores"]], hand_id)) >= 1
            and len(_target_documents(latest[topics["review_decisions"]], hand_id)) >= 1
        ):
            return latest
        time.sleep(1)
    counts = {
        name: len(_target_documents(latest.get(topic, []), manifest["target_hand_id"]))
        for name, topic in topics.items()
        if topic in latest
    }
    raise TimeoutError(f"shadow outputs did not complete: {counts}")


def _wait_for_adapter_commit(
    manifest: dict[str, Any], client_kwargs: dict[str, Any], timeout_seconds: float
) -> int:
    from kafka import KafkaAdminClient

    expected = int(manifest["source"]["offset"]) + 1
    partition = int(manifest["source"]["partition"])
    topic = manifest["topics"]["source"]
    group = manifest["consumer_groups"]["adapter"]
    admin = KafkaAdminClient(client_id="poker-shadow-simulation-verifier-v1", **client_kwargs)
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            offsets = admin.list_consumer_group_offsets(group)
            actual = max(
                (
                    metadata.offset
                    for item, metadata in offsets.items()
                    if item.topic == topic and item.partition == partition
                    and metadata is not None
                ),
                default=-1,
            )
            if actual >= expected:
                return actual
            time.sleep(0.5)
    finally:
        admin.close()
    raise TimeoutError(f"adapter group {group} did not commit source offset {expected}")


def verify(manifest: dict[str, Any], outputs: dict[str, list[Any]]) -> dict[str, Any]:
    topics = manifest["topics"]
    hand_id = manifest["target_hand_id"]
    dataset_id = manifest["adapter_dataset_id"]
    players = set(manifest["target_player_ids"])

    canonical = _target_documents(outputs[topics["canonical"]], hand_id)
    if len(canonical) != 1:
        raise ValueError(f"expected one target canonical hand, found {len(canonical)}")
    canonical_message, canonical_document = canonical[0]
    canonical_event = validate_event(canonical_document)
    if canonical_event.dataset_id != dataset_id:
        raise ValueError("adapter canonical dataset does not match the manifest")
    headers = {key: value.decode() for key, value in canonical_message.headers}
    if (
        int(headers.get("cdc_source_partition", -1))
        != int(manifest["source"]["partition"])
        or int(headers.get("cdc_source_offset", -1))
        != int(manifest["source"]["offset"])
    ):
        raise ValueError("canonical CDC lineage does not match the source record")

    enriched = _models(outputs[topics["player_context"]], hand_id, PlayerHandContextEvent)
    if len(enriched) != 6:
        raise ValueError(f"expected six enriched players, found {len(enriched)}")
    enriched_by_player = {event.payload.player.player_id: event for _, event in enriched}
    if set(enriched_by_player) != players:
        raise ValueError("enriched player set does not match the CDC hand")
    context_ids = {value["event_id"] for value in manifest["contexts"]}
    for message, event in enriched:
        if message.key != event.payload.player.player_id.encode():
            raise ValueError("player-context Kafka key is not the player ID")
        if event.dataset_id != dataset_id or event.payload.context_status != "matched":
            raise ValueError("target player was not joined to simulation context")
        if str(event.payload.source_context_event_id) not in context_ids:
            raise ValueError("player-context source ID is outside this replay")

    pairs = _models(outputs[topics["pair_features"]], hand_id, PairFeatureEvent)
    if len(pairs) != 15 or len({event.payload.pair_key for _, event in pairs}) != 15:
        raise ValueError("target hand did not produce exactly fifteen unique pairs")
    online = {str(event.event_id): event for _, event in pairs}
    ordered_enriched = sorted(
        enriched_by_player.values(),
        key=lambda event: (
            event.payload.played_at,
            event.payload.revision,
            event.payload.player.player_id,
            event.emitted_at,
            str(event.event_id),
        ),
    )
    offline = {
        str(event.event_id): event
        for event in PairFeatureCore().process_many(ordered_enriched)
    }
    if set(online) != set(offline) or any(
        online[event_id].payload != offline[event_id].payload for event_id in online
    ):
        raise ValueError("online/offline target pair-feature parity failed")
    for message, event in pairs:
        if message.key != event.payload.pair_key.encode():
            raise ValueError("pair-feature Kafka key is not the canonical pair key")

    scores = _models(outputs[topics["risk_scores"]], hand_id, RiskScoreEvent)
    decisions = _models(outputs[topics["review_decisions"]], hand_id, ReviewDecisionEvent)
    if len(scores) != 1 or len(decisions) != 1:
        raise ValueError("target must have exactly one score and one decision")
    score_message, score = scores[0]
    _, decision = decisions[0]
    if score_message.key != hand_id.encode() or score.dataset_id != dataset_id:
        raise ValueError("risk score key or dataset is invalid")
    if score.payload.service_build_version != manifest["build_versions"]["risk"]:
        raise ValueError("risk score build does not match the deployed image")
    if score.payload.model_run_id != manifest["model_run_id"]:
        raise ValueError("risk score model run does not match the manifest")
    if len(score.payload.pair_scores) != 15 or len(score.payload.player_scores) != 6:
        raise ValueError("risk score is not a complete six-player hand")
    if {str(value.feature_event_id) for value in score.payload.pair_scores} != set(online):
        raise ValueError("risk score does not reference the exact target pair features")
    if {value.player_id for value in score.payload.player_scores} != players:
        raise ValueError("risk score player set does not match the CDC hand")
    if decision.payload.risk_score_event_id != score.event_id:
        raise ValueError("review decision does not reference the target score")

    evidence = _models(outputs[topics["rule_evidence"]], hand_id, RuleEvidenceEvent)
    evidence_ids = {event.event_id for _, event in evidence}
    if len(evidence) != len(evidence_ids):
        raise ValueError("duplicate rule-evidence records found for the target")
    if set(score.payload.rule_evidence_event_ids) != evidence_ids:
        raise ValueError("risk-score rule-evidence references are incomplete")
    if {item.rule_event_id for item in decision.payload.rule_evidence} - evidence_ids:
        raise ValueError("review decision contains a broken evidence reference")

    alerts = _models(outputs[topics["risk_alerts"]], hand_id, RiskAlertEvent)
    expected_alerts = 1 if score.payload.alert else 0
    if len(alerts) != expected_alerts:
        raise ValueError(
            f"expected {expected_alerts} target alerts, found {len(alerts)}"
        )
    if alerts and alerts[0][1].payload.review_decision_event_id != decision.event_id:
        raise ValueError("risk alert does not reference the review decision")
    if alerts and set(alerts[0][1].payload.rule_evidence_event_ids) != evidence_ids:
        raise ValueError("risk alert rule-evidence references are incomplete")
    if outputs[topics["dead_letters"]]:
        raise ValueError("shadow replay produced dead-letter records")

    return {
        "status": "passed",
        "source_dataset_id": manifest["source_dataset_id"],
        "adapter_dataset_id": dataset_id,
        "target_hand_id": hand_id,
        "enriched_players": len(enriched),
        "pair_features": len(pairs),
        "rule_evidence": len(evidence),
        "risk_scores": len(scores),
        "review_decisions": len(decisions),
        "risk_alerts": len(alerts),
        "online_offline_pair_parity": "passed",
        "broken_references": 0,
        "dead_letters": 0,
        "risk_probability": score.payload.hand_risk_probability,
        "review_outcome": decision.payload.outcome,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    settings = get_settings()
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    committed = _wait_for_adapter_commit(
        manifest, client_kwargs, args.timeout_seconds
    )
    source_partition = int(manifest["source"]["partition"])
    source_offset = int(manifest["source"]["offset"])
    source_records = read_bounded_topic(
        manifest["topics"]["source"],
        {str(source_partition): source_offset},
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    source_matches = [
        message
        for message in source_records
        if message.partition == source_partition and message.offset == source_offset
    ]
    if len(source_matches) != 1:
        raise ValueError("managed CDC source record is no longer readable")
    source = source_matches[0]
    if (
        hashlib.sha256(source.key or b"").hexdigest()
        != manifest["source"]["key_sha256"]
        or hashlib.sha256(source.value or b"").hexdigest()
        != manifest["source"]["value_sha256"]
    ):
        raise ValueError("managed CDC source digest does not match the manifest")
    outputs = _read_outputs(manifest, client_kwargs, args.timeout_seconds)
    result = verify(manifest, outputs)
    result["adapter_committed_offset"] = committed
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
