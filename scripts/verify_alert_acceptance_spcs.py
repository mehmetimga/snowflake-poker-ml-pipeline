#!/usr/bin/env python3
"""Verify an offset-bounded canonical Confluent -> SPCS acceptance replay."""

from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, TypeVar

from pydantic import BaseModel

from pipeline.config import get_settings
from pipeline.events import (
    PairFeatureEvent,
    PlayerHandContextEventV2,
    ReviewDecisionEvent,
    RiskAlertEvent,
    RiskScoreEvent,
    RuleEvidenceEvent,
)
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.kafka.topics import canonical_spcs_topics


ModelT = TypeVar("ModelT", bound=BaseModel)


def read_bounded_topic(
    topic: str,
    starts: dict[str, int],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> list[Any]:
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(group_id=None, enable_auto_commit=False, **client_kwargs)
    records: list[Any] = []
    try:
        assignments = [
            TopicPartition(topic, int(partition))
            for partition in sorted(starts, key=int)
        ]
        consumer.assign(assignments)
        endings = consumer.end_offsets(assignments)
        for item in assignments:
            start = int(starts[str(item.partition)])
            if start > endings[item]:
                raise ValueError(f"invalid start offset for {topic}[{item.partition}]")
            consumer.seek(item, start)
        expected = sum(endings[item] - consumer.position(item) for item in assignments)
        deadline = time.monotonic() + timeout_seconds
        while len(records) < expected and time.monotonic() < deadline:
            for messages in consumer.poll(timeout_ms=500, max_records=500).values():
                records.extend(messages)
        if len(records) != expected:
            raise TimeoutError(
                f"read {len(records)} of {expected} records from {topic}"
            )
        return records
    finally:
        consumer.close()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = json.loads(path.read_text())
    if (
        manifest.get("schema_version") != 1
        or manifest.get("run_type") != "alert_acceptance_spcs"
        or manifest.get("training_allowed") is not False
        or manifest.get("topics") != canonical_spcs_topics()
    ):
        raise ValueError("SPCS acceptance replay manifest is invalid")
    expected = manifest.get("expected_counts", {})
    if (
        len(manifest.get("target_hand_ids", [])) != expected.get("hands")
        or len(set(manifest.get("target_player_ids", []))) != 30
        or len(manifest.get("published_hands", [])) != expected.get("hands")
    ):
        raise ValueError("SPCS acceptance replay manifest counts are invalid")
    if any(
        not topic.startswith("poker.synthetic.")
        for topic in manifest["topics"].values()
    ):
        raise ValueError("SPCS acceptance replay escaped the synthetic boundary")
    return manifest


def _target_documents(
    messages: list[Any],
    *,
    dataset_id: str,
    hand_ids: set[str],
) -> list[tuple[Any, dict[str, Any]]]:
    result = []
    for message in messages:
        try:
            document = json.loads(message.value)
        except (TypeError, json.JSONDecodeError):
            continue
        payload = document.get("payload") if isinstance(document, dict) else None
        if (
            document.get("dataset_id") == dataset_id
            and isinstance(payload, dict)
            and payload.get("hand_id") in hand_ids
        ):
            result.append((message, document))
    return result


def _models(
    messages: list[Any],
    model: type[ModelT],
    *,
    dataset_id: str,
    hand_ids: set[str],
) -> list[tuple[Any, ModelT]]:
    return [
        (message, model.model_validate(document))
        for message, document in _target_documents(
            messages,
            dataset_id=dataset_id,
            hand_ids=hand_ids,
        )
    ]


def read_outputs(
    manifest: dict[str, Any],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, list[Any]]:
    topics = manifest["topics"]
    starts = manifest["output_start_offsets"]
    observed_names = (
        "player_context",
        "pair_features",
        "risk_scores",
        "rule_evidence",
        "review_decisions",
        "risk_alerts",
        "dead_letters",
    )
    expected = manifest["expected_counts"]
    dataset_id = manifest["dataset_id"]
    hand_ids = set(manifest["target_hand_ids"])
    deadline = time.monotonic() + timeout_seconds
    latest: dict[str, list[Any]] = {}
    while time.monotonic() < deadline:
        latest = {
            name: read_bounded_topic(
                topics[name],
                starts[topics[name]],
                client_kwargs=client_kwargs,
                timeout_seconds=5,
            )
            for name in observed_names
        }
        counts = {
            name: len(
                _target_documents(
                    latest[name],
                    dataset_id=dataset_id,
                    hand_ids=hand_ids,
                )
            )
            for name in observed_names
            if name != "dead_letters"
        }
        if all(counts[name] >= expected[name] for name in counts):
            return latest
        time.sleep(1)
    raise TimeoutError(f"SPCS acceptance outputs did not complete: {counts}")


def wait_for_consumer_commits(
    manifest: dict[str, Any],
    outputs: dict[str, list[Any]],
    *,
    client_kwargs: dict[str, Any],
    timeout_seconds: float,
) -> dict[str, dict[str, Any]]:
    """Audit commits, allowing Flink checkpoint-only source progress."""
    from kafka import KafkaAdminClient

    topics = manifest["topics"]
    dataset_id = manifest["dataset_id"]
    hand_ids = set(manifest["target_hand_ids"])

    def message_requirements(
        topic: str, messages: list[Any]
    ) -> dict[tuple[str, int], int]:
        required: dict[tuple[str, int], int] = {}
        for message in messages:
            key = (topic, int(message.partition))
            required[key] = max(required.get(key, 0), int(message.offset) + 1)
        return required

    hand_requirements: dict[tuple[str, int], int] = {}
    for record in [
        *manifest["published_hands"],
        *manifest["watermarks"]["records"],
    ]:
        key = (topics["hands"], int(record["partition"]))
        hand_requirements[key] = max(
            hand_requirements.get(key, 0), int(record["offset"]) + 1
        )
    target_context_messages = [
        message
        for message, _document in _target_documents(
            outputs["player_context"],
            dataset_id=dataset_id,
            hand_ids=hand_ids,
        )
    ]
    target_pair_messages = [
        message
        for message, _document in _target_documents(
            outputs["pair_features"],
            dataset_id=dataset_id,
            hand_ids=hand_ids,
        )
    ]
    requirements = {
        manifest["consumer_groups"]["context"]: {
            "required_broker_commit": False,
            "mode": "flink_checkpoint_managed",
            "offsets": hand_requirements,
        },
        manifest["consumer_groups"]["pair_features"]: {
            "required_broker_commit": True,
            "mode": "kafka_group_commit",
            "offsets": message_requirements(
                topics["player_context"], target_context_messages
            ),
        },
        manifest["consumer_groups"]["risk"]: {
            "required_broker_commit": True,
            "mode": "kafka_group_commit",
            "offsets": message_requirements(
                topics["pair_features"], target_pair_messages
            ),
        },
    }
    admin = KafkaAdminClient(
        client_id="poker-alert-acceptance-spcs-verifier-v1",
        **client_kwargs,
    )
    deadline = time.monotonic() + timeout_seconds
    try:
        while time.monotonic() < deadline:
            result: dict[str, dict[str, Any]] = {}
            complete = True
            for group_id, group_config in requirements.items():
                group_requirements = group_config["offsets"]
                offsets = admin.list_consumer_group_offsets(group_id)
                committed = {
                    (item.topic, item.partition): metadata.offset
                    for item, metadata in offsets.items()
                    if metadata is not None
                }
                result[group_id] = {
                    "mode": group_config["mode"],
                    "required_broker_commit": group_config["required_broker_commit"],
                    "offsets": {
                        f"{topic}[{partition}]": committed.get((topic, partition), -1)
                        for topic, partition in sorted(group_requirements)
                    },
                }
                if group_config["required_broker_commit"] and any(
                    committed.get(key, -1) < expected
                    for key, expected in group_requirements.items()
                ):
                    complete = False
            if complete:
                return result
            time.sleep(1)
    finally:
        admin.close()
    raise TimeoutError(f"SPCS consumer groups did not commit target records: {result}")


def _without_runtime_times(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: _without_runtime_times(item)
            for key, item in value.items()
            if key not in {"emitted_at", "scored_at", "decided_at"}
        }
    if isinstance(value, list):
        return [_without_runtime_times(item) for item in value]
    return value


def _semantic_pair(value: dict[str, Any]) -> dict[str, Any]:
    """Remove only transport lineage that intentionally changed in context v2."""
    normalized = _without_runtime_times(value)
    normalized.pop("event_id", None)
    payload = normalized["payload"]
    payload.pop("source_player_context_event_id_a", None)
    payload.pop("source_player_context_event_id_b", None)
    for event in normalized.get("upstream_rule_evidence", []):
        event.get("payload", {}).get("evidence", {}).pop(
            "source_pair_feature_event_id", None
        )
    return normalized


def _semantic_evidence(value: dict[str, Any]) -> dict[str, Any]:
    normalized = _without_runtime_times(value)
    normalized["payload"].get("evidence", {}).pop("source_pair_feature_event_id", None)
    return normalized


def _unique_by_hand(
    values: list[tuple[Any, ModelT]],
) -> dict[str, ModelT]:
    grouped: dict[str, list[ModelT]] = {}
    for _message, event in values:
        grouped.setdefault(str(event.payload.hand_id), []).append(event)
    duplicates = {
        hand_id: len(rows) for hand_id, rows in grouped.items() if len(rows) != 1
    }
    if duplicates:
        raise ValueError(f"expected one output per hand: {duplicates}")
    return {hand_id: rows[0] for hand_id, rows in grouped.items()}


def verify(
    manifest: dict[str, Any],
    outputs: dict[str, list[Any]],
) -> dict[str, Any]:
    pack_dir = Path(manifest["acceptance_pack"])
    topics = manifest["topics"]
    dataset_id = manifest["dataset_id"]
    hand_ids = set(manifest["target_hand_ids"])
    player_ids = set(manifest["target_player_ids"])
    expected_counts = manifest["expected_counts"]

    contexts = _models(
        outputs["player_context"],
        PlayerHandContextEventV2,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    pairs = _models(
        outputs["pair_features"],
        PairFeatureEvent,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    scores = _models(
        outputs["risk_scores"],
        RiskScoreEvent,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    evidence = _models(
        outputs["rule_evidence"],
        RuleEvidenceEvent,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    decisions = _models(
        outputs["review_decisions"],
        ReviewDecisionEvent,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    alerts = _models(
        outputs["risk_alerts"],
        RiskAlertEvent,
        dataset_id=dataset_id,
        hand_ids=hand_ids,
    )
    observed_counts = {
        "player_context": len(contexts),
        "pair_features": len(pairs),
        "risk_scores": len(scores),
        "rule_evidence": len(evidence),
        "review_decisions": len(decisions),
        "risk_alerts": len(alerts),
    }
    if any(observed_counts[name] != expected_counts[name] for name in observed_counts):
        raise ValueError(
            f"target output counts changed: expected={expected_counts} "
            f"observed={observed_counts}"
        )

    expected_users = {
        row["user_id"]: row for row in _read_jsonl(pack_dir / "snapshots/users.jsonl")
    }
    contexts_per_hand = Counter()
    for message, event in contexts:
        player_id = event.payload.player.player_id
        contexts_per_hand[event.payload.hand_id] += 1
        if (
            message.key != player_id.encode()
            or player_id not in player_ids
            or event.payload.context.model_dump(mode="json")
            != expected_users[player_id]
            or event.payload.context_status != "matched"
            or event.payload.context_resolution.source != "snowflake"
            or event.payload.context_resolution.mode != "snowflake_point_in_time"
            or event.payload.context_resolution.policy_version
            != "snowflake-effective-at-v1"
        ):
            raise ValueError("lazy Snowflake context lineage changed")
    if set(contexts_per_hand) != hand_ids or set(contexts_per_hand.values()) != {6}:
        raise ValueError("each target hand must have six matched contexts")

    expected_pairs = {
        (row["payload"]["hand_id"], row["payload"]["pair_key"]): row
        for row in _read_jsonl(pack_dir / "expected/pair_features.jsonl")
    }
    actual_pairs = {
        (event.payload.hand_id, event.payload.pair_key): event.model_dump(mode="json")
        for _message, event in pairs
    }
    if set(actual_pairs) != set(expected_pairs):
        raise ValueError("deployed pair-feature hand/pair coverage changed")
    context_event_ids = {str(event.event_id) for _, event in contexts}
    for key, expected in expected_pairs.items():
        actual = actual_pairs[key]
        payload = actual["payload"]
        if (
            payload["source_player_context_event_id_a"] not in context_event_ids
            or payload["source_player_context_event_id_b"] not in context_event_ids
            or _semantic_pair(actual) != _semantic_pair(expected)
        ):
            raise ValueError(f"deployed pair feature changed: {key}")

    score_by_hand = _unique_by_hand(scores)
    decision_by_hand = _unique_by_hand(decisions)
    alert_by_hand = _unique_by_hand(alerts)
    expected_scores = {
        row["hand_id"]: row
        for row in _read_jsonl(pack_dir / "private_oracle/score_expectations.jsonl")
    }
    expected_evidence = {
        row["event_id"]: row
        for row in _read_jsonl(pack_dir / "private_oracle/rule_evidence_events.jsonl")
    }
    actual_evidence = {
        str(event.event_id): event.model_dump(mode="json")
        for _message, event in evidence
    }
    if set(actual_evidence) != set(expected_evidence):
        raise ValueError("deployed rule-evidence identities changed")
    for event_id, expected in expected_evidence.items():
        if _semantic_evidence(actual_evidence[event_id]) != _semantic_evidence(
            expected
        ):
            raise ValueError(f"deployed rule evidence changed: {event_id}")

    deployed_lineage: dict[str, dict[str, Any]] = {}
    for hand_id, expected in expected_scores.items():
        score = score_by_hand[hand_id]
        decision = decision_by_hand[hand_id]
        highest = max(
            score.payload.pair_scores,
            key=lambda row: row.calibrated_probability,
        )
        tolerance = float(expected["probability_tolerance"])
        if (
            score.payload.model_name != expected["model_name"]
            or score.payload.model_run_id != expected["model_run_id"]
            or abs(
                score.payload.hand_risk_probability
                - float(expected["hand_risk_probability"])
            )
            > tolerance
            or abs(
                score.payload.decision_threshold - float(expected["decision_threshold"])
            )
            > tolerance
            or score.payload.alert is not expected["expected_alert"]
            or highest.pair_key != expected["highest_risk_pair"]
            or [str(value) for value in score.payload.rule_evidence_event_ids]
            != expected["expected_rule_evidence_event_ids"]
        ):
            raise ValueError(f"deployed model score changed: {hand_id}")
        if (
            decision.payload.outcome != expected["expected_policy_outcome"]
            or decision.payload.risk_score_event_id != score.event_id
            or decision.payload.score_id != score.payload.score_id
        ):
            raise ValueError(f"deployed review decision changed: {hand_id}")
        expected_alert_id = expected["risk_alert_event_id"]
        if expected_alert_id is None:
            if hand_id in alert_by_hand:
                raise ValueError(f"unexpected deployed risk alert: {hand_id}")
        else:
            if hand_id not in alert_by_hand:
                raise ValueError(f"deployed risk alert is missing: {hand_id}")
            alert = alert_by_hand[hand_id]
            if (
                alert.payload.risk_score_event_id != score.event_id
                or alert.payload.review_decision_event_id != decision.event_id
                or alert.payload.score_id != score.payload.score_id
                or alert.payload.highest_risk_pair.pair_key
                != expected["highest_risk_pair"]
                or abs(
                    alert.payload.risk_probability
                    - float(expected["hand_risk_probability"])
                )
                > tolerance
            ):
                raise ValueError(f"deployed risk alert changed: {hand_id}")
        deployed_lineage[hand_id] = {
            "risk_score_event_id": str(score.event_id),
            "score_id": score.payload.score_id,
            "review_decision_event_id": str(decision.event_id),
            "risk_alert_event_id": (
                str(alert_by_hand[hand_id].event_id)
                if hand_id in alert_by_hand
                else None
            ),
        }

    target_needles = [value.encode() for value in hand_ids]
    target_dlq = [
        message
        for message in outputs["dead_letters"]
        if any(needle in (message.value or b"") for needle in target_needles)
    ]
    if target_dlq:
        raise ValueError(f"target acceptance records reached DLQ: {len(target_dlq)}")

    return {
        "status": "passed",
        "dataset_id": dataset_id,
        "counts": observed_counts,
        "observed_context_users": len(
            {event.payload.player.player_id for _, event in contexts}
        ),
        "context_source": "snowflake",
        "target_dead_letters": 0,
        "schema_v2_lineage": deployed_lineage,
        "topics": topics,
        "snowflake_sinks": "not_run",
        "admin": "not_run",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    args = parser.parse_args()
    manifest = load_manifest(args.manifest)
    settings = get_settings()
    if settings.kafka_security_protocol != "SASL_SSL":
        raise SystemExit("SPCS verification requires KAFKA_SECURITY_PROTOCOL=SASL_SSL")
    client_kwargs = {
        "bootstrap_servers": settings.kafka_bootstrap_servers.split(","),
        **kafka_client_kwargs(),
    }
    outputs = read_outputs(
        manifest,
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    report = verify(manifest, outputs)
    report["consumer_commits"] = wait_for_consumer_commits(
        manifest,
        outputs,
        client_kwargs=client_kwargs,
        timeout_seconds=args.timeout_seconds,
    )
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({**report, "report": str(args.report)}, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
