"""Contracts for the controlled retirement of legacy ``POKER_REALTIME``.

R6 is a replacement proof, not an assertion that the legacy per-player
ensemble and the canonical per-hand CatBoost policy produce identical scores.
This module keeps the hard gates explicit:

* exact bounded hand/input identity;
* complete legacy and canonical persistence coverage;
* no unexpected live dependency on legacy Kafka topics; and
* independently accepted canonical scores, decisions, evidence, and alerts.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from pipeline.events import HAND_COMPLETED, validate_event


LEGACY_HANDS_TOPIC = "hands.raw"
LEGACY_ALERTS_TOPIC = "alerts.out"
LEGACY_GROUP_ID = "realtime-processor"
LEGACY_SERVICE = "POKER_REALTIME"
R6_RUN_TYPE = "poker_realtime_retirement"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


def sha256_path(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    values = [
        json.loads(line)
        for line in path.read_text().splitlines()
        if line.strip()
    ]
    if any(not isinstance(value, dict) for value in values):
        raise ValueError(f"expected JSON objects in {path}")
    return values


def load_r6_source(
    manifest_path: Path,
    spcs_report_path: Path,
    sink_report_path: Path,
) -> dict[str, Any]:
    """Load and cross-check the accepted D7 handoff used by R6."""

    manifest = _read_json(manifest_path)
    spcs_report = _read_json(spcs_report_path)
    sink_report = _read_json(sink_report_path)
    dataset_id = str(manifest.get("dataset_id", ""))
    target_hand_ids = tuple(str(value) for value in manifest.get("target_hand_ids", []))
    if (
        manifest.get("schema_version") != 1
        or manifest.get("run_type") != "alert_acceptance_spcs"
        or manifest.get("training_allowed") is not False
        or not dataset_id
        or not target_hand_ids
        or len(target_hand_ids) != len(set(target_hand_ids))
        or int(manifest.get("expected_counts", {}).get("hands", -1))
        != len(target_hand_ids)
    ):
        raise ValueError("R6 requires one valid sealed D7 replay manifest")
    if (
        spcs_report.get("status") != "passed"
        or spcs_report.get("dataset_id") != dataset_id
        or spcs_report.get("target_dead_letters") != 0
    ):
        raise ValueError("R6 requires a passed target-DLQ-free D7 SPCS report")
    if (
        sink_report.get("status") != "passed"
        or sink_report.get("dataset_id") != dataset_id
        or sink_report.get("snowflake_sinks") != "passed"
        or sink_report.get("admin") != "passed"
    ):
        raise ValueError("R6 requires a passed D7 sink/admin report")

    pack_dir = Path(str(manifest.get("acceptance_pack", ""))).resolve()
    pack_manifest_path = pack_dir / "manifest.json"
    pack_manifest = _read_json(pack_manifest_path)
    if (
        pack_manifest.get("schema_version") != 1
        or pack_manifest.get("product_type") != "alert_acceptance"
        or pack_manifest.get("training_allowed") is not False
        or pack_manifest.get("dataset_id") != dataset_id
    ):
        raise ValueError("R6 acceptance pack is not sealed and training-excluded")
    artifacts = pack_manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        raise ValueError("R6 acceptance pack has no artifact hash bindings")
    for relative, expected_hash in artifacts.items():
        artifact = pack_dir / str(relative)
        if (
            not artifact.is_file()
            or not _SHA256.fullmatch(str(expected_hash))
            or sha256_path(artifact) != expected_hash
        ):
            raise ValueError(f"R6 acceptance artifact hash mismatch: {relative}")

    hand_events = _read_jsonl(pack_dir / "events" / "hands.jsonl")
    validated = [validate_event(document) for document in hand_events]
    observed_ids = tuple(str(event.payload["hand_id"]) for event in validated)
    if (
        set(observed_ids) != set(target_hand_ids)
        or len(observed_ids) != len(target_hand_ids)
        or any(
            event.event_type != HAND_COMPLETED
            or event.dataset_id != dataset_id
            for event in validated
        )
    ):
        raise ValueError("R6 hand events do not match the sealed D7 target")

    return {
        "dataset_id": dataset_id,
        "target_hand_ids": target_hand_ids,
        "hand_events": hand_events,
        "hand_payloads": [document["payload"] for document in hand_events],
        "expected_counts": dict(manifest["expected_counts"]),
        "manifest": manifest,
        "spcs_report": spcs_report,
        "sink_report": sink_report,
        "source_hashes": {
            "d7_manifest": sha256_path(manifest_path),
            "d7_spcs_report": sha256_path(spcs_report_path),
            "d7_sink_report": sha256_path(sink_report_path),
            "acceptance_pack_manifest": sha256_path(pack_manifest_path),
        },
    }


def expected_legacy_counts(hand_payloads: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    hands = len(hand_payloads)
    players = sum(len(hand["players"]) for hand in hand_payloads)
    actions = sum(len(hand["actions"]) for hand in hand_payloads)
    return {
        "raw_hands": hands,
        "raw_players": players,
        "raw_actions": actions,
        "features": players,
        "rule_flags": players,
    }


def accepted_admin_ids(spcs_report: Mapping[str, Any]) -> set[str]:
    lineage = spcs_report.get("schema_v2_lineage")
    if not isinstance(lineage, dict):
        raise ValueError("D7 SPCS report is missing schema_v2_lineage")
    return {
        str(row["risk_alert_event_id"])
        for row in lineage.values()
        if isinstance(row, dict) and row.get("risk_alert_event_id") is not None
    }


def validate_replay_manifest(value: Mapping[str, Any]) -> dict[str, Any]:
    records = value.get("records")
    target_ids = value.get("target_hand_ids")
    source_hashes = value.get("source_hashes")
    expected = value.get("expected_legacy_counts")
    records_valid = isinstance(records, list) and all(
        isinstance(record, Mapping)
        and isinstance(record.get("partition"), int)
        and isinstance(record.get("offset"), int)
        and int(record["partition"]) >= 0
        and int(record["offset"]) >= 0
        for record in records
    )
    if (
        value.get("schema_version") != 1
        or value.get("run_type") != R6_RUN_TYPE
        or value.get("phase") != "legacy_replay"
        or value.get("status") != "published"
        or value.get("training_allowed") is not False
        or value.get("topic") != LEGACY_HANDS_TOPIC
        or value.get("consumer_group") != LEGACY_GROUP_ID
        or not isinstance(records, list)
        or not isinstance(target_ids, list)
        or not isinstance(source_hashes, dict)
        or not source_hashes
        or not isinstance(expected, dict)
        or not records_valid
        or len(records) != len(target_ids)
        or len(target_ids) != len(set(target_ids))
        or {str(record.get("hand_id")) for record in records} != set(target_ids)
        or len(
            {
                (int(record.get("partition", -1)), int(record.get("offset", -1)))
                for record in records
            }
        )
        != len(records)
        or any(not _SHA256.fullmatch(str(value)) for value in source_hashes.values())
        or any(int(expected.get(name, 0)) < 1 for name in (
            "raw_hands",
            "raw_players",
            "raw_actions",
            "features",
            "rule_flags",
        ))
        or not re.fullmatch(r"[0-9a-f]{40}", str(value.get("source_commit", "")))
        or not value.get("started_at")
        or not value.get("completed_at")
    ):
        raise ValueError("invalid R6 legacy replay manifest")
    return dict(value)


def _timestamp(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return timestamp.isoformat().replace("+00:00", "Z")


def _array(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return [value]
        return list(decoded) if isinstance(decoded, list) else [decoded]
    if hasattr(value, "tolist"):
        value = value.tolist()
    return list(value) if isinstance(value, (list, tuple)) else [value]


def _float(value: Any) -> float:
    result = float(value)
    if not math.isfinite(result):
        raise ValueError("R6 input contains a non-finite number")
    return round(result, 12)


def _records_by_key(
    frame: pd.DataFrame,
    keys: tuple[str, ...],
    fields: tuple[str, ...],
) -> dict[tuple[str, ...], dict[str, Any]]:
    rows: dict[tuple[str, ...], dict[str, Any]] = {}
    for raw in frame.to_dict(orient="records"):
        key = tuple(str(raw[name]) for name in keys)
        if key in rows:
            raise ValueError(f"duplicate legacy row for {key}")
        rows[key] = {name: raw.get(name) for name in fields}
    return rows


def compare_legacy_inputs(
    hand_payloads: Sequence[Mapping[str, Any]],
    *,
    raw_hands: pd.DataFrame,
    raw_players: pd.DataFrame,
    raw_actions: pd.DataFrame,
    features: pd.DataFrame,
    rule_flags: pd.DataFrame,
    alerts: pd.DataFrame,
) -> dict[str, Any]:
    """Require lossless raw inputs and complete legacy feature/rule coverage."""

    expected_hands: dict[tuple[str, ...], dict[str, Any]] = {}
    expected_players: dict[tuple[str, ...], dict[str, Any]] = {}
    expected_actions: dict[tuple[str, ...], dict[str, Any]] = {}
    for hand in hand_payloads:
        hand_id = str(hand["hand_id"])
        expected_hands[(hand_id,)] = {
            "table_id": str(hand["table_id"]),
            "played_at": _timestamp(hand["played_at"]),
            "small_blind": _float(hand["small_blind"]),
            "big_blind": _float(hand["big_blind"]),
            "num_players": int(hand["num_players"]),
            "pot_size": _float(hand["pot_size"]),
            "board": _array(hand.get("board")),
            "dataset_split": str(hand.get("dataset_split", "live")),
        }
        for player in hand["players"]:
            expected_players[(hand_id, str(player["player_id"]))] = {
                "name": str(player["name"]),
                "position": str(player["position"]),
                "stack_start": _float(player["stack_start"]),
                "hole_cards": player.get("hole_cards"),
                "won_amount": _float(player["won_amount"]),
            }
        for action in hand["actions"]:
            expected_actions[(hand_id, str(action["sequence_no"]))] = {
                "player_id": str(action["player_id"]),
                "street": str(action["street"]),
                "action_type": str(action["action_type"]),
                "amount": _float(action["amount"]),
            }

    actual_hands = _records_by_key(
        raw_hands,
        ("hand_id",),
        (
            "table_id",
            "played_at",
            "small_blind",
            "big_blind",
            "num_players",
            "pot_size",
            "board",
            "dataset_split",
        ),
    )
    for row in actual_hands.values():
        row["table_id"] = str(row["table_id"])
        row["played_at"] = _timestamp(row["played_at"])
        for name in ("small_blind", "big_blind", "pot_size"):
            row[name] = _float(row[name])
        row["num_players"] = int(row["num_players"])
        row["board"] = _array(row["board"])
        row["dataset_split"] = str(row.get("dataset_split", "live"))

    actual_players = _records_by_key(
        raw_players,
        ("hand_id", "player_id"),
        ("name", "position", "stack_start", "hole_cards", "won_amount"),
    )
    for row in actual_players.values():
        row["name"] = str(row["name"])
        row["position"] = str(row["position"])
        row["stack_start"] = _float(row["stack_start"])
        row["hole_cards"] = row.get("hole_cards")
        row["won_amount"] = _float(row["won_amount"])

    actual_actions = _records_by_key(
        raw_actions,
        ("hand_id", "sequence_no"),
        ("player_id", "street", "action_type", "amount"),
    )
    for row in actual_actions.values():
        row["player_id"] = str(row["player_id"])
        row["street"] = str(row["street"])
        row["action_type"] = str(row["action_type"])
        row["amount"] = _float(row["amount"])

    feature_keys = {
        (str(row.hand_id), str(row.player_id))
        for row in features[["hand_id", "player_id"]].itertuples(index=False)
    }
    rule_keys = {
        (str(row.hand_id), str(row.player_id))
        for row in rule_flags[["hand_id", "player_id"]].itertuples(index=False)
    }
    expected_player_keys = set(expected_players)
    input_errors: list[str] = []
    errors: list[str] = []
    if actual_hands != expected_hands:
        input_errors.append(
            "RAW_HANDS differs from the sealed canonical payload projection"
        )
    if actual_players != expected_players:
        input_errors.append(
            "RAW_PLAYERS differs from the sealed canonical payload projection"
        )
    if actual_actions != expected_actions:
        input_errors.append(
            "RAW_ACTIONS differs from the sealed canonical payload projection"
        )
    errors.extend(input_errors)
    if (
        feature_keys != expected_player_keys
        or len(features) != len(feature_keys)
    ):
        errors.append("FEATURES does not cover every target hand/player exactly once")
    if (
        rule_keys != expected_player_keys
        or len(rule_flags) != len(rule_keys)
    ):
        errors.append("RULE_FLAGS does not cover every target hand/player exactly once")

    alert_rows = alerts.to_dict(orient="records")
    target_hands = {key[0] for key in expected_hands}
    if any(str(row.get("hand_id")) not in target_hands for row in alert_rows):
        errors.append("legacy ALERTS escaped the bounded target hand set")
    risk_scores = [float(row["risk_score"]) for row in alert_rows]
    if any(not 0.0 <= score <= 1.0 for score in risk_scores):
        errors.append("legacy ALERTS contains an invalid risk score")
    alert_ids = [str(row["alert_id"]) for row in alert_rows]
    if len(alert_ids) != len(set(alert_ids)):
        errors.append("legacy ALERTS contains duplicate alert IDs")

    expected_counts = expected_legacy_counts(hand_payloads)
    observed_counts = {
        "raw_hands": len(raw_hands),
        "raw_players": len(raw_players),
        "raw_actions": len(raw_actions),
        "features": len(features),
        "rule_flags": len(rule_flags),
        "alerts": len(alerts),
    }
    return {
        "status": "passed" if not errors else "failed",
        "errors": errors,
        "expected_counts": expected_counts,
        "observed_counts": observed_counts,
        "input_identity": "passed" if not input_errors else "failed",
        "feature_coverage": (
            "passed"
            if feature_keys == expected_player_keys
            and rule_keys == expected_player_keys
            and len(features) == len(feature_keys)
            and len(rule_flags) == len(rule_keys)
            else "failed"
        ),
        "legacy_alerts": {
            "rows": len(alert_rows),
            "hands": len({str(row["hand_id"]) for row in alert_rows}),
            "minimum_score": min(risk_scores) if risk_scores else None,
            "maximum_score": max(risk_scores) if risk_scores else None,
        },
    }


def _spec_legacy_topics(spec: str) -> set[str]:
    topics: set[str] = set()
    if re.search(r"(?m)^\s*KAFKA_HANDS_TOPIC:\s*[\"']?hands\.raw[\"']?\s*$", spec):
        topics.add(LEGACY_HANDS_TOPIC)
    if re.search(r"(?m)^\s*KAFKA_ALERTS_TOPIC:\s*[\"']?alerts\.out[\"']?\s*$", spec):
        topics.add(LEGACY_ALERTS_TOPIC)
    return topics


def build_dependency_audit(
    *,
    service_specs: Mapping[str, str],
    kafka_groups: Sequence[Mapping[str, Any]],
    local_processes: Sequence[str] = (),
) -> dict[str, Any]:
    """Classify live legacy-topic dependencies and return a fail-closed gate."""

    services = {
        str(name): sorted(_spec_legacy_topics(spec))
        for name, spec in service_specs.items()
        if _spec_legacy_topics(spec)
    }
    active_states = {"STABLE", "PREPARINGREBALANCE", "COMPLETINGREBALANCE"}
    active_groups = [
        {
            "group_id": str(group["group_id"]),
            "state": str(group.get("state", "")).upper(),
            "members": int(group.get("members", 0)),
            "topics": sorted(str(topic) for topic in group.get("topics", [])),
        }
        for group in kafka_groups
        if str(group.get("state", "")).upper() in active_states
        and int(group.get("members", 0)) > 0
        and set(str(topic) for topic in group.get("topics", []))
        & {LEGACY_HANDS_TOPIC, LEGACY_ALERTS_TOPIC}
    ]
    blockers: list[str] = []
    if services.get(LEGACY_SERVICE) != [LEGACY_ALERTS_TOPIC, LEGACY_HANDS_TOPIC]:
        blockers.append("POKER_REALTIME does not own both expected legacy topics")
    unexpected_services = sorted(set(services) - {LEGACY_SERVICE})
    if unexpected_services:
        blockers.append(
            "unexpected services reference legacy topics: "
            + ",".join(unexpected_services)
        )
    unexpected_groups = [
        group["group_id"]
        for group in active_groups
        if group["group_id"] != LEGACY_GROUP_ID
        or LEGACY_ALERTS_TOPIC in group["topics"]
    ]
    if unexpected_groups:
        blockers.append(
            "unexpected active Kafka groups depend on legacy topics: "
            + ",".join(sorted(unexpected_groups))
        )
    filtered_processes = [
        value
        for value in local_processes
        if any(
            marker in value
            for marker in (
                "scripts/realtime.py",
                "scripts/generate.py",
                "flink_realtime",
                "flink-realtime",
            )
        )
    ]
    if filtered_processes:
        blockers.append("local legacy producer/consumer processes are running")
    return {
        "status": "passed" if not blockers else "failed",
        "blockers": blockers,
        "service_dependencies": services,
        "active_kafka_dependencies": active_groups,
        "local_legacy_processes": filtered_processes,
    }


def output_contract_comparison(
    *,
    legacy_alert_rows: int,
    legacy_alert_hands: int,
    canonical_scores: int,
    canonical_decisions: int,
    canonical_alerts: int,
) -> dict[str, Any]:
    """Describe the output comparison without asserting false numeric parity."""

    return {
        "comparison_mode": "replacement_contract",
        "numeric_score_equality": "not_applicable",
        "reason": (
            "legacy persists thresholded per-player ensemble alerts; canonical "
            "persists complete per-hand CatBoost scores, review decisions, "
            "rule evidence, and deterministic alerts"
        ),
        "legacy": {
            "alert_rows": int(legacy_alert_rows),
            "alert_hands": int(legacy_alert_hands),
            "full_score_rows_persisted": False,
            "review_decisions_persisted": False,
        },
        "canonical": {
            "score_rows": int(canonical_scores),
            "decision_rows": int(canonical_decisions),
            "alert_rows": int(canonical_alerts),
            "full_score_rows_persisted": True,
            "review_decisions_persisted": True,
        },
    }
