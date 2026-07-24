from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
import pytest

from pipeline.events import HAND_COMPLETED, HandCompletedPayload, build_event
from pipeline.ops.realtime_retirement import (
    LEGACY_ALERTS_TOPIC,
    LEGACY_GROUP_ID,
    LEGACY_HANDS_TOPIC,
    LEGACY_SERVICE,
    R6_RUN_TYPE,
    build_dependency_audit,
    compare_legacy_inputs,
    expected_legacy_counts,
    load_r6_source,
    output_contract_comparison,
    validate_replay_manifest,
)
from scripts.manage_r6_realtime_suspension import (
    _validate_completed_check,
    _load_chain,
    _offsets_cover,
    _ready,
    _validate_start_report,
)


def _payload() -> dict:
    return {
        "hand_id": "r6-hand-1",
        "table_id": "r6-table-1",
        "played_at": "2026-07-24T10:00:00Z",
        "small_blind": 0.5,
        "big_blind": 1.0,
        "num_players": 2,
        "pot_size": 3.0,
        "board": ["As", "Kh", "2d"],
        "dataset_split": "acceptance",
        "generator": "pokerkit",
        "players": [
            {
                "player_id": "player-a",
                "name": "Player A",
                "position": "SB",
                "stack_start": 100.0,
                "hole_cards": "Ah Ad",
                "won_amount": 3.0,
            },
            {
                "player_id": "player-b",
                "name": "Player B",
                "position": "BB",
                "stack_start": 100.0,
                "hole_cards": "Ks Kd",
                "won_amount": 0.0,
            },
        ],
        "actions": [
            {
                "sequence_no": 0,
                "player_id": "player-a",
                "street": "preflop",
                "action_type": "raise",
                "amount": 2.0,
            },
            {
                "sequence_no": 1,
                "player_id": "player-b",
                "street": "preflop",
                "action_type": "fold",
                "amount": 0.0,
            },
        ],
    }


def _source_files(tmp_path: Path) -> tuple[Path, Path, Path]:
    pack = tmp_path / "pack"
    hands_path = pack / "events/hands.jsonl"
    hands_path.parent.mkdir(parents=True)
    played_at = datetime(2026, 7, 24, 10, tzinfo=timezone.utc)
    event = build_event(
        event_type=HAND_COMPLETED,
        aggregate_id="r6-hand-1",
        payload=HandCompletedPayload.model_validate(_payload()),
        dataset_id="r6-dataset",
        dataset_split="acceptance",
        occurred_at=played_at,
        emitted_at=played_at + timedelta(seconds=1),
    )
    hands_path.write_text(event.model_dump_json() + "\n")
    hands_hash = hashlib.sha256(hands_path.read_bytes()).hexdigest()
    pack_manifest = {
        "schema_version": 1,
        "product_type": "alert_acceptance",
        "training_allowed": False,
        "dataset_id": "r6-dataset",
        "artifacts": {"events/hands.jsonl": hands_hash},
    }
    (pack / "manifest.json").write_text(json.dumps(pack_manifest))

    manifest = {
        "schema_version": 1,
        "run_type": "alert_acceptance_spcs",
        "training_allowed": False,
        "dataset_id": "r6-dataset",
        "acceptance_pack": str(pack),
        "target_hand_ids": ["r6-hand-1"],
        "expected_counts": {
            "hands": 1,
            "player_context": 2,
            "pair_features": 1,
            "risk_scores": 1,
            "rule_evidence": 1,
            "review_decisions": 1,
            "risk_alerts": 1,
        },
        "topics": {
            "hands": "poker.synthetic.hands.raw.v1",
            "player_context": "poker.synthetic.hand-player-context.v2",
            "pair_features": "poker.synthetic.pair-features.context-v2.v1",
            "risk_scores": "poker.synthetic.risk-scores.v1",
            "rule_evidence": "poker.synthetic.rule-evidence.v1",
            "review_decisions": "poker.synthetic.review-decisions.v1",
            "risk_alerts": "poker.synthetic.risk-alerts.v1",
            "dead_letters": "poker.synthetic.pipeline.dead-letter.v1",
        },
    }
    spcs = {
        "status": "passed",
        "dataset_id": "r6-dataset",
        "target_dead_letters": 0,
        "schema_v2_lineage": {
            "r6-hand-1": {"risk_alert_event_id": "alert-1"}
        },
    }
    sink = {
        "status": "passed",
        "dataset_id": "r6-dataset",
        "snowflake_sinks": "passed",
        "admin": "passed",
    }
    manifest_path = tmp_path / "replay-manifest.json"
    spcs_path = tmp_path / "spcs-report.json"
    sink_path = tmp_path / "sink-report.json"
    manifest_path.write_text(json.dumps(manifest))
    spcs_path.write_text(json.dumps(spcs))
    sink_path.write_text(json.dumps(sink))
    return manifest_path, spcs_path, sink_path


def test_r6_source_requires_one_passed_hash_bound_d7_chain(tmp_path: Path) -> None:
    paths = _source_files(tmp_path)

    source = load_r6_source(*paths)

    assert source["dataset_id"] == "r6-dataset"
    assert source["target_hand_ids"] == ("r6-hand-1",)
    assert source["hand_payloads"] == [_payload()]
    assert set(source["source_hashes"]) == {
        "d7_manifest",
        "d7_spcs_report",
        "d7_sink_report",
        "acceptance_pack_manifest",
    }

    sink = json.loads(paths[2].read_text())
    sink["admin"] = "failed"
    paths[2].write_text(json.dumps(sink))
    with pytest.raises(ValueError, match="sink/admin"):
        load_r6_source(*paths)


def _legacy_frames() -> dict[str, pd.DataFrame]:
    payload = _payload()
    return {
        "raw_hands": pd.DataFrame(
            [
                {
                    "hand_id": payload["hand_id"],
                    "table_id": payload["table_id"],
                    "played_at": pd.Timestamp(payload["played_at"]),
                    "small_blind": payload["small_blind"],
                    "big_blind": payload["big_blind"],
                    "num_players": payload["num_players"],
                    "pot_size": payload["pot_size"],
                    "board": payload["board"],
                    "dataset_split": payload["dataset_split"],
                }
            ]
        ),
        "raw_players": pd.DataFrame(
            [
                {"hand_id": payload["hand_id"], **player}
                for player in payload["players"]
            ]
        ),
        "raw_actions": pd.DataFrame(
            [
                {"hand_id": payload["hand_id"], **action}
                for action in payload["actions"]
            ]
        ),
        "features": pd.DataFrame(
            [
                {
                    "hand_id": payload["hand_id"],
                    "player_id": player["player_id"],
                }
                for player in payload["players"]
            ]
        ),
        "rule_flags": pd.DataFrame(
            [
                {
                    "hand_id": payload["hand_id"],
                    "player_id": player["player_id"],
                }
                for player in payload["players"]
            ]
        ),
        "alerts": pd.DataFrame(
            [
                {
                    "alert_id": "legacy-alert-1",
                    "hand_id": payload["hand_id"],
                    "suspicious_player_id": "player-a",
                    "risk_score": 0.75,
                    "risk_level": "HIGH",
                    "model_scores": {"source": "hand_weighted_ensemble"},
                    "created_at": pd.Timestamp("2026-07-24T10:01:00Z"),
                }
            ]
        ),
    }


def test_legacy_input_and_coverage_comparison_is_exact() -> None:
    frames = _legacy_frames()

    result = compare_legacy_inputs([_payload()], **frames)

    assert result["status"] == "passed"
    assert result["input_identity"] == "passed"
    assert result["feature_coverage"] == "passed"
    assert result["expected_counts"] == {
        "raw_hands": 1,
        "raw_players": 2,
        "raw_actions": 2,
        "features": 2,
        "rule_flags": 2,
    }
    assert result["legacy_alerts"]["rows"] == 1

    frames["raw_hands"].loc[0, "pot_size"] = 99.0
    failed = compare_legacy_inputs([_payload()], **frames)
    assert failed["status"] == "failed"
    assert failed["input_identity"] == "failed"


def test_expected_legacy_counts_follow_payload_cardinality() -> None:
    assert expected_legacy_counts([_payload()]) == {
        "raw_hands": 1,
        "raw_players": 2,
        "raw_actions": 2,
        "features": 2,
        "rule_flags": 2,
    }


def test_r6_replay_manifest_is_exactly_bounded() -> None:
    value = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "legacy_replay",
        "status": "published",
        "training_allowed": False,
        "dataset_id": "r6-dataset",
        "target_hand_ids": ["r6-hand-1"],
        "topic": LEGACY_HANDS_TOPIC,
        "consumer_group": LEGACY_GROUP_ID,
        "records": [{"hand_id": "r6-hand-1", "partition": 1, "offset": 42}],
        "source_hashes": {"d7_manifest": "a" * 64},
        "expected_legacy_counts": expected_legacy_counts([_payload()]),
        "source_commit": "b" * 40,
        "started_at": "2026-07-24T10:00:00Z",
        "completed_at": "2026-07-24T10:00:01Z",
    }

    assert validate_replay_manifest(value) == value
    value["topic"] = "poker.synthetic.hands.raw.v1"
    with pytest.raises(ValueError, match="invalid R6"):
        validate_replay_manifest(value)


def test_dependency_audit_allows_only_the_legacy_service_and_group() -> None:
    realtime_spec = """
    env:
      KAFKA_HANDS_TOPIC: hands.raw
      KAFKA_ALERTS_TOPIC: alerts.out
    """
    canonical_spec = """
    env:
      KAFKA_HANDS_TOPIC: poker.synthetic.hands.raw.v1
    """
    passed = build_dependency_audit(
        service_specs={
            LEGACY_SERVICE: realtime_spec,
            "POKER_SINK": canonical_spec,
        },
        kafka_groups=[
            {
                "group_id": LEGACY_GROUP_ID,
                "state": "Stable",
                "members": 1,
                "topics": [LEGACY_HANDS_TOPIC],
            },
            {
                "group_id": "old-empty-test",
                "state": "Empty",
                "members": 0,
                "topics": [LEGACY_HANDS_TOPIC],
            },
        ],
    )

    assert passed["status"] == "passed"
    assert passed["service_dependencies"] == {
        LEGACY_SERVICE: [LEGACY_ALERTS_TOPIC, LEGACY_HANDS_TOPIC]
    }

    failed = build_dependency_audit(
        service_specs={
            LEGACY_SERVICE: realtime_spec,
            "UNEXPECTED": realtime_spec,
        },
        kafka_groups=[
            {
                "group_id": "unexpected-live",
                "state": "Stable",
                "members": 1,
                "topics": [LEGACY_HANDS_TOPIC],
            }
        ],
        local_processes=[
            "/workspace/snowflake-poker-ml-pipeline/scripts/generate.py"
        ],
    )
    assert failed["status"] == "failed"
    assert len(failed["blockers"]) == 3


def test_output_comparison_records_non_equivalent_model_contracts() -> None:
    result = output_contract_comparison(
        legacy_alert_rows=6,
        legacy_alert_hands=3,
        canonical_scores=16,
        canonical_decisions=16,
        canonical_alerts=14,
    )

    assert result["comparison_mode"] == "replacement_contract"
    assert result["numeric_score_equality"] == "not_applicable"
    assert result["legacy"]["full_score_rows_persisted"] is False
    assert result["canonical"]["full_score_rows_persisted"] is True


def test_suspension_chain_and_start_report_are_hash_bound(
    tmp_path: Path,
) -> None:
    manifest = {
        "dataset_id": "r6-dataset",
        "topics": {"hands": "poker.synthetic.hands.raw.v1"},
    }
    preflight = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "preflight",
        "status": "passed",
        "source_commit": "a" * 40,
    }
    parity = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "bounded_parity",
        "status": "passed",
        "dataset_id": "r6-dataset",
        "source_commit": "a" * 40,
    }
    manifest_path = tmp_path / "manifest.json"
    preflight_path = tmp_path / "preflight.json"
    parity_path = tmp_path / "parity.json"
    manifest_path.write_text(json.dumps(manifest))
    preflight_path.write_text(json.dumps(preflight))
    parity_path.write_text(json.dumps(parity))

    assert _load_chain(
        preflight_path,
        parity_path,
        manifest_path,
    ) == (preflight, parity, manifest)

    start_report = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "suspension_start",
        "status": "observation_started",
        "dataset_id": "r6-dataset",
        "minimum_end_at": "2026-07-25T10:00:00Z",
        "source_reports": {
            "preflight": {
                "path": str(preflight_path.resolve()),
                "sha256": hashlib.sha256(
                    preflight_path.read_bytes()
                ).hexdigest(),
            },
            "parity": {
                "path": str(parity_path.resolve()),
                "sha256": hashlib.sha256(
                    parity_path.read_bytes()
                ).hexdigest(),
            },
            "d7_manifest": {
                "path": str(manifest_path.resolve()),
                "sha256": hashlib.sha256(
                    manifest_path.read_bytes()
                ).hexdigest(),
            }
        },
    }
    start_path = tmp_path / "start.json"
    start_path.write_text(json.dumps(start_report))
    assert _validate_start_report(start_path) == (start_report, manifest)

    completed_check = {
        "schema_version": 1,
        "run_type": R6_RUN_TYPE,
        "phase": "suspension_check",
        "status": "observation_window_complete",
        "start_report": {
            "sha256": hashlib.sha256(start_path.read_bytes()).hexdigest()
        },
    }
    completed_path = tmp_path / "completed.json"
    completed_path.write_text(json.dumps(completed_check))
    assert _validate_completed_check(
        completed_path,
        start_path,
    ) == completed_check

    manifest["dataset_id"] = "changed"
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="d7_manifest"):
        _validate_start_report(start_path)


def test_suspension_health_helpers_require_ready_and_monotonic_offsets() -> None:
    assert _ready(
        {
            "status": "RUNNING",
            "containers": [{"status": "READY"}],
        },
        1,
    )
    assert not _ready(
        {
            "status": "RUNNING",
            "containers": [{"status": "PENDING"}],
        },
        1,
    )
    expected = {
        "offsets": [
            {"topic": LEGACY_HANDS_TOPIC, "partition": 0, "committed": 99}
        ]
    }
    assert _offsets_cover(
        {
            "offsets": [
                {
                    "topic": LEGACY_HANDS_TOPIC,
                    "partition": 0,
                    "committed": 100,
                }
            ]
        },
        expected,
    )
    assert not _offsets_cover({"offsets": []}, expected)


def test_r6_operational_entrypoints_are_packaged() -> None:
    root = Path(__file__).resolve().parent.parent
    makefile = (root / "Makefile").read_text()
    for target in (
        "r6-preflight:",
        "r6-legacy-replay:",
        "r6-parity-verify:",
        "r6-bounded-e2e:",
        "r6-suspension-start:",
        "r6-suspension-check:",
        "r6-suspension-final-check:",
        "r6-rollback:",
        "phase-r6-check:",
    ):
        assert target in makefile
    for relative in (
        "scripts/audit_r6_realtime_dependencies.py",
        "scripts/replay_r6_legacy.py",
        "scripts/verify_r6_realtime_parity.py",
        "scripts/manage_r6_realtime_suspension.py",
        "docs/poker-realtime-retirement.md",
    ):
        assert (root / relative).is_file()
