from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from itertools import combinations
from pathlib import Path

import pytest

from pipeline.config import Settings
from pipeline.events import (
    PairRiskScore,
    PlayerRiskScore,
    RiskScoreEvent,
    RiskScorePayload,
)
from pipeline.warehouse.duckdb import DuckDBWarehouse
from pipeline.warehouse.migrate import run_migrations
from pipeline.warehouse.rule_evidence import (
    RuleEvidenceIngestRecord,
    load_risk_score_rule_references,
    load_rule_evidence_events,
)


NOW = datetime(2026, 7, 21, 10, 15, tzinfo=timezone.utc)
RULE_EVENT_ID = uuid.UUID("8bcfb4e4-2113-52c3-85c2-a6ca4cb19823")


def _warehouse(tmp_path: Path) -> DuckDBWarehouse:
    warehouse = DuckDBWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="duckdb",
            DUCKDB_PATH=tmp_path / "rule-evidence.duckdb",
        )
    )
    run_migrations(warehouse)
    return warehouse


def _score(rule_ids: list[uuid.UUID]) -> RiskScoreEvent:
    players = ["a", "b", "c", "d", "e", "f"]
    pair_scores = [
        PairRiskScore(
            feature_event_id=uuid.uuid5(uuid.NAMESPACE_URL, pair_key),
            pair_key=pair_key,
            player_a=left,
            player_b=right,
            snapshot_revision=1,
            raw_probability=0.9 if pair_key == "a:b" else 0.1,
            calibrated_probability=0.9 if pair_key == "a:b" else 0.1,
            alert=pair_key == "a:b",
        )
        for left, right in combinations(players, 2)
        for pair_key in [f"{left}:{right}"]
    ]
    player_scores = [
        PlayerRiskScore(
            player_id=player,
            risk_probability=0.9 if player in {"a", "b"} else 0.1,
            alert=player in {"a", "b"},
        )
        for player in players
    ]
    return RiskScoreEvent(
        event_id=uuid.UUID("22222222-2222-5222-8222-222222222222"),
        tenant_id="tenant-a",
        product_id="poker",
        dataset_id="context-full-v2",
        dataset_split="test",
        occurred_at=NOW,
        emitted_at=NOW,
        trace_id=uuid.UUID("11111111-1111-5111-8111-111111111111"),
        payload=RiskScorePayload(
            score_id="0123456789abcdef0123456789abcdef",
            hand_id="H-000042",
            table_id="table-1",
            played_at=NOW,
            model_name="pair-catboost-v1",
            model_run_id="pair_7a1c58c1046b",
            decision_threshold=0.8,
            scored_at=NOW,
            rule_evidence_event_ids=rule_ids,
            pair_scores=pair_scores,
            player_scores=player_scores,
            hand_risk_probability=0.9,
            alert=True,
        ),
    )


def test_rule_evidence_and_model_lineage_are_idempotent(tmp_path: Path) -> None:
    warehouse = _warehouse(tmp_path)
    fixture = json.loads(
        Path("schemas/examples/poker.rule-evidence.v1.json").read_text()
    )
    record = RuleEvidenceIngestRecord(
        envelope=fixture,
        topic="poker.rule-evidence.v1",
        partition=2,
        offset=17,
        kafka_timestamp_ms=1_785_147_300_250,
    )

    first = load_rule_evidence_events(warehouse, [record])
    second = load_rule_evidence_events(warehouse, [record])
    first_refs = load_risk_score_rule_references(warehouse, [_score([RULE_EVENT_ID])])
    second_refs = load_risk_score_rule_references(warehouse, [_score([RULE_EVENT_ID])])

    assert first == second
    assert first.events == first.hands == first.entities == 1
    assert first_refs == second_refs
    assert first_refs.references == first_refs.scores == first_refs.model_runs == 1
    rows = warehouse.fetch_df(
        "SELECT tenant_id, model_run_id, risk_score_event_id, rule_id, hand_id, "
        "observation_revision "
        "FROM RULE_EVIDENCE_WITH_MODEL_LINEAGE"
    ).to_dict("records")
    assert rows == [
        {
            "tenant_id": "tenant-a",
            "model_run_id": "pair_7a1c58c1046b",
            "risk_score_event_id": "22222222-2222-5222-8222-222222222222",
            "rule_id": "pair.fold-benefit",
            "hand_id": "H-000042",
            "observation_revision": 1,
        }
    ]
    warehouse.close()


def test_rule_event_collision_and_removed_references_are_handled(
    tmp_path: Path,
) -> None:
    warehouse = _warehouse(tmp_path)
    fixture = json.loads(
        Path("schemas/examples/poker.rule-evidence.v1.json").read_text()
    )
    changed = json.loads(json.dumps(fixture))
    changed["payload"]["raw_score"] = 73.0
    load_rule_evidence_events(warehouse, [RuleEvidenceIngestRecord(envelope=fixture)])
    with pytest.raises(ValueError, match="collision with persisted payload"):
        load_rule_evidence_events(
            warehouse, [RuleEvidenceIngestRecord(envelope=changed)]
        )

    load_risk_score_rule_references(warehouse, [_score([RULE_EVENT_ID])])
    replacement = uuid.UUID("33333333-3333-5333-8333-333333333333")
    replaced = load_risk_score_rule_references(warehouse, [_score([replacement])])
    assert replaced.references == 1
    assert warehouse.fetch_df("SELECT rule_event_id FROM RISK_SCORE_RULE_EVIDENCE")[
        "rule_event_id"
    ].tolist() == [str(replacement)]
    cleared = load_risk_score_rule_references(warehouse, [_score([])])
    assert cleared.references == 0
    assert warehouse.fetch_df("SELECT * FROM RISK_SCORE_RULE_EVIDENCE").empty
    warehouse.close()
