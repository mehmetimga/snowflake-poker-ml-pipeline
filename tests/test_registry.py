from __future__ import annotations

import json
from pathlib import Path

import pytest

from pipeline.ops.candidate import (
    build_candidate_evidence,
    sha256,
    write_candidate_evidence,
)
from pipeline.ops.registry import build_registry, validate_registry


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _candidate(
    root: Path,
    *,
    model_name: str,
    run_id: str,
    model_family: str,
    quality_passed: bool,
    requested_stage: str,
    operational_run_id: str | None = None,
) -> tuple[dict[str, object], Path, Path]:
    dataset_manifest = root / "dataset" / "manifest.json"
    if not dataset_manifest.is_file():
        _write_json(
            dataset_manifest,
            {
                "dataset_id": "dataset-v1",
                "feature_definition_version": "features-v1",
            },
        )
    artifact = root / model_name
    artifact.mkdir(parents=True)
    metrics = {
        "model_name": model_name,
        "run_id": run_id,
        "dataset_id": "dataset-v1",
        "dataset_manifest_sha256": sha256(dataset_manifest),
        "feature_definition_version": "features-v1",
        "benchmark": "cold_start",
        "evaluation": {"test": {"pr_auc": 0.4}},
        "quality_gate": {"promotion_eligible": quality_passed},
    }
    _write_json(artifact / "metrics.json", metrics)
    (artifact / "predictions.parquet").write_bytes(b"immutable-predictions")
    _write_json(
        artifact / "scoring_contract.json",
        {
            "contract_version": 1,
            "model_name": model_name,
            "run_id": run_id,
            "feature_definition_version": "features-v1",
            "batching": {"triton_model": "fixture_model"},
        },
    )
    _write_json(
        artifact / "decision_policy.json",
        {"policy_version": 1, "threshold": 0.75},
    )
    artifacts = {
        name: sha256(artifact / name)
        for name in (
            "metrics.json",
            "predictions.parquet",
            "scoring_contract.json",
            "decision_policy.json",
        )
    }
    _write_json(
        artifact / "artifact_manifest.json",
        {"model_name": model_name, "run_id": run_id, "artifacts": artifacts},
    )
    operational = root / "operational" / f"{run_id}.json"
    operational_arg: Path | None = None
    if requested_stage == "production" or operational_run_id is not None:
        _write_json(
            operational,
            {
                "model_name": model_name,
                "model_run_id": operational_run_id or run_id,
                "artifact_manifest_sha256": sha256(
                    artifact / "artifact_manifest.json"
                ),
                "passed": True,
            },
        )
        operational_arg = operational
    evidence = build_candidate_evidence(
        artifact,
        dataset_manifest,
        model_family=model_family,
        tenant_id="tenant-a",
        product_id="poker",
        benchmark="cold_start",
        metrics_path="metrics.json",
        predictions_path="predictions.parquet",
        metric_name="pr_auc",
        metric_json_path=("evaluation", "test", "pr_auc"),
        quality_gate_json_path=("quality_gate", "promotion_eligible"),
        requested_stage=requested_stage,
        private_challenge="passed" if requested_stage == "production" else "not_run",
        manual_approval="approved" if requested_stage == "production" else "not_requested",
        reasons=() if quality_passed else ("public quality floor not met",),
        scoring_contract_path=(
            "scoring_contract.json" if requested_stage == "production" else None
        ),
        decision_policy_path=(
            "decision_policy.json" if requested_stage == "production" else None
        ),
        operational_report_path=operational_arg,
    )
    evidence_path = root / "inputs" / f"{model_name}.json"
    write_candidate_evidence(evidence_path, evidence)
    return evidence, evidence_path, artifact


def test_registry_accepts_generic_model_families_and_preserves_governance(
    tmp_path: Path,
) -> None:
    _champion, champion_path, _ = _candidate(
        tmp_path,
        model_name="tabular-model",
        run_id="tabular-run",
        model_family="tabular_catboost",
        quality_passed=True,
        requested_stage="production",
    )
    _challenger, challenger_path, _ = _candidate(
        tmp_path,
        model_name="sequence-model",
        run_id="sequence-run",
        model_family="sequence_transformer",
        quality_passed=False,
        requested_stage="candidate",
    )
    registry_dir = tmp_path / "registry"
    registry = build_registry((champion_path, challenger_path), registry_dir)

    assert registry["schema_version"] == 2
    active = [entry for entry in registry["entries"] if entry["stage"] == "production"]
    assert len(active) == 1
    assert active[0]["model_family"] == "tabular_catboost"
    rejected = next(entry for entry in registry["entries"] if entry["stage"] == "rejected")
    assert rejected["model_family"] == "sequence_transformer"
    assert rejected["status"] == "public_gate_failed"
    assert registry["promotion_policy"]["automatic_production_promotion"] is False
    assert registry["promotion_policy"]["rollback_required"] is True

    deployment = json.loads((registry_dir / "deployment.json").read_text())
    assert deployment["rollback"]["required"] is True
    assert deployment["rollback"]["model_run_id"] == "tabular-run"
    assert validate_registry(registry_dir)["active"]["run_id"] == "tabular-run"


def test_candidate_requires_predictions_tracked_by_artifact_manifest(
    tmp_path: Path,
) -> None:
    _evidence, _path, artifact = _candidate(
        tmp_path,
        model_name="graph-model",
        run_id="graph-run",
        model_family="graph_gnn",
        quality_passed=False,
        requested_stage="candidate",
    )
    manifest_path = artifact / "artifact_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    del manifest["artifacts"]["predictions.parquet"]
    _write_json(manifest_path, manifest)

    with pytest.raises(ValueError, match="required evidence is not tracked"):
        build_candidate_evidence(
            artifact,
            tmp_path / "dataset" / "manifest.json",
            model_family="graph_gnn",
            tenant_id="tenant-a",
            product_id="poker",
            benchmark="cold_start",
            metrics_path="metrics.json",
            predictions_path="predictions.parquet",
            metric_name="pr_auc",
            metric_json_path=("evaluation", "test", "pr_auc"),
            quality_gate_json_path=("quality_gate", "promotion_eligible"),
            requested_stage="candidate",
            private_challenge="not_run",
            manual_approval="not_requested",
        )


def test_candidate_rejects_operational_report_from_another_run(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="operational report belongs to another run"):
        _candidate(
            tmp_path,
            model_name="hybrid-model",
            run_id="hybrid-run",
            model_family="hybrid_stack",
            quality_passed=True,
            requested_stage="production",
            operational_run_id="different-run",
        )


def test_registry_rejects_two_active_models_in_one_scope(tmp_path: Path) -> None:
    _first, first_path, _ = _candidate(
        tmp_path,
        model_name="first-model",
        run_id="first-run",
        model_family="tabular_catboost",
        quality_passed=True,
        requested_stage="production",
    )
    _second, second_path, _ = _candidate(
        tmp_path,
        model_name="second-model",
        run_id="second-run",
        model_family="graph_gnn",
        quality_passed=True,
        requested_stage="production",
    )
    with pytest.raises(ValueError, match="exactly one active production"):
        build_registry((first_path, second_path), tmp_path / "registry")


def test_registry_detects_candidate_evidence_mutation(tmp_path: Path) -> None:
    _champion, champion_path, _ = _candidate(
        tmp_path,
        model_name="champion-model",
        run_id="champion-run",
        model_family="tabular_catboost",
        quality_passed=True,
        requested_stage="production",
    )
    registry_dir = tmp_path / "registry"
    registry = build_registry((champion_path,), registry_dir)
    registered_path = registry_dir / registry["entries"][0]["candidate_evidence_path"]
    registered = json.loads(registered_path.read_text())
    registered["candidate"]["model_family"] = "tampered-family"
    _write_json(registered_path, registered)

    with pytest.raises(ValueError, match="candidate evidence changed"):
        validate_registry(registry_dir)


def test_candidate_schema_declares_required_hash_bound_evidence() -> None:
    schema = json.loads(Path("schemas/model_candidate_evidence.schema.json").read_text())
    assert schema["properties"]["schema_version"]["const"] == 1
    assert set(schema["properties"]["evidence"]["required"]) == {
        "metrics",
        "predictions",
    }
    assert schema["properties"]["governance"]["properties"][
        "rollback_required"
    ]["const"] is True
