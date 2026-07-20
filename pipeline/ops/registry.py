"""File-backed model registry and deployment promotion gates."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_artifact_bundle(root: Path) -> dict[str, Any]:
    manifest_path = root / "artifact_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for relative, expected in manifest["artifacts"].items():
        path = root / relative
        if not path.is_file():
            raise FileNotFoundError(path)
        if sha256(path) != expected:
            raise ValueError(f"artifact hash mismatch: {root.name}/{relative}")
    return manifest


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def build_registry(
    champion_dir: Path,
    ensemble_dir: Path,
    output_dir: Path,
    *,
    operational_report: Path | None = None,
) -> dict[str, Any]:
    champion_dir, ensemble_dir, output_dir = (
        champion_dir.resolve(), ensemble_dir.resolve(), output_dir.resolve()
    )
    champion_manifest = verify_artifact_bundle(champion_dir)
    ensemble_manifest = verify_artifact_bundle(ensemble_dir)
    champion_metrics = json.loads((champion_dir / "metrics.json").read_text())
    ensemble_metrics = json.loads((ensemble_dir / "metrics.json").read_text())
    champion_contract = json.loads((champion_dir / "scoring_contract.json").read_text())
    champion_policy = json.loads((champion_dir / "decision_policy.json").read_text())
    if len({champion_manifest["run_id"], champion_metrics["run_id"], champion_contract["run_id"]}) != 1:
        raise ValueError("champion run identity is inconsistent")
    if ensemble_manifest["run_id"] != ensemble_metrics["run_id"]:
        raise ValueError("ensemble run identity is inconsistent")
    now = datetime.now(tz=timezone.utc).isoformat()
    candidate = bool(ensemble_metrics["quality_gate"]["promotion_candidate"])
    operational: dict[str, Any] | None = None
    if operational_report is not None and operational_report.is_file():
        operational = json.loads(operational_report.read_text())
    operational_bound = bool(
        operational
        and operational.get("passed")
        and operational.get("model_name") == champion_metrics["model_name"]
        and operational.get("model_run_id") == champion_metrics["run_id"]
        and operational.get("artifact_manifest_sha256")
        == sha256(champion_dir / "artifact_manifest.json")
    )
    entries = [
        {
            "model_name": champion_metrics["model_name"],
            "run_id": champion_metrics["run_id"],
            "stage": "production",
            "status": "active",
            "artifact_uri": str(champion_dir),
            "artifact_manifest_sha256": sha256(champion_dir / "artifact_manifest.json"),
            "dataset_id": champion_metrics["dataset_id"],
            "feature_definition_version": champion_metrics["feature_definition_version"],
            "benchmark": champion_metrics["benchmark"],
            "test_pr_auc": champion_metrics["reports"]["catboost"]["test"]["pr_auc"],
            "promotion_gates": {
                "artifact_integrity": "passed",
                "public_quality": "passed",
                "private_challenge": "passed",
                "manual_approval": "grandfathered_initial_champion",
                "operational_verification": "passed" if operational_bound else "pending",
            },
        },
        {
            "model_name": ensemble_metrics["model_name"],
            "run_id": ensemble_metrics["run_id"],
            "stage": "candidate" if candidate else "rejected",
            "status": "awaiting_private_challenge_and_manual_approval" if candidate else "public_gate_failed",
            "artifact_uri": str(ensemble_dir),
            "artifact_manifest_sha256": sha256(ensemble_dir / "artifact_manifest.json"),
            "dataset_id": ensemble_metrics["dataset_id"],
            "feature_definition_version": ensemble_metrics["feature_definition_version"],
            "benchmark": ensemble_metrics["benchmark"],
            "test_pr_auc": ensemble_metrics["reports"]["test"]["pr_auc"],
            "promotion_gates": {
                "artifact_integrity": "passed",
                "public_quality": "passed" if candidate else "failed",
                "private_challenge": "pending" if candidate else "not_run",
                "manual_approval": "pending" if candidate else "not_requested",
                "operational_verification": "not_run",
            },
            "reasons": ensemble_metrics["quality_gate"]["reasons"],
        },
    ]
    output_dir.mkdir(parents=True, exist_ok=True)
    registry = {
        "schema_version": 1,
        "generated_at": now,
        "scope": {"tenant_id": "*", "product_id": "poker", "benchmark": "cold_start"},
        "promotion_policy": {
            "required_gates": [
                "artifact_integrity", "public_quality", "private_challenge",
                "manual_approval", "operational_verification",
            ],
            "automatic_production_promotion": False,
            "rollback_required": True,
        },
        "entries": entries,
    }
    deployment = {
        "schema_version": 1,
        "deployment_id": "poker-risk-production",
        "tenant_id": "*",
        "product_id": "poker",
        "environment": "production",
        "state": "active",
        "model_name": champion_metrics["model_name"],
        "model_run_id": champion_metrics["run_id"],
        "artifact_manifest_sha256": sha256(champion_dir / "artifact_manifest.json"),
        "feature_definition_version": champion_metrics["feature_definition_version"],
        "decision_policy_version": champion_policy["policy_version"],
        "decision_threshold": champion_policy["threshold"],
        "scoring_contract_version": champion_contract["contract_version"],
        "triton_model": champion_contract["batching"]["triton_model"],
        "rollback": {
            "model_name": champion_metrics["model_name"],
            "model_run_id": champion_metrics["run_id"],
            "artifact_uri": str(champion_dir),
        },
        "updated_at": now,
    }
    audit = [
        {
            "event_type": "model.registered",
            "occurred_at": now,
            "actor_type": "phase12_pipeline",
            "tenant_id": "*",
            "product_id": "poker",
            "model_name": entry["model_name"],
            "model_run_id": entry["run_id"],
            "result": entry["stage"],
        }
        for entry in entries
    ]
    audit.append(
        {
            "event_type": "deployment.snapshot_recorded",
            "occurred_at": now,
            "actor_type": "phase12_pipeline",
            "tenant_id": "*",
            "product_id": "poker",
            "model_name": deployment["model_name"],
            "model_run_id": deployment["model_run_id"],
            "result": "active",
        }
    )
    _write_json(output_dir / "registry.json", registry)
    _write_json(output_dir / "deployment.json", deployment)
    (output_dir / "audit_log.jsonl").write_text(
        "".join(json.dumps(event, sort_keys=True) + "\n" for event in audit)
    )
    return registry


def validate_registry(registry_dir: Path) -> dict[str, Any]:
    registry = json.loads((registry_dir / "registry.json").read_text())
    deployment = json.loads((registry_dir / "deployment.json").read_text())
    active = [
        entry for entry in registry["entries"]
        if entry["stage"] == "production" and entry["status"] == "active"
    ]
    if len(active) != 1:
        raise ValueError("registry must contain exactly one active production model per scope")
    if active[0]["run_id"] != deployment["model_run_id"]:
        raise ValueError("deployment does not point to the active registry entry")
    if active[0]["artifact_manifest_sha256"] != deployment["artifact_manifest_sha256"]:
        raise ValueError("deployment artifact hash does not match registry")
    artifact_dir = Path(active[0]["artifact_uri"])
    if sha256(artifact_dir / "artifact_manifest.json") != deployment["artifact_manifest_sha256"]:
        raise ValueError("deployed artifact manifest changed after registration")
    audit_lines = [
        json.loads(line) for line in (registry_dir / "audit_log.jsonl").read_text().splitlines()
        if line.strip()
    ]
    if not audit_lines or not all(event.get("tenant_id") and event.get("product_id") for event in audit_lines):
        raise ValueError("registry audit trail is missing tenant/product identity")
    return {"active": active[0], "audit_events": len(audit_lines)}
