"""File-backed model registry built from generic candidate evidence."""

from __future__ import annotations

import json
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from pipeline.ops.candidate import (
    load_candidate_evidence,
    sha256,
    validate_candidate_evidence,
    verify_artifact_bundle,
)


REGISTRY_SCHEMA_VERSION = 2


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _scope_key(scope: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(scope.get("tenant_id", "")),
        str(scope.get("product_id", "")),
        str(scope.get("benchmark", "")),
    )


def _safe_evidence_name(candidate: Mapping[str, Any]) -> str:
    raw = f"{candidate['model_name']}--{candidate['run_id']}"
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", raw) + ".json"


def _operational_status(evidence: Mapping[str, Any]) -> str:
    reference = evidence["runtime"].get("operational_report")
    if reference is None:
        return "not_run"
    report = _load_json(Path(reference["uri"]))
    return "passed" if report.get("passed") is True else "failed"


def _stage_and_status(evidence: Mapping[str, Any]) -> tuple[str, str]:
    governance = evidence["governance"]
    if governance["requested_stage"] == "production":
        return "production", "active"
    if evidence["evaluation"]["quality_gate"] == "failed":
        return "rejected", "public_gate_failed"
    if (
        governance["private_challenge"] == "passed"
        and governance["manual_approval"] in {"approved", "grandfathered_initial_champion"}
        and _operational_status(evidence) == "passed"
    ):
        return "candidate", "awaiting_promotion"
    return "candidate", "awaiting_private_challenge_and_manual_approval"


def _entry_from_evidence(
    evidence: Mapping[str, Any], evidence_path: str, evidence_hash: str
) -> dict[str, Any]:
    stage, status = _stage_and_status(evidence)
    candidate = evidence["candidate"]
    data = evidence["data"]
    evaluation = evidence["evaluation"]
    governance = evidence["governance"]
    operational = _operational_status(evidence)
    return {
        "model_name": candidate["model_name"],
        "run_id": candidate["run_id"],
        "model_family": candidate["model_family"],
        "scope": dict(evidence["scope"]),
        "stage": stage,
        "status": status,
        "artifact_uri": evidence["artifact"]["uri"],
        "artifact_manifest_sha256": evidence["artifact"]["manifest_sha256"],
        "candidate_evidence_path": evidence_path,
        "candidate_evidence_sha256": evidence_hash,
        "dataset_id": data["dataset_id"],
        "dataset_manifest_sha256": data["dataset_manifest_sha256"],
        "feature_definition_version": data["feature_definition_version"],
        "benchmark": evaluation["benchmark"],
        "evaluation_metric": {
            "split": evaluation["split"],
            "name": evaluation["metric_name"],
            "value": evaluation["metric_value"],
        },
        # Compatibility field for current model-card/admin consumers.
        "test_pr_auc": evaluation["metric_value"]
        if evaluation["metric_name"] == "pr_auc" and evaluation["split"] == "test"
        else None,
        "promotion_gates": {
            "artifact_integrity": "passed",
            "public_quality": evaluation["quality_gate"],
            "private_challenge": governance["private_challenge"],
            "manual_approval": governance["manual_approval"],
            "operational_verification": operational,
        },
        "reasons": list(evaluation["reasons"]),
    }


def build_registry(
    candidate_evidence_paths: Sequence[Path],
    output_dir: Path,
    *,
    deployment_id: str = "poker-risk-production",
) -> dict[str, Any]:
    """Register model-family-neutral candidates and snapshot one active deployment."""

    if not candidate_evidence_paths:
        raise ValueError("at least one candidate evidence document is required")
    output_dir = output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    candidate_dir = output_dir / "candidates"
    candidate_dir.mkdir(parents=True, exist_ok=True)

    loaded: list[tuple[dict[str, Any], Path]] = []
    identities: set[tuple[str, str]] = set()
    for source in candidate_evidence_paths:
        source = source.resolve()
        evidence = load_candidate_evidence(source)
        identity = (
            evidence["candidate"]["model_name"], evidence["candidate"]["run_id"]
        )
        if identity in identities:
            raise ValueError(f"duplicate candidate identity: {identity[0]}:{identity[1]}")
        identities.add(identity)
        target = candidate_dir / _safe_evidence_name(evidence["candidate"])
        if source != target.resolve():
            shutil.copyfile(source, target)
        loaded.append((evidence, target))

    scopes = {_scope_key(evidence["scope"]) for evidence, _ in loaded}
    if len(scopes) != 1:
        raise ValueError("one registry snapshot may contain exactly one tenant/product/benchmark scope")
    production = [
        evidence
        for evidence, _ in loaded
        if evidence["governance"]["requested_stage"] == "production"
    ]
    if len(production) != 1:
        raise ValueError("registry must contain exactly one active production model per scope")

    entries = [
        _entry_from_evidence(
            evidence,
            str(target.relative_to(output_dir)),
            sha256(target),
        )
        for evidence, target in loaded
    ]
    active_evidence = production[0]
    active_entry = next(
        entry
        for entry in entries
        if entry["model_name"] == active_evidence["candidate"]["model_name"]
        and entry["run_id"] == active_evidence["candidate"]["run_id"]
    )
    contract_ref = active_evidence["runtime"]["scoring_contract"]
    policy_ref = active_evidence["runtime"]["decision_policy"]
    artifact_dir = Path(active_evidence["artifact"]["uri"])
    scoring_contract = _load_json(artifact_dir / contract_ref["path"])
    decision_policy = _load_json(artifact_dir / policy_ref["path"])
    if scoring_contract.get("model_name") != active_entry["model_name"]:
        raise ValueError("production scoring contract belongs to another model")
    if scoring_contract.get("run_id") != active_entry["run_id"]:
        raise ValueError("production scoring contract belongs to another run")
    if scoring_contract.get("feature_definition_version") != active_entry[
        "feature_definition_version"
    ]:
        raise ValueError("production scoring contract feature version mismatch")

    now = datetime.now(tz=timezone.utc).isoformat()
    scope = dict(active_evidence["scope"])
    registry = {
        "schema_version": REGISTRY_SCHEMA_VERSION,
        "generated_at": now,
        "scope": scope,
        "promotion_policy": {
            "required_gates": [
                "artifact_integrity",
                "public_quality",
                "private_challenge",
                "manual_approval",
                "operational_verification",
            ],
            "automatic_production_promotion": False,
            "rollback_required": True,
        },
        "entries": entries,
    }
    batching = scoring_contract.get("batching", {})
    deployment = {
        "schema_version": 2,
        "deployment_id": deployment_id,
        "tenant_id": scope["tenant_id"],
        "product_id": scope["product_id"],
        "benchmark": scope["benchmark"],
        "environment": "production",
        "state": "active",
        "model_name": active_entry["model_name"],
        "model_run_id": active_entry["run_id"],
        "model_family": active_entry["model_family"],
        "artifact_manifest_sha256": active_entry["artifact_manifest_sha256"],
        "candidate_evidence_sha256": active_entry["candidate_evidence_sha256"],
        "feature_definition_version": active_entry["feature_definition_version"],
        "decision_policy_version": decision_policy["policy_version"],
        "decision_threshold": decision_policy["threshold"],
        "scoring_contract_version": scoring_contract["contract_version"],
        "triton_model": batching.get("triton_model"),
        "rollback": {
            "required": True,
            "model_name": active_entry["model_name"],
            "model_run_id": active_entry["run_id"],
            "artifact_uri": active_entry["artifact_uri"],
            "artifact_manifest_sha256": active_entry["artifact_manifest_sha256"],
        },
        "updated_at": now,
    }
    audit = [
        {
            "event_type": "model.registered",
            "occurred_at": now,
            "actor_type": "phase12_pipeline",
            "tenant_id": scope["tenant_id"],
            "product_id": scope["product_id"],
            "benchmark": scope["benchmark"],
            "model_name": entry["model_name"],
            "model_run_id": entry["run_id"],
            "candidate_evidence_sha256": entry["candidate_evidence_sha256"],
            "result": entry["stage"],
        }
        for entry in entries
    ]
    audit.append(
        {
            "event_type": "deployment.snapshot_recorded",
            "occurred_at": now,
            "actor_type": "phase12_pipeline",
            "tenant_id": scope["tenant_id"],
            "product_id": scope["product_id"],
            "benchmark": scope["benchmark"],
            "model_name": deployment["model_name"],
            "model_run_id": deployment["model_run_id"],
            "candidate_evidence_sha256": deployment["candidate_evidence_sha256"],
            "result": "active",
        }
    )
    _write_json(output_dir / "registry.json", registry)
    _write_json(output_dir / "deployment.json", deployment)
    (output_dir / "audit_log.jsonl").write_text(
        "".join(json.dumps(event, sort_keys=True) + "\n" for event in audit)
    )
    validate_registry(output_dir)
    return registry


def _validate_legacy_registry(
    registry_dir: Path,
    registry: Mapping[str, Any],
    deployment: Mapping[str, Any],
) -> dict[str, Any]:
    """Read schema v1 snapshots while all newly built snapshots use schema v2."""

    active = [
        entry
        for entry in registry["entries"]
        if entry["stage"] == "production" and entry["status"] == "active"
    ]
    if len(active) != 1:
        raise ValueError("registry must contain exactly one active production model per scope")
    if active[0]["run_id"] != deployment["model_run_id"]:
        raise ValueError("deployment does not point to the active registry entry")
    if active[0]["artifact_manifest_sha256"] != deployment["artifact_manifest_sha256"]:
        raise ValueError("deployment artifact hash does not match registry")
    artifact_dir = Path(active[0]["artifact_uri"])
    if sha256(artifact_dir / "artifact_manifest.json") != deployment[
        "artifact_manifest_sha256"
    ]:
        raise ValueError("deployed artifact manifest changed after registration")
    audit_lines = _audit_lines(registry_dir)
    return {"active": active[0], "audit_events": len(audit_lines)}


def _audit_lines(registry_dir: Path) -> list[dict[str, Any]]:
    lines = [
        json.loads(line)
        for line in (registry_dir / "audit_log.jsonl").read_text().splitlines()
        if line.strip()
    ]
    if not lines or not all(
        event.get("tenant_id") and event.get("product_id") for event in lines
    ):
        raise ValueError("registry audit trail is missing tenant/product identity")
    return lines


def validate_registry(registry_dir: Path) -> dict[str, Any]:
    registry_dir = registry_dir.resolve()
    registry = _load_json(registry_dir / "registry.json")
    deployment = _load_json(registry_dir / "deployment.json")
    if registry.get("schema_version") == 1:
        return _validate_legacy_registry(registry_dir, registry, deployment)
    if registry.get("schema_version") != REGISTRY_SCHEMA_VERSION:
        raise ValueError("unsupported model registry schema version")
    policy = registry.get("promotion_policy", {})
    if policy.get("automatic_production_promotion") is not False:
        raise ValueError("automatic production promotion must remain disabled")
    if policy.get("rollback_required") is not True:
        raise ValueError("registry promotion policy must require rollback")
    scope = registry.get("scope")
    if not isinstance(scope, Mapping) or any(not value for value in _scope_key(scope)):
        raise ValueError("registry scope is incomplete")

    entries = registry.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("registry must contain entries")
    active: list[dict[str, Any]] = []
    identities: set[tuple[str, str]] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("registry entry must be an object")
        identity = (entry.get("model_name"), entry.get("run_id"))
        if identity in identities:
            raise ValueError("registry contains duplicate candidate identity")
        identities.add(identity)
        if _scope_key(entry.get("scope", {})) != _scope_key(scope):
            raise ValueError("registry entry belongs to another scope")
        relative = Path(str(entry.get("candidate_evidence_path", "")))
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("candidate evidence path must stay inside registry")
        evidence_path = (registry_dir / relative).resolve()
        if not evidence_path.is_relative_to(registry_dir):
            raise ValueError("candidate evidence path escapes registry")
        if sha256(evidence_path) != entry.get("candidate_evidence_sha256"):
            raise ValueError("registered candidate evidence changed after registration")
        evidence = load_candidate_evidence(evidence_path)
        validate_candidate_evidence(evidence)
        stage, status = _stage_and_status(evidence)
        expected = {
            "model_name": evidence["candidate"]["model_name"],
            "run_id": evidence["candidate"]["run_id"],
            "model_family": evidence["candidate"]["model_family"],
            "artifact_uri": evidence["artifact"]["uri"],
            "artifact_manifest_sha256": evidence["artifact"]["manifest_sha256"],
            "dataset_id": evidence["data"]["dataset_id"],
            "dataset_manifest_sha256": evidence["data"]["dataset_manifest_sha256"],
            "feature_definition_version": evidence["data"][
                "feature_definition_version"
            ],
            "benchmark": evidence["evaluation"]["benchmark"],
            "stage": stage,
            "status": status,
        }
        for key, value in expected.items():
            if entry.get(key) != value:
                raise ValueError(f"registry entry {key} differs from candidate evidence")
        if stage == "production" and status == "active":
            active.append(entry)

    if len(active) != 1:
        raise ValueError("registry must contain exactly one active production model per scope")
    champion = active[0]
    gates = champion.get("promotion_gates", {})
    if any(
        gates.get(name) != "passed"
        for name in (
            "artifact_integrity", "public_quality", "private_challenge",
            "operational_verification",
        )
    ) or gates.get("manual_approval") not in {
        "approved", "grandfathered_initial_champion"
    }:
        raise ValueError("active production model has incomplete promotion gates")
    if deployment.get("model_run_id") != champion["run_id"]:
        raise ValueError("deployment does not point to the active registry entry")
    if deployment.get("artifact_manifest_sha256") != champion[
        "artifact_manifest_sha256"
    ]:
        raise ValueError("deployment artifact hash does not match registry")
    if deployment.get("candidate_evidence_sha256") != champion[
        "candidate_evidence_sha256"
    ]:
        raise ValueError("deployment candidate evidence hash does not match registry")
    if _scope_key(deployment) != _scope_key(scope):
        raise ValueError("deployment belongs to another registry scope")
    rollback = deployment.get("rollback", {})
    if rollback.get("required") is not True:
        raise ValueError("deployment snapshot must require rollback")
    for deployment_key, rollback_key in (
        ("model_name", "model_name"),
        ("model_run_id", "model_run_id"),
        ("artifact_manifest_sha256", "artifact_manifest_sha256"),
    ):
        if deployment.get(deployment_key) != rollback.get(rollback_key):
            raise ValueError("rollback identity differs from active deployment")
    artifact_dir = Path(champion["artifact_uri"])
    if sha256(artifact_dir / "artifact_manifest.json") != deployment[
        "artifact_manifest_sha256"
    ]:
        raise ValueError("deployed artifact manifest changed after registration")
    audit_lines = _audit_lines(registry_dir)
    return {"active": champion, "audit_events": len(audit_lines)}
