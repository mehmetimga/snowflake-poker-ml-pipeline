"""Model-family-neutral candidate evidence and integrity validation."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


CANDIDATE_EVIDENCE_SCHEMA_VERSION = 1
_APPROVED_MANUAL_STATES = {"approved", "grandfathered_initial_champion"}
_MANUAL_STATES = _APPROVED_MANUAL_STATES | {"pending", "not_requested"}
_PRIVATE_STATES = {"passed", "pending", "not_run"}


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _require_exact_keys(
    value: Mapping[str, Any], owner: str, required: set[str]
) -> None:
    missing = required - set(value)
    unknown = set(value) - required
    if missing:
        raise ValueError(f"candidate evidence {owner} is missing: {', '.join(sorted(missing))}")
    if unknown:
        raise ValueError(
            f"candidate evidence {owner} has unknown fields: {', '.join(sorted(unknown))}"
        )


def _require_reference(reference: Mapping[str, Any], owner: str, location: str) -> None:
    keys = {location, "sha256"}
    _require_exact_keys(reference, owner, keys)
    if not isinstance(reference.get(location), str) or not reference[location]:
        raise ValueError(f"candidate evidence {owner}.{location} must be non-empty")
    digest = reference.get("sha256")
    if not isinstance(digest, str) or len(digest) != 64 or any(
        character not in "0123456789abcdef" for character in digest
    ):
        raise ValueError(f"candidate evidence {owner}.sha256 is invalid")


def verify_artifact_bundle(root: Path) -> dict[str, Any]:
    manifest_path = root / "artifact_manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(manifest_path)
    manifest = json.loads(manifest_path.read_text())
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict) or not artifacts:
        raise ValueError("artifact manifest must contain at least one hashed artifact")
    for relative, expected in artifacts.items():
        path = _artifact_path(root, relative)
        if not path.is_file():
            raise FileNotFoundError(path)
        if sha256(path) != expected:
            raise ValueError(f"artifact hash mismatch: {root.name}/{relative}")
    return manifest


def _artifact_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ValueError(f"artifact path must be relative and contained: {relative}")
    resolved = (root / path).resolve()
    if not resolved.is_relative_to(root.resolve()):
        raise ValueError(f"artifact path escapes bundle: {relative}")
    return resolved


def _load_object(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def json_value(document: Any, path: Sequence[str]) -> Any:
    value = document
    if not path:
        raise ValueError("JSON evidence path must not be empty")
    for key in path:
        if not isinstance(key, str) or not key:
            raise ValueError("JSON evidence path components must be non-empty strings")
        if not isinstance(value, Mapping) or key not in value:
            raise ValueError(f"JSON evidence path does not exist: {'.'.join(path)}")
        value = value[key]
    return value


def _artifact_reference(
    artifact_dir: Path, manifest: Mapping[str, Any], relative: str
) -> dict[str, str]:
    artifacts = manifest["artifacts"]
    if relative not in artifacts:
        raise ValueError(f"required evidence is not tracked by artifact manifest: {relative}")
    path = _artifact_path(artifact_dir, relative)
    if not path.is_file():
        raise FileNotFoundError(path)
    actual = sha256(path)
    if actual != artifacts[relative]:
        raise ValueError(f"artifact hash mismatch: {artifact_dir.name}/{relative}")
    return {"path": relative, "sha256": actual}


def _optional_artifact_reference(
    artifact_dir: Path,
    manifest: Mapping[str, Any],
    relative: str | None,
) -> dict[str, str] | None:
    if relative is None:
        return None
    return _artifact_reference(artifact_dir, manifest, relative)


def build_candidate_evidence(
    artifact_dir: Path,
    dataset_manifest_path: Path,
    *,
    model_family: str,
    tenant_id: str,
    product_id: str,
    benchmark: str,
    metrics_path: str,
    predictions_path: str,
    metric_name: str,
    metric_json_path: Sequence[str],
    quality_gate_json_path: Sequence[str],
    requested_stage: str,
    private_challenge: str,
    manual_approval: str,
    reasons: Sequence[str] = (),
    scoring_contract_path: str | None = None,
    decision_policy_path: str | None = None,
    operational_report_path: Path | None = None,
) -> dict[str, Any]:
    """Build one generic descriptor from explicitly located model evidence."""

    artifact_dir = artifact_dir.resolve()
    dataset_manifest_path = dataset_manifest_path.resolve()
    manifest = verify_artifact_bundle(artifact_dir)
    metrics_ref = _artifact_reference(artifact_dir, manifest, metrics_path)
    predictions_ref = _artifact_reference(artifact_dir, manifest, predictions_path)
    metrics = _load_object(artifact_dir / metrics_path)
    dataset = _load_object(dataset_manifest_path)
    metric_value = json_value(metrics, metric_json_path)
    quality_passed = json_value(metrics, quality_gate_json_path)
    if not isinstance(quality_passed, bool):
        raise ValueError("public quality gate evidence must resolve to a boolean")
    if not isinstance(metric_value, (int, float)) or isinstance(metric_value, bool):
        raise ValueError("public metric evidence must resolve to a number")
    identity = {
        "model_name": metrics.get("model_name"),
        "run_id": metrics.get("run_id"),
    }
    if any(not isinstance(value, str) or not value for value in identity.values()):
        raise ValueError("metrics must contain non-empty model_name and run_id")
    if manifest.get("model_name") != identity["model_name"]:
        raise ValueError("artifact manifest and metrics model names differ")
    if manifest.get("run_id") != identity["run_id"]:
        raise ValueError("artifact manifest and metrics run identities differ")
    if dataset.get("dataset_id") != metrics.get("dataset_id"):
        raise ValueError("dataset manifest and metrics dataset identities differ")
    if dataset.get("feature_definition_version") != metrics.get(
        "feature_definition_version"
    ):
        raise ValueError("dataset manifest and metrics feature versions differ")
    if metrics.get("dataset_manifest_sha256") != sha256(dataset_manifest_path):
        raise ValueError("metrics do not bind the supplied dataset manifest")
    if metrics.get("benchmark") != benchmark:
        raise ValueError("metrics benchmark differs from candidate scope")

    operational_ref: dict[str, str] | None = None
    if operational_report_path is not None:
        operational_report_path = operational_report_path.resolve()
        operational_ref = {
            "uri": str(operational_report_path),
            "sha256": sha256(operational_report_path),
        }
    evidence: dict[str, Any] = {
        "schema_version": CANDIDATE_EVIDENCE_SCHEMA_VERSION,
        "candidate": {**identity, "model_family": model_family},
        "scope": {
            "tenant_id": tenant_id,
            "product_id": product_id,
            "benchmark": benchmark,
        },
        "artifact": {
            "uri": str(artifact_dir),
            "manifest_path": "artifact_manifest.json",
            "manifest_sha256": sha256(artifact_dir / "artifact_manifest.json"),
        },
        "data": {
            "dataset_id": dataset["dataset_id"],
            "dataset_manifest_uri": str(dataset_manifest_path),
            "dataset_manifest_sha256": sha256(dataset_manifest_path),
            "feature_definition_version": dataset["feature_definition_version"],
        },
        "evaluation": {
            "benchmark": benchmark,
            "split": "test",
            "metric_name": metric_name,
            "metric_value": float(metric_value),
            "metric_json_path": list(metric_json_path),
            "quality_gate": "passed" if quality_passed else "failed",
            "quality_gate_json_path": list(quality_gate_json_path),
            "reasons": list(reasons),
        },
        "evidence": {
            "metrics": metrics_ref,
            "predictions": predictions_ref,
        },
        "runtime": {
            "scoring_contract": _optional_artifact_reference(
                artifact_dir, manifest, scoring_contract_path
            ),
            "decision_policy": _optional_artifact_reference(
                artifact_dir, manifest, decision_policy_path
            ),
            "operational_report": operational_ref,
        },
        "governance": {
            "requested_stage": requested_stage,
            "private_challenge": private_challenge,
            "manual_approval": manual_approval,
            "rollback_required": True,
        },
    }
    evidence["document_sha256"] = _canonical_sha256(evidence)
    validate_candidate_evidence(evidence)
    return evidence


def write_candidate_evidence(path: Path, evidence: Mapping[str, Any]) -> None:
    validate_candidate_evidence(evidence)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")


def load_candidate_evidence(path: Path) -> dict[str, Any]:
    evidence = _load_object(path)
    validate_candidate_evidence(evidence)
    return evidence


def validate_candidate_evidence(evidence: Mapping[str, Any]) -> None:
    """Validate document shape, hashes, cross-file identity, and production gates."""

    if evidence.get("schema_version") != CANDIDATE_EVIDENCE_SCHEMA_VERSION:
        raise ValueError("unsupported candidate evidence schema version")
    _require_exact_keys(
        evidence,
        "document",
        {
            "schema_version", "candidate", "scope", "artifact", "data",
            "evaluation", "evidence", "runtime", "governance",
            "document_sha256",
        },
    )
    required_objects = (
        "candidate", "scope", "artifact", "data", "evaluation", "evidence",
        "runtime", "governance",
    )
    for key in required_objects:
        if not isinstance(evidence.get(key), Mapping):
            raise ValueError(f"candidate evidence {key} must be an object")
    expected_document_hash = evidence.get("document_sha256")
    unsigned = dict(evidence)
    unsigned.pop("document_sha256", None)
    if expected_document_hash != _canonical_sha256(unsigned):
        raise ValueError("candidate evidence document hash mismatch")

    candidate = evidence["candidate"]
    scope = evidence["scope"]
    artifact = evidence["artifact"]
    data = evidence["data"]
    evaluation = evidence["evaluation"]
    sources = evidence["evidence"]
    runtime = evidence["runtime"]
    governance = evidence["governance"]
    _require_exact_keys(
        candidate, "candidate", {"model_name", "run_id", "model_family"}
    )
    _require_exact_keys(
        scope, "scope", {"tenant_id", "product_id", "benchmark"}
    )
    _require_exact_keys(
        artifact, "artifact", {"uri", "manifest_path", "manifest_sha256"}
    )
    _require_exact_keys(
        data,
        "data",
        {
            "dataset_id", "dataset_manifest_uri", "dataset_manifest_sha256",
            "feature_definition_version",
        },
    )
    _require_exact_keys(
        evaluation,
        "evaluation",
        {
            "benchmark", "split", "metric_name", "metric_value",
            "metric_json_path", "quality_gate", "quality_gate_json_path", "reasons",
        },
    )
    _require_exact_keys(sources, "evidence", {"metrics", "predictions"})
    _require_exact_keys(
        runtime,
        "runtime",
        {"scoring_contract", "decision_policy", "operational_report"},
    )
    _require_exact_keys(
        governance,
        "governance",
        {"requested_stage", "private_challenge", "manual_approval", "rollback_required"},
    )
    for owner, keys in (
        (candidate, ("model_name", "run_id", "model_family")),
        (scope, ("tenant_id", "product_id", "benchmark")),
        (data, ("dataset_id", "feature_definition_version")),
    ):
        if any(not isinstance(owner.get(key), str) or not owner[key] for key in keys):
            raise ValueError("candidate identity and scope fields must be non-empty strings")
    if evaluation.get("benchmark") != scope["benchmark"]:
        raise ValueError("evaluation benchmark differs from registry scope")
    if evaluation.get("split") != "test":
        raise ValueError("candidate promotion evidence must use the public test split")
    if not isinstance(evaluation.get("metric_name"), str) or not evaluation[
        "metric_name"
    ]:
        raise ValueError("candidate metric name must be a non-empty string")
    if evaluation.get("quality_gate") not in {"passed", "failed"}:
        raise ValueError("candidate quality gate must be passed or failed")
    if not isinstance(evaluation.get("metric_value"), (int, float)) or isinstance(
        evaluation.get("metric_value"), bool
    ):
        raise ValueError("candidate evaluation metric must be numeric")
    if not isinstance(evaluation.get("reasons"), list) or any(
        not isinstance(reason, str) for reason in evaluation["reasons"]
    ):
        raise ValueError("candidate evaluation reasons must be a string array")
    for path_name in ("metric_json_path", "quality_gate_json_path"):
        path_value = evaluation.get(path_name)
        if not isinstance(path_value, list) or not path_value or any(
            not isinstance(component, str) or not component
            for component in path_value
        ):
            raise ValueError(f"candidate {path_name} must be a non-empty string array")

    artifact_dir = Path(str(artifact.get("uri", ""))).resolve()
    _require_reference(
        {"uri": artifact.get("uri"), "sha256": artifact.get("manifest_sha256")},
        "artifact",
        "uri",
    )
    if artifact.get("manifest_path") != "artifact_manifest.json":
        raise ValueError("unsupported artifact manifest path")
    manifest_path = artifact_dir / "artifact_manifest.json"
    if sha256(manifest_path) != artifact.get("manifest_sha256"):
        raise ValueError("candidate artifact manifest hash mismatch")
    manifest = verify_artifact_bundle(artifact_dir)
    if manifest.get("model_name") != candidate["model_name"]:
        raise ValueError("candidate model name differs from artifact manifest")
    if manifest.get("run_id") != candidate["run_id"]:
        raise ValueError("candidate run differs from artifact manifest")

    dataset_path = Path(str(data.get("dataset_manifest_uri", ""))).resolve()
    _require_reference(
        {
            "uri": data.get("dataset_manifest_uri"),
            "sha256": data.get("dataset_manifest_sha256"),
        },
        "dataset_manifest",
        "uri",
    )
    if sha256(dataset_path) != data.get("dataset_manifest_sha256"):
        raise ValueError("candidate dataset manifest hash mismatch")
    dataset = _load_object(dataset_path)
    if dataset.get("dataset_id") != data["dataset_id"]:
        raise ValueError("candidate dataset identity differs from its manifest")
    if dataset.get("feature_definition_version") != data[
        "feature_definition_version"
    ]:
        raise ValueError("candidate feature version differs from dataset manifest")

    for name in ("metrics", "predictions"):
        reference = sources.get(name)
        if not isinstance(reference, Mapping):
            raise ValueError(f"candidate must include {name} evidence")
        _require_reference(reference, name, "path")
        relative = str(reference.get("path", ""))
        if manifest["artifacts"].get(relative) != reference.get("sha256"):
            raise ValueError(f"candidate {name} hash is not bound by artifact manifest")
        if sha256(_artifact_path(artifact_dir, relative)) != reference.get("sha256"):
            raise ValueError(f"candidate {name} evidence hash mismatch")

    metrics = _load_object(_artifact_path(artifact_dir, sources["metrics"]["path"]))
    expected_identity = {
        "model_name": candidate["model_name"],
        "run_id": candidate["run_id"],
        "dataset_id": data["dataset_id"],
        "dataset_manifest_sha256": data["dataset_manifest_sha256"],
        "feature_definition_version": data["feature_definition_version"],
        "benchmark": scope["benchmark"],
    }
    for key, expected in expected_identity.items():
        if metrics.get(key) != expected:
            raise ValueError(f"candidate metrics {key} mismatch")
    metric = json_value(metrics, evaluation.get("metric_json_path", []))
    if float(metric) != float(evaluation["metric_value"]):
        raise ValueError("candidate metric value differs from hashed metrics")
    quality = json_value(metrics, evaluation.get("quality_gate_json_path", []))
    if not isinstance(quality, bool):
        raise ValueError("candidate quality evidence must resolve to a boolean")
    if quality != (evaluation["quality_gate"] == "passed"):
        raise ValueError("candidate quality gate differs from hashed metrics")

    for name in ("scoring_contract", "decision_policy"):
        reference = runtime.get(name)
        if reference is None:
            continue
        if not isinstance(reference, Mapping):
            raise ValueError(f"candidate runtime {name} must be an artifact reference")
        _require_reference(reference, name, "path")
        relative = str(reference.get("path", ""))
        if manifest["artifacts"].get(relative) != reference.get("sha256"):
            raise ValueError(f"candidate runtime {name} is not hash-bound")
        if sha256(_artifact_path(artifact_dir, relative)) != reference.get("sha256"):
            raise ValueError(f"candidate runtime {name} hash mismatch")

    operational = runtime.get("operational_report")
    operational_passed = False
    if operational is not None:
        if not isinstance(operational, Mapping):
            raise ValueError("operational report reference must be an object or null")
        _require_reference(operational, "operational_report", "uri")
        operational_path = Path(str(operational.get("uri", ""))).resolve()
        if sha256(operational_path) != operational.get("sha256"):
            raise ValueError("candidate operational report hash mismatch")
        report = _load_object(operational_path)
        if report.get("model_name") != candidate["model_name"]:
            raise ValueError("operational report belongs to another model")
        if report.get("model_run_id") != candidate["run_id"]:
            raise ValueError("operational report belongs to another run")
        if report.get("artifact_manifest_sha256") != artifact["manifest_sha256"]:
            raise ValueError("operational report belongs to another artifact")
        operational_passed = report.get("passed") is True

    if governance.get("requested_stage") not in {"production", "candidate"}:
        raise ValueError("requested stage must be production or candidate")
    if governance.get("private_challenge") not in _PRIVATE_STATES:
        raise ValueError("invalid private challenge status")
    if governance.get("manual_approval") not in _MANUAL_STATES:
        raise ValueError("invalid manual approval status")
    if governance.get("rollback_required") is not True:
        raise ValueError("candidate governance must require rollback")
    if governance["requested_stage"] == "production":
        production_checks = {
            "public quality": evaluation["quality_gate"] == "passed",
            "private challenge": governance["private_challenge"] == "passed",
            "manual approval": governance["manual_approval"]
            in _APPROVED_MANUAL_STATES,
            "operational verification": operational_passed,
            "scoring contract": runtime.get("scoring_contract") is not None,
            "decision policy": runtime.get("decision_policy") is not None,
        }
        failed = [name for name, passed in production_checks.items() if not passed]
        if failed:
            raise ValueError(
                "production candidate is missing required gates: " + ", ".join(failed)
            )
