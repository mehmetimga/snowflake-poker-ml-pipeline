#!/usr/bin/env python3
"""Fail closed when R7 image roles, lifecycle, or hardening contracts drift."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "infra/snowflake/image-roles.yaml"
GENERIC_PLACEHOLDER = "__IMAGE_PATH__"
ALLOWED_LIFECYCLES = {
    "persistent-service",
    "persistent-service-and-ephemeral-savepoint-job",
    "on-demand-service",
    "ephemeral-job",
    "retained-suspended-service",
}
PLACEHOLDERS = {
    "poker-adapter": "__ADAPTER_IMAGE_PATH__",
    "poker-flink": "__FLINK_IMAGE_PATH__",
    "poker-risk": "__RISK_IMAGE_PATH__",
    "poker-sink": "__SINK_IMAGE_PATH__",
    "poker-admin": "__ADMIN_IMAGE_PATH__",
    "poker-train": "__TRAIN_IMAGE_PATH__",
    "poker-pipeline": GENERIC_PLACEHOLDER,
}
FORBIDDEN_ADMIN_DEPENDENCIES = {
    "catboost",
    "lightgbm",
    "torch",
    "torch-geometric",
    "xgboost",
}
FORBIDDEN_TRAIN_DEPENDENCIES = {
    "kafka-python-ng",
    "pokerkit",
    "qdrant-client",
    "streamlit",
    "torch",
    "torch-geometric",
}


def _load(path: Path) -> dict[str, Any]:
    value = yaml.safe_load(path.read_text())
    if not isinstance(value, dict):
        raise ValueError("R7 image-role manifest must be a mapping")
    return value


def _requirements(path: Path) -> set[str]:
    names = set()
    for raw in path.read_text().splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        name = line.split("==", 1)[0].split("[", 1)[0].lower()
        names.add(name)
    return names


def _last_user(dockerfile: str) -> str | None:
    users = [
        line.split(maxsplit=1)[1].strip()
        for line in dockerfile.splitlines()
        if line.strip().upper().startswith("USER ")
    ]
    return users[-1] if users else None


def audit_image_roles(
    manifest_path: Path = DEFAULT_MANIFEST,
    *,
    root: Path = ROOT,
) -> list[str]:
    manifest = _load(manifest_path)
    errors: list[str] = []
    if manifest.get("version") != 1:
        errors.append("manifest version must be 1")
    images = manifest.get("images")
    if not isinstance(images, dict) or set(images) != set(PLACEHOLDERS):
        errors.append("manifest must declare exactly the governed R7 image set")
        return errors

    claimed_specs: dict[str, str] = {}
    for repository, raw in images.items():
        if not isinstance(raw, dict):
            errors.append(f"{repository}: contract must be a mapping")
            continue
        lifecycle = str(raw.get("lifecycle", ""))
        if lifecycle not in ALLOWED_LIFECYCLES:
            errors.append(f"{repository}: invalid lifecycle {lifecycle!r}")
        dockerfile_path = root / str(raw.get("dockerfile", ""))
        if not dockerfile_path.is_file():
            errors.append(f"{repository}: missing Dockerfile")
            continue
        dockerfile = dockerfile_path.read_text()
        if f'org.opencontainers.image.title="{repository}"' not in dockerfile:
            errors.append(f"{repository}: OCI title does not match repository")
        if 'org.opencontainers.image.revision="${BUILD_VERSION}"' not in dockerfile:
            errors.append(f"{repository}: missing immutable build revision label")
        if ":latest" in dockerfile:
            errors.append(f"{repository}: floating latest tag is forbidden")
        if "@sha256:" not in dockerfile:
            errors.append(f"{repository}: runtime base image is not digest pinned")
        expected_user = raw.get("runtime_user")
        if expected_user is not None and _last_user(dockerfile) != str(expected_user):
            errors.append(
                f"{repository}: final USER must be {expected_user}, "
                f"got {_last_user(dockerfile)}"
            )
        if expected_user is None and "runtime_user_exception" not in raw:
            errors.append(f"{repository}: non-root user or explicit exception required")

        specs = list(raw.get("specs", [])) + list(raw.get("job_specs", []))
        if not specs:
            errors.append(f"{repository}: no service or job spec is assigned")
        for spec_name in specs:
            spec_path = root / "infra/snowflake/specs" / str(spec_name)
            if not spec_path.is_file():
                errors.append(f"{repository}: missing spec {spec_name}")
                continue
            if spec_name in claimed_specs:
                errors.append(
                    f"{spec_name}: claimed by {claimed_specs[spec_name]} and {repository}"
                )
            claimed_specs[str(spec_name)] = repository
            text = spec_path.read_text()
            placeholder = PLACEHOLDERS[repository]
            if placeholder not in text:
                errors.append(f"{repository}: {spec_name} lacks {placeholder}")
            if repository != "poker-pipeline" and GENERIC_PLACEHOLDER in text:
                errors.append(f"{repository}: {spec_name} uses legacy generic image")

        requirements = raw.get("requirements")
        if requirements is not None and not (root / str(requirements)).is_file():
            errors.append(f"{repository}: missing requirements lock")

    template_names = {
        path.name
        for path in (root / "infra/snowflake/specs").glob("*.yaml.template")
        if path.name
        in {
            "adapter-sim.yaml.template",
            "admin.yaml.template",
            "flink-sim.yaml.template",
            "flink.yaml.template",
            "realtime.yaml.template",
            "risk-sim.yaml.template",
            "risk.yaml.template",
            "sink.yaml.template",
            "train-job.yaml.template",
        }
    }
    if set(claimed_specs) != template_names:
        missing = sorted(template_names - set(claimed_specs))
        extra = sorted(set(claimed_specs) - template_names)
        errors.append(f"spec ownership mismatch missing={missing} extra={extra}")

    generic_specs = sorted(
        path.name
        for path in (root / "infra/snowflake/specs").glob("*.yaml.template")
        if GENERIC_PLACEHOLDER in path.read_text()
    )
    if generic_specs != ["realtime.yaml.template"]:
        errors.append(
            "legacy generic image must be referenced only by realtime.yaml.template"
        )

    admin_dependencies = _requirements(root / "requirements.admin.txt")
    forbidden_admin = sorted(admin_dependencies & FORBIDDEN_ADMIN_DEPENDENCIES)
    if forbidden_admin:
        errors.append(f"admin image includes training dependencies: {forbidden_admin}")
    train_dependencies = _requirements(root / "requirements.train.txt")
    forbidden_train = sorted(train_dependencies & FORBIDDEN_TRAIN_DEPENDENCIES)
    if forbidden_train:
        errors.append(f"training image includes unrelated dependencies: {forbidden_train}")

    retrain_page = (root / "admin/pages/5_Retrain.py").read_text()
    if "subprocess" in retrain_page or "scripts/train.py" in retrain_page:
        errors.append("admin UI must not execute training in-process")
    train_spec = (root / "infra/snowflake/specs/train-job.yaml.template").read_text()
    if "__TRAIN_IMAGE_PATH__" not in train_spec:
        errors.append("training job does not use the dedicated image")
    savepoint_controller = (root / "scripts/spcs_flink_savepoints.py").read_text()
    if "EXECUTE JOB SERVICE" not in savepoint_controller:
        errors.append("Flink savepoint controller must use ephemeral job services")

    makefile = (root / "Makefile").read_text()
    required_make_contracts = {
        "default build routes to R7 images": "snow-build: r7-build",
        "default push routes to R7 images": "snow-push: r7-push",
        "admin deploy routes to R7": "snow-deploy-admin: r7-deploy-admin",
        "training submission routes to R7": "snow-train: r7-train-run",
        "legacy build is explicit": "snow-legacy-realtime-build:",
        "legacy push is explicit": "snow-legacy-realtime-push:",
        "R5 admin uses its dedicated image": (
            "R5_ADMIN_IMAGE ?= poker-admin:$(R5_SINK_IMAGE_TAG)"
        ),
        "R5 admin uses its dedicated Dockerfile": (
            "-f Dockerfile.admin -t $(R5_ADMIN_IMAGE) ."
        ),
        "R7 admin image is dedicated": (
            "R7_ADMIN_IMAGE ?= poker-admin:$(R7_IMAGE_TAG)"
        ),
        "R7 training image is dedicated": (
            "R7_TRAIN_IMAGE ?= poker-train:$(R7_IMAGE_TAG)"
        ),
    }
    for label, contract in required_make_contracts.items():
        if contract not in makefile:
            errors.append(f"Makefile: {label} contract is missing")
    forbidden_make_contracts = {
        "R5_ADMIN_IMAGE ?= poker-pipeline",
        "R5_REMOTE_ADMIN_IMAGE ?= $(SNOW_REPO_URL)/poker-pipeline",
        "-f Dockerfile.spcs -t $(R5_ADMIN_IMAGE)",
        "snow-build:\n\tdocker buildx",
        "snow-push:\n\tdocker tag $(SNOW_IMAGE)",
    }
    for contract in sorted(forbidden_make_contracts):
        if contract in makefile:
            errors.append(
                f"Makefile: legacy generic image escaped its rollback role: {contract}"
            )
    return errors


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    args = parser.parse_args()
    errors = audit_image_roles(args.manifest)
    if errors:
        raise SystemExit(
            "[r7-image-roles] failed:\n- " + "\n- ".join(errors)
        )
    print("[r7-image-roles] passed repositories=7 generic_consumers=1")


if __name__ == "__main__":
    main()
