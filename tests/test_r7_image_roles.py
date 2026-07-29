from __future__ import annotations

from pathlib import Path

import yaml

from infra.snowflake import deploy
from scripts.check_r7_image_roles import audit_image_roles


ROOT = Path(__file__).resolve().parents[1]


def test_r7_image_role_audit_passes_repository_contract() -> None:
    assert audit_image_roles() == []


def test_admin_and_training_render_to_dedicated_images(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        admin_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-admin:0123456789ab"
        ),
        train_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-train:0123456789ab"
        ),
        admin_build_version="0123456789ab",
        train_build_version="0123456789ab",
    )

    admin = yaml.safe_load((tmp_path / "admin.yaml").read_text())["spec"][
        "containers"
    ][0]
    training = yaml.safe_load((tmp_path / "train-job.yaml").read_text())[
        "spec"
    ]["containers"][0]
    realtime = yaml.safe_load((tmp_path / "realtime.yaml").read_text())[
        "spec"
    ]["containers"][0]

    assert admin["image"].endswith("poker-admin:0123456789ab")
    assert admin["env"]["ADMIN_BUILD_VERSION"] == "0123456789ab"
    assert training["image"].endswith("poker-train:0123456789ab")
    assert training["env"]["TRAIN_BUILD_VERSION"] == "0123456789ab"
    assert realtime["image"].endswith("poker-pipeline:dev")


def test_dedicated_python_images_are_non_root_minimal_and_revisioned() -> None:
    admin = (ROOT / "Dockerfile.admin").read_text()
    training = (ROOT / "Dockerfile.train").read_text()

    for repository, dockerfile in (
        ("poker-admin", admin),
        ("poker-train", training),
    ):
        assert "FROM python:3.11-slim@sha256:" in dockerfile
        assert f'org.opencontainers.image.title="{repository}"' in dockerfile
        assert 'org.opencontainers.image.revision="${BUILD_VERSION}"' in dockerfile
        assert "USER 65532:65532" in dockerfile
        assert ":latest" not in dockerfile

    assert "requirements.admin.txt" in admin
    assert "requirements.train.txt" not in admin
    assert "requirements.train.txt" in training
    assert "requirements.admin.txt" not in training
    assert "COPY --chown=65532:65532 admin/" in admin
    assert "COPY --chown=65532:65532 admin/" not in training


def test_training_is_ephemeral_and_not_executed_by_admin() -> None:
    retrain_page = (ROOT / "admin/pages/5_Retrain.py").read_text()
    training_entrypoint = (ROOT / "scripts/snowflake_train.py").read_text()
    train_spec = yaml.safe_load(
        (ROOT / "infra/snowflake/specs/train-job.yaml.template").read_text()
    )["spec"]["containers"][0]

    assert "subprocess" not in retrain_page
    assert "scripts/train.py" not in retrain_page
    assert "r7-train-run" in retrain_page
    assert "subprocess" not in training_entrypoint
    assert "train_all(" in training_entrypoint
    assert train_spec["image"] == "__TRAIN_IMAGE_PATH__"


def test_flink_runtime_returns_to_non_root_user() -> None:
    dockerfile = (ROOT / "Dockerfile.flink").read_text().strip()
    assert "\nUSER flink\n" in dockerfile
    assert dockerfile.rfind("USER flink") > dockerfile.rfind("USER root")


def test_r7_make_targets_cover_release_security_and_lifecycle() -> None:
    makefile = (ROOT / "Makefile").read_text()
    for target in (
        "r7-role-audit:",
        "r7-admin-build:",
        "r7-train-build:",
        "r7-image-smoke:",
        "r7-release-check:",
        "r7-sbom:",
        "r7-vulnerability-scan:",
        "r7-deploy-admin:",
        "r7-train-run:",
        "phase-r7-check:",
    ):
        assert target in makefile
    assert "syft $(R7_ADMIN_IMAGE)" in makefile
    assert "trivy image --exit-code 1 --severity HIGH,CRITICAL" in makefile
    assert "snow-build: r7-build" in makefile
    assert "snow-push: r7-push" in makefile
    assert "snow-deploy-admin: r7-deploy-admin" in makefile
    assert "snow-train: r7-train-run" in makefile
    assert "snow-legacy-realtime-build:" in makefile
    assert "snow-legacy-realtime-push:" in makefile
    assert "R5_ADMIN_IMAGE ?= poker-admin:" in makefile
    assert "-f Dockerfile.admin -t $(R5_ADMIN_IMAGE) ." in makefile
