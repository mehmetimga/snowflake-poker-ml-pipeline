from __future__ import annotations

import hashlib
from pathlib import Path

import yaml
import pytest

from infra.snowflake import deploy
from scripts.build_risk_runtime_bundle import build_runtime_bundle
from scripts.check_c1_release import validate_release_tag
from scripts.c1_image_tag import default_image_tag


ROOT = Path(__file__).resolve().parents[1]


def test_runtime_bundle_is_minimal_and_hash_verified(tmp_path: Path) -> None:
    output = tmp_path / "runtime"
    manifest = build_runtime_bundle(ROOT / "models/pair-catboost-full-v2", output)
    assert manifest["run_id"] == deploy.DEFAULT_MODEL_RUN_ID
    assert "predictions.parquet" not in manifest["artifacts"]
    assert "triton/pair_catboost/1/model.onnx" in manifest["artifacts"]
    for relative, expected in manifest["artifacts"].items():
        actual = hashlib.sha256((output / relative).read_bytes()).hexdigest()
        assert actual == expected


def test_upload_rejects_runtime_bundle_mutation_before_connecting(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "runtime"
    build_runtime_bundle(ROOT / "models/pair-catboost-full-v2", output)
    (output / "calibration.json").write_text("{}\n")
    monkeypatch.setattr(
        deploy,
        "_warehouse",
        lambda: (_ for _ in ()).throw(AssertionError("warehouse must not open")),
    )
    with pytest.raises(SystemExit, match="hash mismatch"):
        deploy.upload_risk_bundle(output)


def test_c1_specs_define_separate_private_services(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        risk_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-risk:0123456789ab"
        ),
        flink_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:0123456789ab"
        ),
        triton_image_path=deploy.DEFAULT_TRITON_IMAGE_PATH,
        build_version="0123456789ab",
        model_run_id=deploy.DEFAULT_MODEL_RUN_ID,
        allowed_tenants="tenant-a,tenant-b",
    )

    risk = yaml.safe_load((tmp_path / "risk.yaml").read_text())["spec"]
    flink = yaml.safe_load((tmp_path / "flink.yaml").read_text())["spec"]

    assert [value["name"] for value in risk["containers"]] == ["risk", "triton"]
    assert risk["containers"][0]["image"].endswith("poker-risk:0123456789ab")
    assert risk["containers"][0]["env"]["RISK_ALLOWED_TENANTS"] == (
        "tenant-a,tenant-b"
    )
    assert risk["containers"][0]["readinessProbe"] == {
        "port": 9091,
        "path": "/healthz",
    }
    assert all(endpoint["public"] is False for endpoint in risk["endpoints"])
    assert risk["volumes"][0]["source"] == "stage"
    assert deploy.DEFAULT_MODEL_RUN_ID in risk["volumes"][0]["stageConfig"]["name"]

    assert [value["name"] for value in flink["containers"]] == [
        "jobmanager",
        "taskmanager",
        "submitter",
    ]
    assert all(
        container["image"].endswith("poker-flink:0123456789ab")
        for container in flink["containers"]
    )
    state = flink["volumes"][0]
    assert state["source"] == "block"
    assert state["blockConfig"]["snapshotOnDelete"] is True
    assert "state.checkpoints.dir: file:///opt/flink/state/checkpoints" in (
        flink["containers"][0]["env"]["FLINK_PROPERTIES"]
    )
    assert all(endpoint["public"] is False for endpoint in flink["endpoints"])
    assert "__" not in (tmp_path / "risk.yaml").read_text()
    assert "__" not in (tmp_path / "flink.yaml").read_text()


def test_c1_dockerfiles_pin_language_and_runtime_versions() -> None:
    risk = (ROOT / "Dockerfile.risk").read_text()
    flink = (ROOT / "Dockerfile.flink").read_text()
    assert "golang:${GO_VERSION}-bookworm" in risk
    assert "ARG GO_VERSION=1.23.8" in risk
    assert "USER 65532:65532" in risk
    assert "apache/flink:1.19.1-scala_2.12-java17" in flink
    assert "maven:3.9.9-eclipse-temurin-17" in flink
    assert ":latest" not in risk + flink


def test_flink_supervisor_requires_both_jobs() -> None:
    script = (ROOT / "streaming/flink-java/docker/submit-jobs.sh").read_text()
    assert "poker-active-context-enrichment-v2" in script
    assert "poker-pair-features-v1-from-context-v2" in script
    assert "FLINK_CONTEXT_SAVEPOINT_PATH" in script
    assert "FLINK_PAIR_SAVEPOINT_PATH" in script
    assert "flink list -r" in script


def test_flink_image_check_works_in_jre_and_requires_snowflake_adapter() -> None:
    script = (ROOT / "streaming/flink-java/docker/check-image.sh").read_text()
    assert "jar tf" not in script
    assert 'grep -aFq "${class_path}" "${jar_path}"' in script
    assert "SnowflakeServiceConnectionFactory.class" in script
    assert "SnowflakeUserContextRepository.class" in script
    assert "net/snowflake/client/api/driver/SnowflakeDriver.class" in script


def test_release_guard_rejects_non_head_tag(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.check_c1_release.git",
        lambda *arguments: "" if arguments[0] == "status" else "0123456789ab",
    )
    try:
        validate_release_tag("dev-0123456789ab")
    except ValueError as error:
        assert "must equal current Git SHA" in str(error)
    else:
        raise AssertionError("development tag unexpectedly passed release guard")


def test_dirty_tree_default_tag_cannot_look_like_release(monkeypatch) -> None:
    monkeypatch.setattr(
        "scripts.c1_image_tag.git",
        lambda *arguments: "changed" if arguments[0] == "status" else "0123456789ab",
    )
    assert default_image_tag() == "dev-0123456789ab"
