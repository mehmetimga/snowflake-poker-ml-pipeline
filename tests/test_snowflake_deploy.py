from __future__ import annotations

from pathlib import Path

import pytest

from infra.snowflake import deploy


def test_parse_brokers_accepts_managed_kafka_endpoints():
    assert deploy._parse_brokers("broker-a.example.com:9092, broker-b.example.com:9092") == [
        "broker-a.example.com:9092",
        "broker-b.example.com:9092",
    ]


def test_configured_kafka_egress_uses_explicit_brokers(monkeypatch):
    monkeypatch.setenv("KAFKA_EGRESS_BROKERS", "b0.example.com:9092,b1.example.com:9092")

    assert deploy._configured_kafka_egress("bootstrap.example.com:9092") == (
        "b0.example.com:9092,b1.example.com:9092"
    )


@pytest.mark.parametrize("value", ["", "https://broker:9092", "broker", "broker:25"])
def test_parse_brokers_rejects_invalid_or_restricted_endpoints(value: str):
    with pytest.raises(SystemExit):
        deploy._parse_brokers(value)


def test_render_specs_substitutes_image_and_kafka(monkeypatch, tmp_path: Path):
    templates = tmp_path / "templates"
    rendered = tmp_path / "rendered"
    templates.mkdir()
    (templates / "realtime.yaml.template").write_text(
        "image: __IMAGE_PATH__\nbrokers: __KAFKA_BOOTSTRAP_SERVERS__\n"
        "group: __RISK_SCORER_GROUP_ID__\n"
        "risk_build: __RISK_BUILD_VERSION__\n"
        "flink_build: __FLINK_BUILD_VERSION__\n"
        "context_savepoint: __FLINK_CONTEXT_SAVEPOINT_PATH__\n"
        "pair_savepoint: __FLINK_PAIR_SAVEPOINT_PATH__\n"
    )
    monkeypatch.setattr(deploy, "SPECS_DIR", templates)
    monkeypatch.setattr(deploy, "RENDERED_DIR", rendered)

    deploy.render_specs(
        "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-pipeline:dev",
        "broker.example.com:9092",
    )

    assert (rendered / "realtime.yaml").read_text() == (
        "image: /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-pipeline:dev\n"
        "brokers: broker.example.com:9092\n"
        "group: poker-go-risk-scorer-v1\n"
        "risk_build: dev\n"
        "flink_build: dev\n"
        "context_savepoint: \n"
        "pair_savepoint: \n"
    )


def test_render_specs_supports_mixed_immutable_builds(monkeypatch, tmp_path: Path):
    templates = tmp_path / "templates"
    rendered = tmp_path / "rendered"
    templates.mkdir()
    (templates / "mixed.yaml.template").write_text(
        "risk: __RISK_BUILD_VERSION__\nflink: __FLINK_BUILD_VERSION__\n"
    )
    monkeypatch.setattr(deploy, "SPECS_DIR", templates)
    monkeypatch.setattr(deploy, "RENDERED_DIR", rendered)

    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        risk_build_version="21ebb31c01d6",
        flink_build_version="603ff5dbd89f",
    )

    assert (rendered / "mixed.yaml").read_text() == (
        "risk: 21ebb31c01d6\nflink: 603ff5dbd89f\n"
    )


def test_render_specs_rejects_unsafe_release_identity(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(deploy, "SPECS_DIR", tmp_path)
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path / "rendered")
    with pytest.raises(SystemExit, match="build version"):
        deploy.render_specs(
            deploy.DEFAULT_IMAGE_PATH,
            "broker.example.com:9092",
            build_version="sha;DROP SERVICE",
        )

    with pytest.raises(SystemExit, match="risk scorer group ID"):
        deploy.render_specs(
            deploy.DEFAULT_IMAGE_PATH,
            "broker.example.com:9092",
            risk_scorer_group_id="group;DROP SERVICE",
        )


def test_render_specs_accepts_validated_flink_savepoints(monkeypatch, tmp_path: Path):
    templates = tmp_path / "templates"
    rendered = tmp_path / "rendered"
    templates.mkdir()
    (templates / "flink.yaml.template").write_text(
        "context: '__FLINK_CONTEXT_SAVEPOINT_PATH__'\n"
        "pair: '__FLINK_PAIR_SAVEPOINT_PATH__'\n"
    )
    monkeypatch.setattr(deploy, "SPECS_DIR", templates)
    monkeypatch.setattr(deploy, "RENDERED_DIR", rendered)

    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        flink_context_savepoint_path=(
            "file:///opt/flink/state/savepoints/savepoint-context123"
        ),
        flink_pair_savepoint_path="file:/opt/flink/state/savepoints/savepoint-pair456",
    )

    assert (rendered / "flink.yaml").read_text() == (
        "context: 'file:///opt/flink/state/savepoints/savepoint-context123'\n"
        "pair: 'file:/opt/flink/state/savepoints/savepoint-pair456'\n"
    )


@pytest.mark.parametrize(
    "value",
    [
        "https://example.com/savepoint-1",
        "file:///tmp/savepoint-1",
        "file:///opt/flink/state/savepoints/../escape",
        "file:///opt/flink/state/savepoints/savepoint-x\"\nenv: injected",
    ],
)
def test_render_specs_rejects_unsafe_flink_savepoint_uri(
    value: str, monkeypatch, tmp_path: Path
):
    monkeypatch.setattr(deploy, "SPECS_DIR", tmp_path)
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path / "rendered")

    with pytest.raises(SystemExit, match="context savepoint URI"):
        deploy.render_specs(
            deploy.DEFAULT_IMAGE_PATH,
            "broker.example.com:9092",
            flink_context_savepoint_path=value,
        )
