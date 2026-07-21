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
