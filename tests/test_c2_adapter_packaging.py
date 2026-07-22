from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from infra.snowflake import deploy


ROOT = Path(__file__).resolve().parents[1]


def test_simulation_adapter_spec_is_private_and_topic_isolated(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        adapter_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-adapter:0123456789ab"
        ),
        adapter_build_version="0123456789ab",
        adapter_dataset_id="sim-cdc-v1",
        adapter_allowed_tenants="tenant-a,tenant-b",
    )

    rendered = (tmp_path / "adapter-sim.yaml").read_text()
    adapter = yaml.safe_load(rendered)["spec"]
    container = adapter["containers"][0]

    assert container["name"] == "hand-adapter-sim"
    assert container["image"].endswith("poker-adapter:0123456789ab")
    assert container["args"][:2] == [
        "--simulation-mode",
        "--allow-simulation-codecs",
    ]
    assert container["env"]["CDC_DATASET_ID"] == "sim-cdc-v1"
    assert container["env"]["CDC_ALLOWED_TENANTS"] == "tenant-a,tenant-b"
    assert container["env"]["KAFKA_CDC_HAND_OUTBOX_TOPIC"] == (
        "poker.sim.cdc-hand-outbox.v1"
    )
    assert container["env"]["KAFKA_CDC_CANONICAL_HANDS_TOPIC"] == (
        "poker.sim.hands.raw.v1"
    )
    assert container["env"]["KAFKA_DEAD_LETTER_TOPIC"] == (
        "poker.sim.pipeline.dead-letter.v1"
    )
    assert container["readinessProbe"] == {"port": 9093, "path": "/healthz"}
    assert all(endpoint["public"] is False for endpoint in adapter["endpoints"])
    assert {secret["envVarName"] for secret in container["secrets"]} == {
        "KAFKA_SASL_USERNAME",
        "KAFKA_SASL_PASSWORD",
    }
    assert all(
        secret["snowflakeSecret"].endswith("KAFKA_ADAPTER_SIM_CREDENTIALS")
        for secret in container["secrets"]
    )
    assert "poker.hands.raw.v1" not in rendered.replace("poker.sim.hands.raw.v1", "")
    assert "__" not in rendered


def test_simulation_adapter_render_rejects_non_simulation_dataset(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    with pytest.raises(SystemExit, match="must start with sim-"):
        deploy.render_specs(
            deploy.DEFAULT_IMAGE_PATH,
            "broker.example.com:9092",
            adapter_dataset_id="poker-live-v1",
        )


def test_adapter_dockerfile_is_immutable_minimal_and_non_root() -> None:
    dockerfile = (ROOT / "Dockerfile.adapter").read_text()
    assert "ARG GO_VERSION=1.23.8" in dockerfile
    assert "golang:${GO_VERSION}-bookworm@sha256:" in dockerfile
    assert "debian:bookworm-slim@sha256:" in dockerfile
    assert "go build -trimpath" in dockerfile
    assert "./cmd/hand-adapter" in dockerfile
    assert "USER 65532:65532" in dockerfile
    assert 'ENTRYPOINT ["/usr/local/bin/hand-adapter"]' in dockerfile
    assert ":latest" not in dockerfile
