from __future__ import annotations

from pathlib import Path

import yaml

from infra.snowflake import deploy


def test_shadow_specs_are_private_and_fail_closed(monkeypatch, tmp_path: Path) -> None:
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
        risk_build_version="0123456789ab",
        flink_build_version="0123456789ab",
    )

    flink_text = (tmp_path / "flink-sim.yaml").read_text()
    risk_text = (tmp_path / "risk-sim.yaml").read_text()
    flink = yaml.safe_load(flink_text)["spec"]
    risk = yaml.safe_load(risk_text)["spec"]
    submitter = flink["containers"][2]
    scorer = risk["containers"][0]

    assert submitter["env"]["FLINK_SIMULATION_MODE"] == "true"
    assert submitter["env"]["KAFKA_WORLD_HANDS_TOPIC"] == (
        "poker.sim.hands.raw.v1"
    )
    assert submitter["env"]["KAFKA_PAIR_FEATURES_TOPIC"] == (
        "poker.sim.pair-features.v1"
    )
    assert scorer["args"][0] == "--simulation-mode"
    assert scorer["env"]["KAFKA_PAIR_FEATURES_TOPIC"] == (
        "poker.sim.pair-features.v1"
    )
    assert scorer["env"]["KAFKA_RISK_SCORES_TOPIC"] == (
        "poker.sim.risk-scores.v1"
    )
    assert all(endpoint["public"] is False for endpoint in flink["endpoints"])
    assert all(endpoint["public"] is False for endpoint in risk["endpoints"])
    assert flink["volumes"][0]["name"] == "flink-sim-state"
    for container in (submitter, scorer):
        assert all(
            secret["snowflakeSecret"].endswith(
                "KAFKA_ADAPTER_SIM_CREDENTIALS"
            )
            for secret in container["secrets"]
        )
    assert "poker.hands.raw.v1" not in flink_text.replace(
        "poker.sim.hands.raw.v1", ""
    )
    assert "poker.pair-features.v1" not in risk_text.replace(
        "poker.sim.pair-features.v1", ""
    )
    assert "__" not in flink_text + risk_text
