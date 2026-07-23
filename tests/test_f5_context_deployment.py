from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from infra.snowflake import deploy


def test_canonical_flink_render_uses_internal_snowflake_context(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
    )
    deploy.validate_rendered_catalog()

    text = (tmp_path / "flink.yaml").read_text()
    spec = yaml.safe_load(text)["spec"]
    containers = {container["name"]: container for container in spec["containers"]}
    taskmanager = containers["taskmanager"]
    context_proxy = containers["context-proxy"]
    submitter = containers["submitter"]

    assert submitter["env"]["FLINK_CONTEXT_SOURCE"] == "snowflake"
    assert submitter["env"]["USER_CONTEXT_SNOWFLAKE_PROXY_URL"] == (
        "http://127.0.0.1:8090"
    )
    assert submitter["env"]["USER_CONTEXT_SNOWFLAKE_TABLE"] == (
        "POKER_ML_DEMO.SPCS.POKER_USER_CONTEXT_HISTORY"
    )
    assert submitter["env"]["KAFKA_WORLD_HANDS_TOPIC"] == (
        "poker.synthetic.hands.raw.v1"
    )
    assert submitter["env"]["KAFKA_PLAYER_CONTEXT_V2_TOPIC"] == (
        "poker.synthetic.hand-player-context.v2"
    )
    assert submitter["env"]["KAFKA_PAIR_FEATURES_V2_TOPIC"] == (
        "poker.synthetic.pair-features.context-v2.v1"
    )
    assert submitter["env"]["FLINK_PLAYER_CONTEXT_SCHEMA_VERSION"] == "2"
    assert "KAFKA_USER_CONTEXT_TOPIC" not in submitter["env"]
    assert "USER_CONTEXT_JDBC_URL" not in submitter["env"]
    assert "USER_CONTEXT_DB_USER" not in text
    assert "USER_CONTEXT_DB_PASSWORD" not in text
    assert "CONTEXT_DB_CREDENTIALS" not in text
    assert context_proxy["command"] == ["/opt/context-proxy/venv/bin/python"]
    assert context_proxy["args"] == ["/opt/context-proxy/server.py"]
    assert context_proxy["env"]["SNOWFLAKE_WAREHOUSE"] == "DEMO_WH"
    assert context_proxy["readinessProbe"] == {
        "port": 8090,
        "path": "/healthz",
    }
    assert all(
        endpoint["port"] != 8090 for endpoint in spec["endpoints"]
    )
    assert "secrets" not in context_proxy
    assert "secrets" not in taskmanager
    assert "secrets" not in containers["jobmanager"]
    assert {
        item["snowflakeSecret"] for item in submitter["secrets"]
    } == {"POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS"}


def test_catalog_declares_no_external_context_access() -> None:
    flink = deploy._load_service_catalog()["POKER_FLINK"]

    assert flink["external_access_integrations"] == (
        "POKER_FLINK_KAFKA_EAI",
    )
    assert flink["secret_references"] == {
        "submitter": ("POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS",)
    }


def _write_test_catalog(path: Path) -> None:
    path.write_text(
        """
version: 1
database: POKER_ML_DEMO
schema: SPCS
services:
  POKER_FLINK:
    spec: flink.yaml
    compute_pool: POKER_ML_CPU_POOL
    external_access_integrations:
      - POKER_FLINK_KAFKA_EAI
    secret_references:
      submitter:
        - POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS
""".lstrip()
    )


def _test_flink_spec() -> str:
    return """
spec:
  containers:
  - name: taskmanager
    image: /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:test
    env:
      FLINK_CONTEXT_SOURCE: snowflake
  - name: submitter
    image: /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:test
    secrets:
    - snowflakeSecret: POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS
      secretKeyRef: username
      envVarName: KAFKA_SASL_USERNAME
""".lstrip()


def test_deploy_reconciles_kafka_only_eai_and_reads_it_back(
    monkeypatch, tmp_path: Path
) -> None:
    rendered = tmp_path / "rendered"
    rendered.mkdir()
    spec = _test_flink_spec()
    (rendered / "flink.yaml").write_text(spec)
    catalog = tmp_path / "services.yaml"
    _write_test_catalog(catalog)

    class Warehouse:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

        def fetch_df(self, statement: str) -> pd.DataFrame:
            assert statement == (
                "DESCRIBE SERVICE POKER_ML_DEMO.SPCS.POKER_FLINK"
            )
            return pd.DataFrame(
                [
                    {
                        "name": "POKER_FLINK",
                        "spec": spec,
                        "external_access_integrations": json.dumps(
                            ["POKER_FLINK_KAFKA_EAI"]
                        ),
                        "spec_digest": "digest-1",
                        "status": "RUNNING",
                        "is_upgrading": False,
                    }
                ]
            )

        def close(self) -> None:
            return None

    warehouse = Warehouse()
    monkeypatch.setattr(deploy, "RENDERED_DIR", rendered)
    monkeypatch.setattr(deploy, "SERVICE_CATALOG", catalog)
    monkeypatch.setattr(deploy, "_warehouse", lambda: warehouse)

    deploy.deploy_service(
        "POKER_FLINK",
        "flink.yaml",
        external_access_integrations=("POKER_FLINK_KAFKA_EAI",),
    )

    eai_clause = (
        "EXTERNAL_ACCESS_INTEGRATIONS = (POKER_FLINK_KAFKA_EAI)"
    )
    assert eai_clause in warehouse.statements[3]
    assert eai_clause in warehouse.statements[4]
    assert "ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_FLINK FROM SPECIFICATION" in (
        warehouse.statements[5]
    )


def test_service_readback_rejects_eai_or_secret_drift() -> None:
    declared = {
        "external_access_integrations": ("POKER_FLINK_KAFKA_EAI",),
        "secret_references": {
            "submitter": ("POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS",),
        },
    }
    with pytest.raises(SystemExit, match="EAI readback drift"):
        deploy._assert_service_matches_catalog(
            "POKER_FLINK",
            {
                "external_access_integrations": (
                    "POKER_FLINK_KAFKA_EAI",
                    "POKER_FLINK_CONTEXT_DB_EAI",
                ),
                "secret_references": declared["secret_references"],
            },
            declared,
        )
    with pytest.raises(SystemExit, match="secret-reference readback drift"):
        deploy._assert_service_matches_catalog(
            "POKER_FLINK",
            {
                "external_access_integrations": declared[
                    "external_access_integrations"
                ],
                "secret_references": {},
            },
            declared,
        )
