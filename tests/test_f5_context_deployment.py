from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
import yaml

from infra.snowflake import deploy


JDBC_URL = "jdbc:postgresql://context-db.example.com:5432/poker?sslmode=require"


def test_context_db_plan_has_exact_non_secret_resources() -> None:
    plan = deploy.context_db_access_plan(JDBC_URL)

    assert plan == {
        "service": "POKER_ML_DEMO.SPCS.POKER_FLINK",
        "jdbc_url": JDBC_URL,
        "network_rule": (
            "POKER_ML_DEMO.SPCS.POKER_FLINK_CONTEXT_DB_EGRESS_RULE"
        ),
        "network_endpoint": "context-db.example.com:5432",
        "secret": "POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS",
        "external_access_integration": "POKER_FLINK_CONTEXT_DB_EAI",
        "secret_container": "taskmanager",
    }
    assert "password" not in json.dumps(plan).lower()


@pytest.mark.parametrize(
    "value",
    [
        "",
        "postgresql://context-db.example.com:5432/poker",
        "jdbc:postgresql://context-db.example.com/poker",
        "jdbc:postgresql://user:password@context-db.example.com:5432/poker",
        "jdbc:postgresql://context-db.example.com:5432/",
        "jdbc:postgresql://context-db.example.com:5432/poker/extra",
        "jdbc:postgresql://context-db.example.com:5432/poker?password=secret",
        'jdbc:postgresql://context-db.example.com:5432/poker"\nenv: injected',
    ],
)
def test_context_db_plan_rejects_unsafe_jdbc_urls(value: str) -> None:
    with pytest.raises(SystemExit):
        deploy.context_db_access_plan(value)


def test_configure_context_db_creates_narrow_resources(monkeypatch) -> None:
    class Warehouse:
        def __init__(self) -> None:
            self.statements: list[str] = []
            self.closed = False

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

        def close(self) -> None:
            self.closed = True

    warehouse = Warehouse()
    monkeypatch.setenv("USER_CONTEXT_JDBC_URL", JDBC_URL)
    monkeypatch.setenv("USER_CONTEXT_DB_USER", "context-user")
    monkeypatch.setenv("USER_CONTEXT_DB_PASSWORD", "context-password")
    monkeypatch.setattr(deploy, "_warehouse", lambda: warehouse)

    deploy.configure_flink_context_db()

    joined = "\n".join(warehouse.statements)
    assert (
        "CREATE OR REPLACE NETWORK RULE POKER_FLINK_CONTEXT_DB_EGRESS_RULE "
        "MODE = EGRESS TYPE = HOST_PORT "
        "VALUE_LIST = ('context-db.example.com:5432')"
    ) in joined
    assert "CREATE OR REPLACE SECRET CONTEXT_DB_CREDENTIALS TYPE = PASSWORD" in joined
    assert "USERNAME = 'context-user' PASSWORD = 'context-password'" in joined
    assert (
        "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION "
        "POKER_FLINK_CONTEXT_DB_EAI"
    ) in joined
    assert "POKER_ML_DEMO.SPCS.POKER_FLINK_CONTEXT_DB_EGRESS_RULE" in joined
    assert (
        "GRANT READ ON SECRET POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS "
        "TO ROLE SYSADMIN"
    ) in joined
    assert "ALLOWED_AUTHENTICATION_SECRETS" not in joined
    assert warehouse.closed is True


def test_canonical_flink_render_is_jdbc_v2_and_secret_safe(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        user_context_jdbc_url=JDBC_URL,
    )
    deploy.validate_rendered_catalog()

    text = (tmp_path / "flink.yaml").read_text()
    spec = yaml.safe_load(text)["spec"]
    containers = {container["name"]: container for container in spec["containers"]}
    taskmanager = containers["taskmanager"]
    submitter = containers["submitter"]

    assert submitter["env"]["FLINK_CONTEXT_SOURCE"] == "jdbc"
    assert submitter["env"]["USER_CONTEXT_JDBC_URL"] == JDBC_URL
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
    assert "KAFKA_PLAYER_CONTEXT_TOPIC" not in submitter["env"]
    assert "KAFKA_PAIR_FEATURES_TOPIC" not in submitter["env"]

    taskmanager_secret_names = {
        item["snowflakeSecret"] for item in taskmanager["secrets"]
    }
    submitter_secret_names = {
        item["snowflakeSecret"] for item in submitter["secrets"]
    }
    assert taskmanager_secret_names == {
        "POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS"
    }
    assert submitter_secret_names == {"POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS"}
    assert "secrets" not in containers["jobmanager"]
    assert "CONTEXT_DB_CREDENTIALS" not in yaml.safe_dump(submitter)
    assert "context-user" not in text
    assert "context-password" not in text


def test_render_without_jdbc_url_removes_stale_canonical_flink_spec(
    monkeypatch, tmp_path: Path
) -> None:
    templates = tmp_path / "templates"
    rendered = tmp_path / "rendered"
    templates.mkdir()
    rendered.mkdir()
    (templates / "flink.yaml.template").write_text(
        "url: __USER_CONTEXT_JDBC_URL__\n"
    )
    stale = rendered / "flink.yaml"
    stale.write_text("url: stale\n")
    monkeypatch.setattr(deploy, "SPECS_DIR", templates)
    monkeypatch.setattr(deploy, "RENDERED_DIR", rendered)

    deploy.render_specs(deploy.DEFAULT_IMAGE_PATH, None)

    assert not stale.exists()


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
      - POKER_FLINK_CONTEXT_DB_EAI
    secret_references:
      taskmanager:
        - POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS
      submitter:
        - POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS
""".lstrip()
    )


def _test_flink_spec(*, context_secret: str | None = None) -> str:
    context_secret = (
        context_secret or "POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS"
    )
    return f"""
spec:
  containers:
  - name: taskmanager
    image: /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:test
    secrets:
    - snowflakeSecret: {context_secret}
      secretKeyRef: username
      envVarName: USER_CONTEXT_DB_USER
    - snowflakeSecret: {context_secret}
      secretKeyRef: password
      envVarName: USER_CONTEXT_DB_PASSWORD
  - name: submitter
    image: /POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:test
    secrets:
    - snowflakeSecret: POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS
      secretKeyRef: username
      envVarName: KAFKA_SASL_USERNAME
""".lstrip()


def test_deploy_reconciles_complete_eai_set_and_reads_it_back(
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
                            [
                                "POKER_FLINK_CONTEXT_DB_EAI",
                                "POKER_FLINK_KAFKA_EAI",
                            ]
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
        external_access_integrations=(
            "POKER_FLINK_KAFKA_EAI",
            "POKER_FLINK_CONTEXT_DB_EAI",
        ),
    )

    joined = "\n".join(warehouse.statements)
    eai_clause = (
        "EXTERNAL_ACCESS_INTEGRATIONS = "
        "(POKER_FLINK_KAFKA_EAI, POKER_FLINK_CONTEXT_DB_EAI)"
    )
    assert eai_clause in warehouse.statements[3]
    assert eai_clause in warehouse.statements[4]
    assert "ALTER SERVICE POKER_ML_DEMO.SPCS.POKER_FLINK FROM SPECIFICATION" in (
        warehouse.statements[5]
    )
    assert joined.index(" SET EXTERNAL_ACCESS_INTEGRATIONS") < joined.index(
        " FROM SPECIFICATION", joined.index("ALTER SERVICE")
    )


def test_service_readback_rejects_eai_or_secret_drift() -> None:
    declared = {
        "external_access_integrations": (
            "POKER_FLINK_KAFKA_EAI",
            "POKER_FLINK_CONTEXT_DB_EAI",
        ),
        "secret_references": {
            "taskmanager": (
                "POKER_ML_DEMO.SPCS.CONTEXT_DB_CREDENTIALS",
            ),
            "submitter": ("POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS",),
        },
    }
    with pytest.raises(SystemExit, match="EAI readback drift"):
        deploy._assert_service_matches_catalog(
            "POKER_FLINK",
            {
                "external_access_integrations": ("POKER_FLINK_KAFKA_EAI",),
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
