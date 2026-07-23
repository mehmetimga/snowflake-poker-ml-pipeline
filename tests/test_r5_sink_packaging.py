from __future__ import annotations

from pathlib import Path

import yaml

from infra.snowflake import deploy


ROOT = Path(__file__).resolve().parents[1]


def test_sink_spec_is_private_and_uses_writer_sidecar(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(deploy, "RENDERED_DIR", tmp_path)
    deploy.render_specs(
        deploy.DEFAULT_IMAGE_PATH,
        "broker.example.com:9092",
        sink_image_path=(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-sink:0123456789ab"
        ),
        sink_build_version="0123456789ab",
        sink_allowed_tenants="demo",
        sink_group_id="poker-snowflake-sink-synthetic-v1",
    )

    document = yaml.safe_load((tmp_path / "sink.yaml").read_text())
    spec = document["spec"]
    assert [container["name"] for container in spec["containers"]] == [
        "sink",
        "snowflake-writer",
    ]
    assert all(
        container["image"].endswith("poker-sink:0123456789ab")
        for container in spec["containers"]
    )
    sink, writer = spec["containers"]
    assert sink["env"]["SINK_FROM_BEGINNING"] == "true"
    assert sink["env"]["SNOWFLAKE_EVENT_WRITER_URL"] == "http://127.0.0.1:8091"
    assert len(sink["secrets"]) == 2
    assert "secrets" not in writer
    assert writer["command"] == [
        "/opt/snowflake-event-writer/venv/bin/python"
    ]
    assert spec["endpoints"] == [
        {
            "name": "sink-metrics",
            "port": 9094,
            "protocol": "HTTP",
            "public": False,
        }
    ]
    assert "__" not in (tmp_path / "sink.yaml").read_text()


def test_sink_catalog_declares_only_kafka_egress_and_sink_secret() -> None:
    catalog = yaml.safe_load(
        (ROOT / "infra/snowflake/services.yaml").read_text()
    )["services"]["POKER_SINK"]
    assert catalog["spec"] == "sink.yaml"
    assert catalog["external_access_integrations"] == ["POKER_KAFKA_EAI"]
    assert catalog["secret_references"] == {
        "sink": ["POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS"]
    }


def test_admin_spec_selects_canonical_reader() -> None:
    admin = yaml.safe_load(
        (ROOT / "infra/snowflake/specs/admin.yaml.template").read_text()
    )["spec"]["containers"][0]
    assert admin["env"]["ADMIN_DATA_MODE"] == "canonical"
    assert admin["env"]["SNOWFLAKE_SCHEMA"] == "PUBLIC"


def test_sink_sql_defines_ledger_native_tables_and_admin_views() -> None:
    sql = (ROOT / "infra/snowflake/sql/sink.sql").read_text()
    expected = {
        "POKER_EVENT_ENVELOPES",
        "POKER_HAND_EVENTS",
        "POKER_PLAYER_CONTEXT_EVENTS",
        "POKER_PAIR_FEATURE_EVENTS_V2",
        "POKER_RISK_SCORE_EVENTS",
        "POKER_RULE_EVIDENCE_EVENTS_V2",
        "POKER_REVIEW_DECISION_EVENTS",
        "POKER_RISK_ALERT_EVENTS",
        "POKER_SINK_DEAD_LETTERS",
        "POKER_ALERT_REVIEW_V",
        "POKER_SINK_TOPIC_PROGRESS_V",
    }
    assert all(name in sql for name in expected)
    assert "raw_value" not in sql.lower()
    assert "event_sha256" in sql


def test_sink_dockerfile_pins_runtime_and_contains_both_processes() -> None:
    dockerfile = (ROOT / "Dockerfile.sink").read_text()
    assert "ARG GO_VERSION=1.23.8" in dockerfile
    assert "debian:bookworm-slim@sha256:" in dockerfile
    assert "go build -trimpath" in dockerfile
    assert "./cmd/sink-kafka" in dockerfile
    assert "/opt/snowflake-event-writer/server.py" in dockerfile
    assert "snowflake-connector-python==4.5.0" not in dockerfile
    assert "USER 65532:65532" in dockerfile
    assert ":latest" not in dockerfile


def test_admin_image_is_revisioned_and_base_digest_pinned() -> None:
    dockerfile = (ROOT / "Dockerfile.spcs").read_text()
    assert "FROM python:3.11-slim@sha256:" in dockerfile
    assert "ARG BUILD_VERSION=dev" in dockerfile
    assert 'org.opencontainers.image.revision="${BUILD_VERSION}"' in dockerfile


def test_bootstrap_sink_executes_only_sink_ddl(monkeypatch) -> None:
    class Warehouse:
        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute(self, statement: str) -> None:
            self.statements.append(statement)

        def close(self) -> None:
            return None

    warehouse = Warehouse()
    monkeypatch.setattr(deploy, "_warehouse", lambda: warehouse)
    deploy.bootstrap_sink()

    joined = "\n".join(warehouse.statements)
    assert "CREATE TABLE IF NOT EXISTS POKER_EVENT_ENVELOPES" in joined
    assert "CREATE OR REPLACE VIEW POKER_ALERT_REVIEW_V" in joined
    assert "CREATE SERVICE" not in joined
