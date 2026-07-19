"""Provision and deploy the Snowpark Container Services demo slice.

Image building and pushing remain separate because registry authentication is
handled by the Snowflake CLI and Docker.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent / "sql"
SPECS_DIR = Path(__file__).resolve().parent / "specs"
RENDERED_DIR = Path(__file__).resolve().parent / "rendered"

DEFAULT_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-pipeline:dev"
POOL = "POKER_ML_CPU_POOL"
DATABASE = "POKER_ML_DEMO"
SCHEMA = "SPCS"

_HOST = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?:[0-9]{2,5}$")
_IMAGE = re.compile(
    r"^/[A-Za-z_][A-Za-z0-9_$]*/[A-Za-z_][A-Za-z0-9_$]*/"
    r"[A-Za-z_][A-Za-z0-9_$]*/[A-Za-z0-9._-]+:[A-Za-z0-9._-]+$"
)


def _warehouse():
    sys.path.insert(0, str(ROOT))
    from pipeline.warehouse import get_warehouse

    return get_warehouse()


def _sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _statements(text: str) -> list[str]:
    return [statement.strip() for statement in text.split(";") if statement.strip()]


def bootstrap() -> None:
    wh = _warehouse()
    try:
        for statement in _statements((SQL_DIR / "bootstrap.sql").read_text()):
            wh.execute(statement)
            first_line = next(
                (line for line in statement.splitlines() if not line.lstrip().startswith("--")),
                statement,
            )
            print(f"[snowflake] {first_line[:100]}")
    finally:
        wh.close()


def _parse_brokers(raw: str) -> list[str]:
    brokers = [item.strip() for item in raw.split(",") if item.strip()]
    if not brokers:
        raise SystemExit("KAFKA_BOOTSTRAP_SERVERS must contain at least one host:port")
    for broker in brokers:
        if not _HOST.fullmatch(broker):
            raise SystemExit(f"Invalid Kafka broker host:port: {broker!r}")
        port = int(broker.rsplit(":", 1)[1])
        if port not in {22, 80, 443} and port < 1024:
            raise SystemExit(
                f"Kafka port {port} is not allowed by Snowpark Container Services"
            )
    return brokers


def _configured_kafka() -> tuple[str, str | None, str | None]:
    from pipeline.config import get_settings

    settings = get_settings()
    return (
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", settings.kafka_bootstrap_servers),
        os.environ.get("KAFKA_SASL_USERNAME", settings.kafka_sasl_username),
        os.environ.get("KAFKA_SASL_PASSWORD", settings.kafka_sasl_password),
    )


def _configured_kafka_egress(raw_bootstrap_servers: str) -> str:
    """Return every broker endpoint that the Snowflake service may contact.

    Managed Kafka bootstrap endpoints commonly advertise separate broker
    hostnames after the initial metadata request. Those hostnames must also be
    present in the Snowflake egress network rule.
    """
    from pipeline.config import get_settings

    settings = get_settings()
    return (
        os.environ.get("KAFKA_EGRESS_BROKERS")
        or settings.kafka_egress_brokers
        or raw_bootstrap_servers
    )


def configure_kafka() -> None:
    raw_brokers, username, password = _configured_kafka()
    bootstrap_brokers = _parse_brokers(raw_brokers)
    brokers = _parse_brokers(_configured_kafka_egress(raw_brokers))
    if not username or not password:
        raise SystemExit(
            "Set KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD in the shell. "
            "They are stored as a Snowflake Secret and never written to a spec file."
        )

    value_list = ", ".join(_sql_string(broker) for broker in brokers)
    wh = _warehouse()
    try:
        statements = [
            "USE ROLE SYSADMIN",
            f"USE DATABASE {DATABASE}",
            f"USE SCHEMA {SCHEMA}",
            (
                "CREATE OR REPLACE NETWORK RULE KAFKA_EGRESS_RULE "
                "MODE = EGRESS TYPE = HOST_PORT "
                f"VALUE_LIST = ({value_list})"
            ),
            (
                "CREATE OR REPLACE SECRET KAFKA_CREDENTIALS TYPE = PASSWORD "
                f"USERNAME = {_sql_string(username)} PASSWORD = {_sql_string(password)}"
            ),
            "USE ROLE ACCOUNTADMIN",
            (
                "CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION POKER_KAFKA_EAI "
                "ALLOWED_NETWORK_RULES = "
                "(POKER_ML_DEMO.SPCS.KAFKA_EGRESS_RULE) ENABLED = TRUE"
            ),
            "GRANT USAGE ON INTEGRATION POKER_KAFKA_EAI TO ROLE SYSADMIN",
            "USE ROLE SYSADMIN",
            (
                "GRANT READ ON SECRET POKER_ML_DEMO.SPCS.KAFKA_CREDENTIALS "
                "TO ROLE SYSADMIN"
            ),
        ]
        for statement in statements:
            wh.execute(statement)
        print(
            "[snowflake] Kafka egress and secret configured for: "
            + ", ".join(brokers)
        )
        print("[snowflake] Kafka bootstrap servers: " + ", ".join(bootstrap_brokers))
    finally:
        wh.close()


def render_specs(image_path: str, kafka_bootstrap_servers: str | None) -> None:
    if not _IMAGE.fullmatch(image_path):
        raise SystemExit(
            "Image path must look like /DATABASE/SCHEMA/REPOSITORY/image:tag"
        )
    RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    for template_path in sorted(SPECS_DIR.glob("*.yaml.template")):
        text = template_path.read_text().replace("__IMAGE_PATH__", image_path)
        if "__KAFKA_BOOTSTRAP_SERVERS__" in text:
            if not kafka_bootstrap_servers:
                print(f"[render] skipped {template_path.name}: Kafka brokers not set")
                continue
            brokers = _parse_brokers(kafka_bootstrap_servers)
            text = text.replace("__KAFKA_BOOTSTRAP_SERVERS__", ",".join(brokers))
        output = RENDERED_DIR / template_path.name.removesuffix(".template")
        output.write_text(text)
        display_path = output.relative_to(ROOT) if output.is_relative_to(ROOT) else output
        print(f"[render] {display_path}")


def _read_rendered(name: str) -> str:
    path = RENDERED_DIR / name
    if not path.is_file():
        raise SystemExit(f"Missing {path}. Run the render command first.")
    spec = path.read_text()
    if "__" in spec:
        raise SystemExit(f"Unresolved placeholder in {path}")
    return spec


def deploy_service(name: str, spec_name: str, *, kafka_eai: bool = False) -> None:
    spec = _read_rendered(spec_name)
    eai = " EXTERNAL_ACCESS_INTEGRATIONS = (POKER_KAFKA_EAI)" if kafka_eai else ""
    wh = _warehouse()
    try:
        wh.execute("USE ROLE SYSADMIN")
        wh.execute(f"USE DATABASE {DATABASE}")
        wh.execute(f"USE SCHEMA {SCHEMA}")
        create_sql = (
            f"CREATE SERVICE IF NOT EXISTS {name} IN COMPUTE POOL {POOL} "
            f"FROM SPECIFICATION $${spec}$${eai} AUTO_RESUME = TRUE "
            "MIN_INSTANCES = 1 MAX_INSTANCES = 1 QUERY_WAREHOUSE = DEMO_WH"
        )
        wh.execute(create_sql)
        wh.execute(f"ALTER SERVICE {name} FROM SPECIFICATION $${spec}$$")
        print(f"[snowflake] service submitted: {DATABASE}.{SCHEMA}.{name}")
    finally:
        wh.close()


def run_training_job(async_: bool) -> None:
    spec = _read_rendered("train-job.yaml")
    async_sql = "TRUE" if async_ else "FALSE"
    wh = _warehouse()
    try:
        wh.execute("USE ROLE SYSADMIN")
        wh.execute(f"USE DATABASE {DATABASE}")
        wh.execute(f"USE SCHEMA {SCHEMA}")
        wh.execute(
            f"EXECUTE JOB SERVICE IN COMPUTE POOL {POOL} "
            "NAME = POKER_TRAIN_JOB "
            f"ASYNC = {async_sql} QUERY_WAREHOUSE = DEMO_WH "
            f"FROM SPECIFICATION $${spec}$$"
        )
        print(f"[snowflake] training job submitted (async={async_sql})")
    finally:
        wh.close()


def set_admin_state(state: str) -> None:
    wh = _warehouse()
    try:
        wh.execute("USE ROLE SYSADMIN")
        wh.execute(f"ALTER SERVICE {DATABASE}.{SCHEMA}.POKER_ADMIN {state}")
        print(f"[snowflake] POKER_ADMIN {state.lower()}")
    finally:
        wh.close()


def status() -> None:
    wh = _warehouse()
    try:
        cursor = wh.conn.cursor()  # type: ignore[attr-defined]
        try:
            for label, statement in [
                ("compute pools", "SHOW COMPUTE POOLS LIKE 'POKER_ML_CPU_POOL'"),
                (
                    "images",
                    "SHOW IMAGES IN IMAGE REPOSITORY "
                    "POKER_ML_DEMO.SPCS.POKER_ML_REPO",
                ),
                ("services", "SHOW SERVICES IN SCHEMA POKER_ML_DEMO.SPCS"),
            ]:
                cursor.execute(statement)
                print(f"[{label}]")
                for row in cursor.fetchall():
                    print(row)
        finally:
            cursor.close()
    finally:
        wh.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("bootstrap")
    sub.add_parser("configure-kafka")

    render = sub.add_parser("render")
    render.add_argument(
        "--image-path", default=os.environ.get("SPCS_IMAGE_PATH", DEFAULT_IMAGE_PATH)
    )
    render.add_argument(
        "--kafka-bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    )

    sub.add_parser("deploy-admin")
    sub.add_parser("suspend-admin")
    sub.add_parser("resume-admin")
    sub.add_parser("deploy-realtime")
    train = sub.add_parser("run-training-job")
    train.add_argument("--sync", action="store_true")
    sub.add_parser("status")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap()
    elif args.command == "configure-kafka":
        configure_kafka()
    elif args.command == "render":
        kafka_bootstrap_servers = args.kafka_bootstrap_servers
        if not kafka_bootstrap_servers:
            configured_brokers, username, password = _configured_kafka()
            if username and password:
                kafka_bootstrap_servers = configured_brokers
        render_specs(args.image_path, kafka_bootstrap_servers)
    elif args.command == "deploy-admin":
        deploy_service("POKER_ADMIN", "admin.yaml")
    elif args.command == "suspend-admin":
        set_admin_state("SUSPEND")
    elif args.command == "resume-admin":
        set_admin_state("RESUME")
    elif args.command == "deploy-realtime":
        deploy_service("POKER_REALTIME", "realtime.yaml", kafka_eai=True)
    elif args.command == "run-training-job":
        run_training_job(async_=not args.sync)
    elif args.command == "status":
        status()


if __name__ == "__main__":
    main()
