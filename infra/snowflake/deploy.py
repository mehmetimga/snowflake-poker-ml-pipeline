"""Provision and deploy the Snowpark Container Services demo slice.

Image building and pushing remain separate because registry authentication is
handled by the Snowflake CLI and Docker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys


ROOT = Path(__file__).resolve().parents[2]
SQL_DIR = Path(__file__).resolve().parent / "sql"
SPECS_DIR = Path(__file__).resolve().parent / "specs"
RENDERED_DIR = Path(__file__).resolve().parent / "rendered"

DEFAULT_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-pipeline:dev"
DEFAULT_RISK_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-risk:dev"
DEFAULT_FLINK_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:dev"
DEFAULT_ADAPTER_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-adapter:dev"
DEFAULT_TRITON_IMAGE_PATH = "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/tritonserver:25.12-py3"
DEFAULT_MODEL_RUN_ID = "pair_7a1c58c1046b"
DEFAULT_RISK_SCORER_GROUP_ID = "poker-go-risk-scorer-v1"
DEFAULT_ADAPTER_GROUP_ID = "poker-go-hand-adapter-sim-v1"
DEFAULT_ADAPTER_DATASET_ID = "sim-cdc-v1"
POOL = "POKER_ML_CPU_POOL"
DATABASE = "POKER_ML_DEMO"
SCHEMA = "SPCS"
ADAPTER_SIM_KAFKA_EAI = "POKER_ADAPTER_SIM_KAFKA_EAI"

_HOST = re.compile(r"^[A-Za-z0-9](?:[A-Za-z0-9.-]*[A-Za-z0-9])?:[0-9]{2,5}$")
_IMAGE = re.compile(
    r"^/[A-Za-z_][A-Za-z0-9_$]*/[A-Za-z_][A-Za-z0-9_$]*/"
    r"[A-Za-z_][A-Za-z0-9_$]*/[A-Za-z0-9._-]+:[A-Za-z0-9._-]+$"
)
_SAFE_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_SAVEPOINT_URI = re.compile(
    r"^file:/+opt/flink/state/savepoints/savepoint-[A-Za-z0-9_-]+$"
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
                (
                    line
                    for line in statement.splitlines()
                    if not line.lstrip().startswith("--")
                ),
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
    advertised = (
        os.environ.get("KAFKA_EGRESS_BROKERS") or settings.kafka_egress_brokers or ""
    )
    combined = [
        item.strip()
        for item in f"{raw_bootstrap_servers},{advertised}".split(",")
        if item.strip()
    ]
    return ",".join(dict.fromkeys(combined))


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
            "[snowflake] Kafka egress and secret configured for: " + ", ".join(brokers)
        )
        print("[snowflake] Kafka bootstrap servers: " + ", ".join(bootstrap_brokers))
    finally:
        wh.close()


def configure_adapter_sim_kafka(*, allow_shared_credentials: bool = False) -> None:
    """Configure an isolated EAI and Secret for the synthetic C2 adapter."""
    raw_brokers, shared_username, shared_password = _configured_kafka()
    bootstrap_brokers = _parse_brokers(raw_brokers)
    brokers = _parse_brokers(_configured_kafka_egress(raw_brokers))
    username = os.environ.get("KAFKA_ADAPTER_SIM_SASL_USERNAME")
    password = os.environ.get("KAFKA_ADAPTER_SIM_SASL_PASSWORD")
    if bool(username) != bool(password):
        raise SystemExit(
            "Set both KAFKA_ADAPTER_SIM_SASL_USERNAME and "
            "KAFKA_ADAPTER_SIM_SASL_PASSWORD"
        )
    shared = False
    if not username and allow_shared_credentials:
        username, password = shared_username, shared_password
        shared = True
    if not username or not password:
        raise SystemExit(
            "Set dedicated KAFKA_ADAPTER_SIM_SASL_USERNAME and "
            "KAFKA_ADAPTER_SIM_SASL_PASSWORD. For a bounded demo only, pass "
            "--allow-shared-credentials to copy the configured Kafka principal."
        )

    value_list = ", ".join(_sql_string(broker) for broker in brokers)
    wh = _warehouse()
    try:
        statements = [
            "USE ROLE SYSADMIN",
            f"USE DATABASE {DATABASE}",
            f"USE SCHEMA {SCHEMA}",
            (
                "CREATE OR REPLACE NETWORK RULE KAFKA_ADAPTER_SIM_EGRESS_RULE "
                "MODE = EGRESS TYPE = HOST_PORT "
                f"VALUE_LIST = ({value_list})"
            ),
            (
                "CREATE OR REPLACE SECRET KAFKA_ADAPTER_SIM_CREDENTIALS "
                "TYPE = PASSWORD "
                f"USERNAME = {_sql_string(username)} PASSWORD = {_sql_string(password)}"
            ),
            "USE ROLE ACCOUNTADMIN",
            (
                f"CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION {ADAPTER_SIM_KAFKA_EAI} "
                "ALLOWED_NETWORK_RULES = "
                "(POKER_ML_DEMO.SPCS.KAFKA_ADAPTER_SIM_EGRESS_RULE) ENABLED = TRUE"
            ),
            (
                f"GRANT USAGE ON INTEGRATION {ADAPTER_SIM_KAFKA_EAI} "
                "TO ROLE SYSADMIN"
            ),
            "USE ROLE SYSADMIN",
            (
                "GRANT READ ON SECRET "
                "POKER_ML_DEMO.SPCS.KAFKA_ADAPTER_SIM_CREDENTIALS "
                "TO ROLE SYSADMIN"
            ),
        ]
        for statement in statements:
            wh.execute(statement)
        mode = "shared demo principal" if shared else "dedicated principal"
        print(
            "[snowflake] isolated adapter Kafka access configured "
            f"mode={mode} egress={','.join(brokers)}"
        )
        print("[snowflake] Kafka bootstrap servers: " + ", ".join(bootstrap_brokers))
    finally:
        wh.close()


def render_specs(
    image_path: str,
    kafka_bootstrap_servers: str | None,
    *,
    risk_image_path: str = DEFAULT_RISK_IMAGE_PATH,
    flink_image_path: str = DEFAULT_FLINK_IMAGE_PATH,
    adapter_image_path: str = DEFAULT_ADAPTER_IMAGE_PATH,
    triton_image_path: str = DEFAULT_TRITON_IMAGE_PATH,
    build_version: str = "dev",
    risk_build_version: str | None = None,
    flink_build_version: str | None = None,
    adapter_build_version: str | None = None,
    model_run_id: str = DEFAULT_MODEL_RUN_ID,
    allowed_tenants: str = "demo",
    risk_scorer_group_id: str = DEFAULT_RISK_SCORER_GROUP_ID,
    adapter_group_id: str = DEFAULT_ADAPTER_GROUP_ID,
    adapter_dataset_id: str = DEFAULT_ADAPTER_DATASET_ID,
    adapter_allowed_tenants: str = "demo",
    flink_context_savepoint_path: str = "",
    flink_pair_savepoint_path: str = "",
) -> None:
    image_paths = {
        "application": image_path,
        "risk": risk_image_path,
        "flink": flink_image_path,
        "adapter": adapter_image_path,
        "triton": triton_image_path,
    }
    for label, candidate in image_paths.items():
        if not _IMAGE.fullmatch(candidate):
            raise SystemExit(
                f"{label} image path must look like "
                "/DATABASE/SCHEMA/REPOSITORY/image:tag"
            )
    risk_build_version = risk_build_version or build_version
    flink_build_version = flink_build_version or build_version
    adapter_build_version = adapter_build_version or build_version
    for label, candidate in {
        "build version": build_version,
        "risk build version": risk_build_version,
        "Flink build version": flink_build_version,
        "adapter build version": adapter_build_version,
        "model run ID": model_run_id,
        "risk scorer group ID": risk_scorer_group_id,
        "adapter group ID": adapter_group_id,
        "adapter dataset ID": adapter_dataset_id,
    }.items():
        if not _SAFE_ID.fullmatch(candidate):
            raise SystemExit(f"Invalid {label}: {candidate!r}")
    tenants = [value.strip() for value in allowed_tenants.split(",") if value.strip()]
    if not tenants or any(not _SAFE_ID.fullmatch(value) for value in tenants):
        raise SystemExit("RISK_ALLOWED_TENANTS must be a comma-separated ID allowlist")
    adapter_tenants = [
        value.strip() for value in adapter_allowed_tenants.split(",") if value.strip()
    ]
    if not adapter_tenants or any(
        not _SAFE_ID.fullmatch(value) for value in adapter_tenants
    ):
        raise SystemExit("CDC_ALLOWED_TENANTS must be a comma-separated ID allowlist")
    if not adapter_dataset_id.startswith("sim-"):
        raise SystemExit("simulation adapter dataset ID must start with sim-")
    for label, candidate in {
        "context savepoint URI": flink_context_savepoint_path,
        "pair savepoint URI": flink_pair_savepoint_path,
    }.items():
        if candidate and not _SAVEPOINT_URI.fullmatch(candidate):
            raise SystemExit(
                f"Invalid {label}: expected a savepoint below "
                "file:///opt/flink/state/savepoints"
            )

    replacements = {
        "__IMAGE_PATH__": image_path,
        "__RISK_IMAGE_PATH__": risk_image_path,
        "__FLINK_IMAGE_PATH__": flink_image_path,
        "__ADAPTER_IMAGE_PATH__": adapter_image_path,
        "__TRITON_IMAGE_PATH__": triton_image_path,
        "__RISK_BUILD_VERSION__": risk_build_version,
        "__FLINK_BUILD_VERSION__": flink_build_version,
        "__ADAPTER_BUILD_VERSION__": adapter_build_version,
        "__MODEL_RUN_ID__": model_run_id,
        "__RISK_ALLOWED_TENANTS__": ",".join(tenants),
        "__RISK_SCORER_GROUP_ID__": risk_scorer_group_id,
        "__ADAPTER_GROUP_ID__": adapter_group_id,
        "__ADAPTER_DATASET_ID__": adapter_dataset_id,
        "__ADAPTER_ALLOWED_TENANTS__": ",".join(adapter_tenants),
        "__FLINK_CONTEXT_SAVEPOINT_PATH__": flink_context_savepoint_path,
        "__FLINK_PAIR_SAVEPOINT_PATH__": flink_pair_savepoint_path,
    }
    RENDERED_DIR.mkdir(parents=True, exist_ok=True)
    for template_path in sorted(SPECS_DIR.glob("*.yaml.template")):
        text = template_path.read_text()
        for placeholder, replacement in replacements.items():
            text = text.replace(placeholder, replacement)
        if "__KAFKA_BOOTSTRAP_SERVERS__" in text:
            if not kafka_bootstrap_servers:
                print(f"[render] skipped {template_path.name}: Kafka brokers not set")
                continue
            brokers = _parse_brokers(kafka_bootstrap_servers)
            text = text.replace("__KAFKA_BOOTSTRAP_SERVERS__", ",".join(brokers))
        output = RENDERED_DIR / template_path.name.removesuffix(".template")
        output.write_text(text)
        display_path = (
            output.relative_to(ROOT) if output.is_relative_to(ROOT) else output
        )
        print(f"[render] {display_path}")


def upload_risk_bundle(bundle_dir: Path) -> None:
    bundle_dir = bundle_dir.resolve()
    manifest_path = bundle_dir / "artifact_manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(
            f"Missing {manifest_path}. Run scripts/build_risk_runtime_bundle.py first."
        )
    manifest = json.loads(manifest_path.read_text())
    run_id = str(manifest.get("run_id", ""))
    if not _SAFE_ID.fullmatch(run_id):
        raise SystemExit(f"Invalid runtime bundle run ID: {run_id!r}")
    relative_files = [Path("artifact_manifest.json")]
    relative_files.extend(Path(value) for value in manifest.get("artifacts", {}))
    for relative in relative_files:
        path = (bundle_dir / relative).resolve()
        if bundle_dir not in path.parents or not path.is_file():
            raise SystemExit(f"Unsafe or missing runtime bundle file: {relative}")
        if relative.as_posix() != "artifact_manifest.json":
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
            expected = manifest["artifacts"][relative.as_posix()]
            if actual != expected:
                raise SystemExit(f"Runtime bundle hash mismatch: {relative}")

    wh = _warehouse()
    try:
        wh.execute("USE ROLE SYSADMIN")
        wh.execute(f"USE DATABASE {DATABASE}")
        wh.execute(f"USE SCHEMA {SCHEMA}")
        for relative in relative_files:
            path = (bundle_dir / relative).resolve()
            parent = relative.parent.as_posix()
            destination = f"@MODEL_ARTIFACTS/risk/{run_id}"
            if parent != ".":
                destination += f"/{parent}"
            wh.execute(
                f"PUT {_sql_string(path.as_uri())} {destination} "
                "AUTO_COMPRESS = FALSE OVERWRITE = TRUE"
            )
        print(
            f"[snowflake] uploaded risk bundle run={run_id} "
            f"files={len(relative_files)} stage=@MODEL_ARTIFACTS/risk/{run_id}"
        )
    finally:
        wh.close()


def _read_rendered(name: str) -> str:
    path = RENDERED_DIR / name
    if not path.is_file():
        raise SystemExit(f"Missing {path}. Run the render command first.")
    spec = path.read_text()
    if "__" in spec:
        raise SystemExit(f"Unresolved placeholder in {path}")
    return spec


def deploy_service(
    name: str,
    spec_name: str,
    *,
    kafka_eai: bool | str = False,
) -> None:
    spec = _read_rendered(spec_name)
    if kafka_eai is True:
        kafka_eai = "POKER_KAFKA_EAI"
    eai = f" EXTERNAL_ACCESS_INTEGRATIONS = ({kafka_eai})" if kafka_eai else ""
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
    adapter_kafka = sub.add_parser("configure-adapter-sim-kafka")
    adapter_kafka.add_argument("--allow-shared-credentials", action="store_true")

    render = sub.add_parser("render")
    render.add_argument(
        "--image-path", default=os.environ.get("SPCS_IMAGE_PATH", DEFAULT_IMAGE_PATH)
    )
    render.add_argument(
        "--kafka-bootstrap-servers",
        default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS"),
    )
    render.add_argument(
        "--risk-image-path",
        default=os.environ.get("SPCS_RISK_IMAGE_PATH", DEFAULT_RISK_IMAGE_PATH),
    )
    render.add_argument(
        "--flink-image-path",
        default=os.environ.get("SPCS_FLINK_IMAGE_PATH", DEFAULT_FLINK_IMAGE_PATH),
    )
    render.add_argument(
        "--adapter-image-path",
        default=os.environ.get("SPCS_ADAPTER_IMAGE_PATH", DEFAULT_ADAPTER_IMAGE_PATH),
    )
    render.add_argument(
        "--triton-image-path",
        default=os.environ.get("SPCS_TRITON_IMAGE_PATH", DEFAULT_TRITON_IMAGE_PATH),
    )
    render.add_argument(
        "--build-version", default=os.environ.get("SPCS_BUILD_VERSION", "dev")
    )
    render.add_argument(
        "--risk-build-version", default=os.environ.get("SPCS_RISK_BUILD_VERSION")
    )
    render.add_argument(
        "--flink-build-version", default=os.environ.get("SPCS_FLINK_BUILD_VERSION")
    )
    render.add_argument(
        "--adapter-build-version",
        default=os.environ.get("SPCS_ADAPTER_BUILD_VERSION"),
    )
    render.add_argument(
        "--model-run-id",
        default=os.environ.get("SPCS_MODEL_RUN_ID", DEFAULT_MODEL_RUN_ID),
    )
    render.add_argument(
        "--allowed-tenants", default=os.environ.get("RISK_ALLOWED_TENANTS", "demo")
    )
    render.add_argument(
        "--risk-scorer-group-id",
        default=os.environ.get(
            "SPCS_RISK_SCORER_GROUP_ID", DEFAULT_RISK_SCORER_GROUP_ID
        ),
    )
    render.add_argument(
        "--adapter-group-id",
        default=os.environ.get("SPCS_ADAPTER_GROUP_ID", DEFAULT_ADAPTER_GROUP_ID),
    )
    render.add_argument(
        "--adapter-dataset-id",
        default=os.environ.get("SPCS_ADAPTER_DATASET_ID", DEFAULT_ADAPTER_DATASET_ID),
    )
    render.add_argument(
        "--adapter-allowed-tenants",
        default=os.environ.get("SPCS_ADAPTER_ALLOWED_TENANTS", "demo"),
    )
    render.add_argument(
        "--flink-context-savepoint-path",
        default=os.environ.get("SPCS_FLINK_CONTEXT_SAVEPOINT_PATH", ""),
    )
    render.add_argument(
        "--flink-pair-savepoint-path",
        default=os.environ.get("SPCS_FLINK_PAIR_SAVEPOINT_PATH", ""),
    )

    sub.add_parser("deploy-admin")
    sub.add_parser("suspend-admin")
    sub.add_parser("resume-admin")
    sub.add_parser("deploy-realtime")
    sub.add_parser("deploy-risk")
    sub.add_parser("deploy-flink")
    sub.add_parser("deploy-adapter-sim")
    upload = sub.add_parser("upload-risk-bundle")
    upload.add_argument(
        "--bundle-dir", type=Path, default=ROOT / "build/c1/risk-runtime"
    )
    train = sub.add_parser("run-training-job")
    train.add_argument("--sync", action="store_true")
    sub.add_parser("status")
    args = parser.parse_args()

    if args.command == "bootstrap":
        bootstrap()
    elif args.command == "configure-kafka":
        configure_kafka()
    elif args.command == "configure-adapter-sim-kafka":
        configure_adapter_sim_kafka(
            allow_shared_credentials=args.allow_shared_credentials
        )
    elif args.command == "render":
        kafka_bootstrap_servers = args.kafka_bootstrap_servers
        if not kafka_bootstrap_servers:
            configured_brokers, username, password = _configured_kafka()
            if username and password:
                kafka_bootstrap_servers = configured_brokers
        render_specs(
            args.image_path,
            kafka_bootstrap_servers,
            risk_image_path=args.risk_image_path,
            flink_image_path=args.flink_image_path,
            adapter_image_path=args.adapter_image_path,
            triton_image_path=args.triton_image_path,
            build_version=args.build_version,
            risk_build_version=args.risk_build_version,
            flink_build_version=args.flink_build_version,
            adapter_build_version=args.adapter_build_version,
            model_run_id=args.model_run_id,
            allowed_tenants=args.allowed_tenants,
            risk_scorer_group_id=args.risk_scorer_group_id,
            adapter_group_id=args.adapter_group_id,
            adapter_dataset_id=args.adapter_dataset_id,
            adapter_allowed_tenants=args.adapter_allowed_tenants,
            flink_context_savepoint_path=args.flink_context_savepoint_path,
            flink_pair_savepoint_path=args.flink_pair_savepoint_path,
        )
    elif args.command == "deploy-admin":
        deploy_service("POKER_ADMIN", "admin.yaml")
    elif args.command == "suspend-admin":
        set_admin_state("SUSPEND")
    elif args.command == "resume-admin":
        set_admin_state("RESUME")
    elif args.command == "deploy-realtime":
        deploy_service("POKER_REALTIME", "realtime.yaml", kafka_eai=True)
    elif args.command == "deploy-risk":
        deploy_service("POKER_RISK", "risk.yaml", kafka_eai=True)
    elif args.command == "deploy-flink":
        deploy_service("POKER_FLINK", "flink.yaml", kafka_eai=True)
    elif args.command == "deploy-adapter-sim":
        deploy_service(
            "POKER_ADAPTER_SIM",
            "adapter-sim.yaml",
            kafka_eai=ADAPTER_SIM_KAFKA_EAI,
        )
    elif args.command == "upload-risk-bundle":
        upload_risk_bundle(args.bundle_dir)
    elif args.command == "run-training-job":
        run_training_job(async_=not args.sync)
    elif args.command == "status":
        status()


if __name__ == "__main__":
    main()
