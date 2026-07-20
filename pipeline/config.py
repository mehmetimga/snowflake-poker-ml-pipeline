from __future__ import annotations

from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    warehouse_backend: Literal["duckdb", "snowflake"] = Field("duckdb", alias="WAREHOUSE_BACKEND")
    duckdb_path: Path = Field(Path("./data/parquet/warehouse.duckdb"), alias="DUCKDB_PATH")
    duckdb_s3_bucket: Optional[str] = Field(None, alias="DUCKDB_S3_BUCKET")
    duckdb_s3_prefix: str = Field("warehouse/", alias="DUCKDB_S3_PREFIX")

    snowflake_account: Optional[str] = Field(None, alias="SNOWFLAKE_ACCOUNT")
    snowflake_host: Optional[str] = Field(None, alias="SNOWFLAKE_HOST")
    snowflake_user: Optional[str] = Field(None, alias="SNOWFLAKE_USER")
    snowflake_password: Optional[str] = Field(None, alias="SNOWFLAKE_PASSWORD")
    snowflake_authenticator: Optional[str] = Field(None, alias="SNOWFLAKE_AUTHENTICATOR")
    snowflake_client_request_mfa_token: bool = Field(
        False, alias="SNOWFLAKE_CLIENT_REQUEST_MFA_TOKEN"
    )
    snowflake_private_key_path: Optional[Path] = Field(None, alias="SNOWFLAKE_PRIVATE_KEY_PATH")
    snowflake_oauth_token_path: Path = Field(
        Path("/snowflake/session/token"), alias="SNOWFLAKE_OAUTH_TOKEN_PATH"
    )
    snowflake_warehouse: str = Field("DEMO_WH", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field("POKER_ML_DEMO", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field("PUBLIC", alias="SNOWFLAKE_SCHEMA")
    snowflake_role: str = Field("SYSADMIN", alias="SNOWFLAKE_ROLE")
    snowflake_model_stage: str = Field(
        "POKER_ML_DEMO.SPCS.MODEL_ARTIFACTS", alias="SNOWFLAKE_MODEL_STAGE"
    )

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_egress_brokers: Optional[str] = Field(None, alias="KAFKA_EGRESS_BROKERS")
    kafka_hands_topic: str = Field("hands.raw", alias="KAFKA_HANDS_TOPIC")
    kafka_world_hands_topic: str = Field(
        "poker.hands.raw.v1", alias="KAFKA_WORLD_HANDS_TOPIC"
    )
    kafka_user_context_topic: str = Field(
        "poker.user-context.v1", alias="KAFKA_USER_CONTEXT_TOPIC"
    )
    kafka_session_context_topic: str = Field(
        "poker.session-context.v1", alias="KAFKA_SESSION_CONTEXT_TOPIC"
    )
    kafka_account_links_topic: str = Field(
        "poker.account-links.v1", alias="KAFKA_ACCOUNT_LINKS_TOPIC"
    )
    kafka_player_context_topic: str = Field(
        "poker.hand-player-context.v1", alias="KAFKA_PLAYER_CONTEXT_TOPIC"
    )
    kafka_pair_features_topic: str = Field(
        "poker.pair-features.v1", alias="KAFKA_PAIR_FEATURES_TOPIC"
    )
    kafka_risk_scores_topic: str = Field(
        "poker.risk-scores.v1", alias="KAFKA_RISK_SCORES_TOPIC"
    )
    kafka_risk_alerts_topic: str = Field(
        "poker.risk-alerts.v1", alias="KAFKA_RISK_ALERTS_TOPIC"
    )
    kafka_dead_letter_topic: str = Field(
        "poker.pipeline.dead-letter.v1", alias="KAFKA_DEAD_LETTER_TOPIC"
    )
    kafka_alerts_topic: str = Field("alerts.out", alias="KAFKA_ALERTS_TOPIC")
    kafka_pair_memory_topic: str = Field("pair.memory", alias="KAFKA_PAIR_MEMORY_TOPIC")
    kafka_action_patterns_topic: str = Field("patterns.action", alias="KAFKA_ACTION_PATTERNS_TOPIC")
    kafka_security_protocol: str = Field("PLAINTEXT", alias="KAFKA_SECURITY_PROTOCOL")
    kafka_sasl_mechanism: Optional[str] = Field(None, alias="KAFKA_SASL_MECHANISM")
    kafka_sasl_username: Optional[str] = Field(None, alias="KAFKA_SASL_USERNAME")
    kafka_sasl_password: Optional[str] = Field(None, alias="KAFKA_SASL_PASSWORD")
    aws_region: str = Field("us-west-2", alias="AWS_REGION")

    qdrant_url: str = Field("http://localhost:6333", alias="QDRANT_URL")
    qdrant_collusion_collection: str = Field("collusion_patterns", alias="QDRANT_COLLUSION_COLLECTION")
    qdrant_normal_collection: str = Field("normal_patterns", alias="QDRANT_NORMAL_COLLECTION")

    models_dir: Path = Field(Path("./models"), alias="MODELS_DIR")
    random_seed: int = Field(42, alias="RANDOM_SEED")


_settings: Optional[Settings] = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings
