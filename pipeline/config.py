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
    snowflake_user: Optional[str] = Field(None, alias="SNOWFLAKE_USER")
    snowflake_password: Optional[str] = Field(None, alias="SNOWFLAKE_PASSWORD")
    snowflake_private_key_path: Optional[Path] = Field(None, alias="SNOWFLAKE_PRIVATE_KEY_PATH")
    snowflake_warehouse: str = Field("DEMO_WH", alias="SNOWFLAKE_WAREHOUSE")
    snowflake_database: str = Field("POKER_ML_DEMO", alias="SNOWFLAKE_DATABASE")
    snowflake_schema: str = Field("PUBLIC", alias="SNOWFLAKE_SCHEMA")
    snowflake_role: str = Field("SYSADMIN", alias="SNOWFLAKE_ROLE")

    kafka_bootstrap_servers: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_hands_topic: str = Field("hands.raw", alias="KAFKA_HANDS_TOPIC")
    kafka_alerts_topic: str = Field("alerts.out", alias="KAFKA_ALERTS_TOPIC")

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
