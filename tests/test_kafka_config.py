from __future__ import annotations

from pipeline.config import Settings
from pipeline.kafka import config as kafka_config


def test_kafka_client_kwargs_defaults_to_plaintext(monkeypatch):
    monkeypatch.setattr(kafka_config, "get_settings", lambda: Settings())

    assert kafka_config.kafka_client_kwargs() == {"security_protocol": "PLAINTEXT"}


def test_kafka_client_kwargs_maps_aws_msk_iam_to_oauth_provider(monkeypatch):
    settings = Settings(
        KAFKA_SECURITY_PROTOCOL="SASL_SSL",
        KAFKA_SASL_MECHANISM="AWS_MSK_IAM",
        AWS_REGION="us-east-1",
    )
    monkeypatch.setattr(kafka_config, "get_settings", lambda: settings)

    kwargs = kafka_config.kafka_client_kwargs()

    assert kwargs["security_protocol"] == "SASL_SSL"
    assert kwargs["sasl_mechanism"] == "OAUTHBEARER"
    assert kwargs["sasl_oauth_token_provider"].region == "us-east-1"
