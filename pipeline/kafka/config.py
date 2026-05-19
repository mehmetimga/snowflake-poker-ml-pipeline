from __future__ import annotations

from typing import Any

from pipeline.config import get_settings


class _MskIamTokenProvider:
    def __init__(self, region: str) -> None:
        self.region = region

    def token(self) -> str:
        try:
            from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
        except ImportError as exc:
            raise RuntimeError(
                "KAFKA_SASL_MECHANISM=AWS_MSK_IAM requires "
                "aws-msk-iam-sasl-signer-python. Run `make install` after "
                "updating requirements."
            ) from exc
        token, _ = MSKAuthTokenProvider.generate_auth_token(self.region)
        return token


def kafka_client_kwargs() -> dict[str, Any]:
    """Shared Kafka security settings for local Kafka and AWS MSK clients."""
    settings = get_settings()
    kwargs: dict[str, Any] = {
        "security_protocol": settings.kafka_security_protocol,
    }
    mechanism = settings.kafka_sasl_mechanism
    if not mechanism:
        return kwargs

    if mechanism == "AWS_MSK_IAM":
        kwargs["sasl_mechanism"] = "OAUTHBEARER"
        kwargs["sasl_oauth_token_provider"] = _MskIamTokenProvider(settings.aws_region)
        return kwargs

    kwargs["sasl_mechanism"] = mechanism
    if settings.kafka_sasl_username is not None:
        kwargs["sasl_plain_username"] = settings.kafka_sasl_username
    if settings.kafka_sasl_password is not None:
        kwargs["sasl_plain_password"] = settings.kafka_sasl_password
    return kwargs


def flink_kafka_properties() -> dict[str, str]:
    """Java Kafka client properties for Flink Kafka source/sink connectors."""
    settings = get_settings()
    props = {"security.protocol": settings.kafka_security_protocol}
    mechanism = settings.kafka_sasl_mechanism
    if not mechanism:
        return props

    if mechanism == "AWS_MSK_IAM":
        props["sasl.mechanism"] = "AWS_MSK_IAM"
        props["sasl.jaas.config"] = "software.amazon.msk.auth.iam.IAMLoginModule required;"
        props["sasl.client.callback.handler.class"] = (
            "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
        )
        return props

    props["sasl.mechanism"] = mechanism
    if settings.kafka_sasl_username is not None or settings.kafka_sasl_password is not None:
        props["sasl.jaas.config"] = (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{settings.kafka_sasl_username or ""}" '
            f'password="{settings.kafka_sasl_password or ""}";'
        )
    return props
