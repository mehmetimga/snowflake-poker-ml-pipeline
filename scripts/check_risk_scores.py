"""Consume and validate versioned Go risk-score events from Kafka."""

from __future__ import annotations

import argparse
import json

from pipeline.config import get_settings
from pipeline.events import RiskScoreEvent
from pipeline.kafka.config import kafka_client_kwargs


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--topic", default=None)
    parser.add_argument("--model-run-id", default=None)
    parser.add_argument("--dataset-id", default=None)
    parser.add_argument("--minimum-records", type=int, default=1)
    parser.add_argument("--timeout-ms", type=int, default=15_000)
    args = parser.parse_args()
    if args.minimum_records < 1 or args.timeout_ms < 1:
        parser.error("minimum-records and timeout-ms must be positive")

    from kafka import KafkaConsumer

    settings = get_settings()
    topic = args.topic or settings.kafka_risk_scores_topic
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=(
            args.bootstrap_servers or settings.kafka_bootstrap_servers
        ).split(","),
        key_deserializer=lambda value: value.decode("utf-8") if value else None,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=args.timeout_ms,
        **kafka_client_kwargs(),
    )
    matches: list[RiskScoreEvent] = []
    try:
        for message in consumer:
            event = RiskScoreEvent.model_validate(message.value)
            if message.key != event.payload.hand_id:
                raise ValueError(
                    f"risk score {event.event_id} key={message.key!r}, "
                    f"expected {event.payload.hand_id!r}"
                )
            if args.model_run_id and event.payload.model_run_id != args.model_run_id:
                continue
            if args.dataset_id and event.dataset_id != args.dataset_id:
                continue
            matches.append(event)
            if len(matches) >= args.minimum_records:
                break
    finally:
        consumer.close()

    if len(matches) < args.minimum_records:
        raise RuntimeError(
            f"found {len(matches)} matching scores; expected {args.minimum_records} "
            f"on {topic}"
        )
    latest = matches[-1]
    print(
        "[risk-scores-check] "
        + json.dumps(
            {
                "validated": len(matches),
                "event_id": str(latest.event_id),
                "score_id": latest.payload.score_id,
                "hand_id": latest.payload.hand_id,
                "dataset_id": latest.dataset_id,
                "model_run_id": latest.payload.model_run_id,
                "feature_definition_version": (
                    latest.payload.feature_definition_version
                ),
                "decision_policy_version": latest.payload.decision_policy_version,
                "service_implementation": latest.payload.service_implementation,
                "service_build_version": latest.payload.service_build_version,
                "pair_scores": len(latest.payload.pair_scores),
                "player_scores": len(latest.payload.player_scores),
                "hand_risk_probability": latest.payload.hand_risk_probability,
                "alert": latest.payload.alert,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
