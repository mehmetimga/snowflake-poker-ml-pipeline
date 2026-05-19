"""PyFlink job for action-level collusion pattern candidates."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import flink_kafka_properties
from pipeline.realtime.action_patterns import action_events_from_hand, detect_action_patterns


def action_pattern_jsons_from_hand_json(
    value: str,
    max_gap: int = 3,
    min_call_amount_bb: float = 2.0,
) -> list[str]:
    hand = json.loads(value)
    events = action_events_from_hand(hand)
    patterns = detect_action_patterns(
        events,
        max_gap=max_gap,
        min_call_amount_bb=min_call_amount_bb,
    )
    return [json.dumps(row, separators=(",", ":"), sort_keys=True) for row in patterns]


def action_pattern_key(value: str) -> str:
    row = json.loads(value)
    return str(row.get("pair_key") or row["hand_id"])


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--input-topic", default=None)
    parser.add_argument("--action-patterns-topic", default=None)
    parser.add_argument("--group-id", default="flink-action-patterns")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--python-executable", default=os.getenv("PYFLINK_PYTHON") or None)
    parser.add_argument("--max-gap", type=int, default=3)
    parser.add_argument("--min-call-amount-bb", type=float, default=2.0)
    parser.add_argument(
        "--kafka-connector-jar",
        default=os.getenv("FLINK_KAFKA_CONNECTOR_JAR") or None,
    )
    return parser.parse_args()


def _apply_kafka_properties(builder: Any) -> Any:
    for key, value in flink_kafka_properties().items():
        builder.set_property(key, value)
    return builder


def main() -> None:
    try:
        from pyflink.common import Types
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.watermark_strategy import WatermarkStrategy
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            DeliveryGuarantee,
            KafkaOffsetsInitializer,
            KafkaRecordSerializationSchema,
            KafkaSink,
            KafkaSource,
        )
    except ImportError as exc:
        raise SystemExit(
            "PyFlink is required for the action-pattern job. Install it with "
            "`pip install -r requirements-flink.txt`, or run inside a Flink "
            "Python container."
        ) from exc

    args = _parse_args()
    settings = get_settings()
    bootstrap_servers = args.bootstrap_servers or settings.kafka_bootstrap_servers
    input_topic = args.input_topic or settings.kafka_hands_topic
    action_patterns_topic = (
        args.action_patterns_topic or settings.kafka_action_patterns_topic
    )

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    if args.python_executable:
        env.set_python_executable(args.python_executable)
    if args.checkpoint_interval_ms > 0:
        env.enable_checkpointing(args.checkpoint_interval_ms)
    if args.kafka_connector_jar:
        env.add_jars(Path(args.kafka_connector_jar).resolve().as_uri())

    source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(input_topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
            if args.from_beginning
            else KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
    )
    source = _apply_kafka_properties(source_builder).build()

    sink_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(action_patterns_topic)
        .set_value_serialization_schema(SimpleStringSchema())
        .build()
    )
    sink_builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_record_serializer(sink_serializer)
        .set_delivery_guarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    )
    sink = _apply_kafka_properties(sink_builder).build()

    def detect_patterns(value: str):
        yield from action_pattern_jsons_from_hand_json(
            value,
            max_gap=args.max_gap,
            min_call_amount_bb=args.min_call_amount_bb,
        )

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "hands.raw")
        .flat_map(detect_patterns, output_type=Types.STRING())
        .sink_to(sink)
    )
    print(
        json.dumps(
            {
                "job": "flink-action-patterns",
                "input_topic": input_topic,
                "action_patterns_topic": action_patterns_topic,
                "max_gap": args.max_gap,
                "min_call_amount_bb": args.min_call_amount_bb,
            },
            sort_keys=True,
        )
    )
    env.execute("poker-flink-action-patterns")


if __name__ == "__main__":
    main()
