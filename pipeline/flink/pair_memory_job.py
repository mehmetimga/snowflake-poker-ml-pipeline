"""PyFlink keyed-state job for rolling player-pair memory."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import flink_kafka_properties
from pipeline.realtime.pair_memory import PairState, pair_state_from_update, pair_update_records_from_hand


def pair_update_jsons_from_hand_json(value: str) -> list[str]:
    hand = json.loads(value)
    return [json.dumps(row, separators=(",", ":")) for row in pair_update_records_from_hand(hand)]


def pair_update_key(value: str) -> str:
    return str(json.loads(value)["pair_key"])


def apply_pair_update_json(state_json: str | None, update_json: str) -> tuple[str, str]:
    update = json.loads(update_json)
    state = PairState.from_state_dict(json.loads(state_json)) if state_json else None
    state = pair_state_from_update(update, state)
    state_dict = state.to_state_dict()
    output = {
        "pair_key": update["pair_key"],
        "hand_id": update["hand_id"],
        "table_id": update.get("table_id"),
        "played_at": update.get("played_at"),
        **state.to_row(str(update["hand_id"])),
    }
    return json.dumps(state_dict, separators=(",", ":")), json.dumps(output, separators=(",", ":"))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--input-topic", default=None)
    parser.add_argument("--pair-memory-topic", default=None)
    parser.add_argument("--group-id", default="flink-pair-memory")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--python-executable", default=os.getenv("PYFLINK_PYTHON") or None)
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
        from pyflink.datastream.functions import KeyedProcessFunction
        from pyflink.datastream.state import ValueStateDescriptor
    except ImportError as exc:
        raise SystemExit(
            "PyFlink is required for the managed pair-memory job. Install it "
            "with `pip install -r requirements-flink.txt`, or run inside a "
            "Flink Python container."
        ) from exc

    class PairMemoryProcessFunction(KeyedProcessFunction):
        def open(self, runtime_context):
            self.state = runtime_context.get_state(
                ValueStateDescriptor("pair-state-json", Types.STRING())
            )

        def process_element(self, value, ctx):
            state_json, output_json = apply_pair_update_json(self.state.value(), value)
            self.state.update(state_json)
            yield output_json

    args = _parse_args()
    settings = get_settings()
    bootstrap_servers = args.bootstrap_servers or settings.kafka_bootstrap_servers
    input_topic = args.input_topic or settings.kafka_hands_topic
    pair_memory_topic = args.pair_memory_topic or settings.kafka_pair_memory_topic

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
        .set_topic(pair_memory_topic)
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

    def expand_pair_updates(value: str):
        yield from pair_update_jsons_from_hand_json(value)

    (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "hands.raw")
        .flat_map(expand_pair_updates, output_type=Types.STRING())
        .key_by(pair_update_key, key_type=Types.STRING())
        .process(PairMemoryProcessFunction(), output_type=Types.STRING())
        .sink_to(sink)
    )
    print(
        json.dumps(
            {
                "job": "flink-pair-memory",
                "input_topic": input_topic,
                "pair_memory_topic": pair_memory_topic,
            },
            sort_keys=True,
        )
    )
    env.execute("poker-flink-pair-memory")


if __name__ == "__main__":
    main()
