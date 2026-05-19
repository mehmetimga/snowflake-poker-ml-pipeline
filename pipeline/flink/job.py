"""PyFlink hot path for live poker-collusion scoring.

The job consumes complete hand JSON events from Kafka/MSK, runs the same
in-memory feature/rule/model/Qdrant scoring used by `scripts/realtime.py`, and
emits alert JSON records to a Kafka topic.
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from dataclasses import asdict
from pathlib import Path
from typing import Any

from pipeline.config import get_settings
from pipeline.kafka.config import flink_kafka_properties
from pipeline.realtime.batch import score_live_hands
from pipeline.realtime.pair_memory import RollingPairMemory, pair_memory_frame_for_hand, pair_update_records_from_hand
from pipeline.realtime.pattern_search import PatternSearchConfig


def alerts_from_hand_json(
    value: str,
    threshold: float = 0.5,
    pattern_search: PatternSearchConfig | None = None,
    pair_memory: RollingPairMemory | None = None,
    pair_memory_by_key: Mapping[str, object] | None = None,
) -> list[str]:
    """Return serialized alerts for one Kafka hand JSON message."""
    hand = json.loads(value)
    pair_memory_stats = (
        pair_memory_frame_for_hand(hand, pair_memory_by_key)
        if pair_memory_by_key is not None
        else None
    )
    scored = score_live_hands(
        [hand],
        threshold=threshold,
        pattern_search=pattern_search,
        pair_memory=pair_memory,
        pair_memory_stats=pair_memory_stats,
        log=False,
    )
    if scored.alerts.empty:
        return []
    return [
        json.dumps(record, default=str, separators=(",", ":"))
        for record in scored.alerts.to_dict("records")
    ]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default=None)
    parser.add_argument("--input-topic", default=None)
    parser.add_argument("--alerts-topic", default=None)
    parser.add_argument("--pair-memory-topic", default=None)
    parser.add_argument("--group-id", default="flink-realtime-processor")
    parser.add_argument("--pair-memory-group-id", default="flink-alert-pair-memory")
    parser.add_argument("--use-pair-memory-topic", action="store_true")
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--checkpoint-interval-ms", type=int, default=30000)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--python-executable", default=os.getenv("PYFLINK_PYTHON") or None)
    parser.add_argument(
        "--kafka-connector-jar",
        default=os.getenv("FLINK_KAFKA_CONNECTOR_JAR") or None,
    )
    parser.add_argument("--enable-pattern-search", action="store_true")
    parser.add_argument("--pattern-candidate-rule-score", type=float, default=1.0)
    parser.add_argument("--pattern-candidate-risk-score", type=float, default=0.5)
    parser.add_argument("--pattern-candidate-pair-memory-score", type=float, default=0.65)
    parser.add_argument("--pattern-max-pairs", type=int, default=50)
    parser.add_argument("--pattern-timeout", type=float, default=1.5)
    parser.add_argument("--pair-memory-max-pairs", type=int, default=10000)
    parser.add_argument("--no-pair-memory", action="store_true")
    return parser.parse_args()


def _apply_kafka_properties(builder: Any) -> Any:
    for key, value in flink_kafka_properties().items():
        builder.set_property(key, value)
    return builder


def main() -> None:
    try:
        from pyflink.common import Types
        from pyflink.common.state import MapStateDescriptor
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
        from pyflink.datastream.functions import BroadcastProcessFunction
    except ImportError as exc:
        raise SystemExit(
            "PyFlink is required for the Flink hot path. Install it with "
            "`pip install -r requirements-flink.txt`, or run this job inside "
            "a Flink Python container."
        ) from exc

    args = _parse_args()
    settings = get_settings()
    bootstrap_servers = args.bootstrap_servers or settings.kafka_bootstrap_servers
    input_topic = args.input_topic or settings.kafka_hands_topic
    alerts_topic = args.alerts_topic or settings.kafka_alerts_topic
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

    pair_source_builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(bootstrap_servers)
        .set_topics(pair_memory_topic)
        .set_group_id(args.pair_memory_group_id)
        .set_starting_offsets(
            KafkaOffsetsInitializer.earliest()
            if args.from_beginning
            else KafkaOffsetsInitializer.latest()
        )
        .set_value_only_deserializer(SimpleStringSchema())
    )
    pair_source = _apply_kafka_properties(pair_source_builder).build()

    sink_serializer = (
        KafkaRecordSerializationSchema.builder()
        .set_topic(alerts_topic)
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

    pattern_search = PatternSearchConfig(
        enabled=args.enable_pattern_search,
        candidate_rule_score=args.pattern_candidate_rule_score,
        candidate_risk_score=args.pattern_candidate_risk_score,
        candidate_pair_memory_score=args.pattern_candidate_pair_memory_score,
        max_pairs=args.pattern_max_pairs,
        timeout=args.pattern_timeout,
    )
    pair_memory = (
        None
        if args.no_pair_memory or args.use_pair_memory_topic
        else RollingPairMemory(max_pairs=args.pair_memory_max_pairs)
    )

    def alert_flat_map(value: str):
        yield from alerts_from_hand_json(
            value,
            threshold=args.threshold,
            pattern_search=pattern_search,
            pair_memory=pair_memory,
        )

    hand_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "hands.raw")
    if args.use_pair_memory_topic:
        pair_state_descriptor = MapStateDescriptor(
            "pair-memory-broadcast",
            Types.STRING(),
            Types.STRING(),
        )

        class PairMemoryEnrichedAlerts(BroadcastProcessFunction):
            def process_broadcast_element(self, value, ctx):
                row = json.loads(value)
                ctx.get_broadcast_state(pair_state_descriptor).put(str(row["pair_key"]), value)

            def process_element(self, value, ctx):
                hand = json.loads(value)
                state = ctx.get_broadcast_state(pair_state_descriptor)
                pair_memory_by_key = {}
                for update in pair_update_records_from_hand(hand):
                    raw = state.get(update["pair_key"])
                    if raw is not None:
                        pair_memory_by_key[update["pair_key"]] = raw
                yield from alerts_from_hand_json(
                    value,
                    threshold=args.threshold,
                    pattern_search=pattern_search,
                    pair_memory_by_key=pair_memory_by_key,
                )

        (
            hand_stream.connect(
                env.from_source(pair_source, WatermarkStrategy.no_watermarks(), "pair.memory")
                .broadcast(pair_state_descriptor)
            )
            .process(PairMemoryEnrichedAlerts(), output_type=Types.STRING())
            .sink_to(sink)
        )
    else:
        hand_stream.flat_map(alert_flat_map, output_type=Types.STRING()).sink_to(sink)
    print(
        json.dumps(
            {
                "job": "flink-realtime",
                "input_topic": input_topic,
                "alerts_topic": alerts_topic,
                "pair_memory_topic": pair_memory_topic if args.use_pair_memory_topic else None,
                "pattern_search": asdict(pattern_search),
                "pair_memory_enabled": pair_memory is not None or args.use_pair_memory_topic,
                "pair_memory_mode": "broadcast-topic" if args.use_pair_memory_topic else "operator-local",
            },
            sort_keys=True,
        )
    )
    env.execute("poker-flink-realtime")


if __name__ == "__main__":
    main()
