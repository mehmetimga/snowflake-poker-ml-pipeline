"""Real-time batch processor for live hand events.

The hot path scores each Kafka batch directly from memory. Warehouse writes are
optional persistence for history/training/admin; realtime scoring never reads
from the warehouse.
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Iterable, Optional

from pipeline.config import get_settings
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.realtime.batch import score_live_hands
from pipeline.realtime.pair_memory import RollingPairMemory
from pipeline.realtime.pattern_search import PatternSearchConfig
from pipeline.warehouse import Warehouse, get_warehouse
from pipeline.warehouse.sql import delete_by_values


@dataclass(frozen=True)
class RealTimeBatchResult:
    hands: int
    features: int
    rule_flags: int
    pair_stats: int
    alerts: int


class RealTimeProcessor:
    """Feature, score, alert, and optionally persist each live batch."""

    def __init__(
        self,
        warehouse: Optional[Warehouse] = None,
        threshold: float = 0.5,
        persist_history: bool = True,
        persist_alerts: bool = True,
        pattern_search: PatternSearchConfig | None = None,
        enable_pair_memory: bool = True,
        pair_memory_max_pairs: int = 10000,
        pair_memory: RollingPairMemory | None = None,
    ) -> None:
        self.warehouse = warehouse
        self.threshold = threshold
        self.persist_history = persist_history
        self.persist_alerts = persist_alerts
        self.pattern_search = pattern_search or PatternSearchConfig()
        self.pair_memory = (
            pair_memory
            if pair_memory is not None
            else RollingPairMemory(max_pairs=pair_memory_max_pairs)
            if enable_pair_memory
            else None
        )

    def _warehouse(self) -> Warehouse:
        if self.warehouse is None:
            self.warehouse = get_warehouse()
        return self.warehouse

    def process_hands(self, hands: Iterable[dict]) -> RealTimeBatchResult:
        scored = score_live_hands(
            hands,
            threshold=self.threshold,
            pattern_search=self.pattern_search,
            pair_memory=self.pair_memory,
        )
        if not scored.hand_ids:
            return RealTimeBatchResult(hands=0, features=0, rule_flags=0, pair_stats=0, alerts=0)

        if self.persist_history or self.persist_alerts:
            warehouse = self._warehouse()
            tables = []
            if self.persist_alerts:
                tables.append("ALERTS")
            if self.persist_history:
                tables.extend(["RULE_FLAGS", "FEATURES", "RAW_ACTIONS", "RAW_PLAYERS", "RAW_HANDS"])

            # Make batch replay idempotent for Snowflake too, where primary keys
            # are informational and write_pandas appends by default.
            for table in tables:
                delete_by_values(warehouse, table, "hand_id", scored.hand_ids)

            if self.persist_history:
                warehouse.write_pandas(scored.hands, "RAW_HANDS")
                warehouse.write_pandas(scored.actions, "RAW_ACTIONS")
                warehouse.write_pandas(scored.players, "RAW_PLAYERS")
                warehouse.write_pandas(scored.features, "FEATURES")
                warehouse.write_pandas(scored.rule_flags, "RULE_FLAGS")
            if self.persist_alerts and not scored.alerts.empty:
                warehouse.write_pandas(scored.alerts, "ALERTS")

        return RealTimeBatchResult(
            hands=len(scored.hands),
            features=len(scored.features),
            rule_flags=len(scored.rule_flags),
            pair_stats=scored.pair_stats_count,
            alerts=len(scored.alerts),
        )

    def run_kafka(
        self,
        bootstrap_servers: Optional[str] = None,
        topic: Optional[str] = None,
        batch_size: int = 25,
        max_messages: Optional[int] = None,
        group_id: str = "realtime-processor",
        auto_offset_reset: str = "latest",
        flush_interval_seconds: float = 5.0,
        poll_timeout_ms: int = 1000,
        bounded_idle_timeout_seconds: float = 10.0,
    ) -> int:
        from kafka import KafkaConsumer

        if batch_size < 1:
            raise ValueError("batch_size must be positive")
        if max_messages is not None and max_messages < 1:
            raise ValueError("max_messages must be positive when provided")
        if flush_interval_seconds < 0:
            raise ValueError("flush_interval_seconds cannot be negative")
        if poll_timeout_ms < 1:
            raise ValueError("poll_timeout_ms must be positive")
        if bounded_idle_timeout_seconds < 0:
            raise ValueError("bounded_idle_timeout_seconds cannot be negative")

        settings = get_settings()
        consumer = KafkaConsumer(
            topic or settings.kafka_hands_topic,
            bootstrap_servers=(bootstrap_servers or settings.kafka_bootstrap_servers).split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            # Persist the complete batch before advancing offsets. If scoring,
            # Snowflake, or the commit itself fails, a restart replays the same
            # records; warehouse writes are idempotent by hand_id.
            enable_auto_commit=False,
            **kafka_client_kwargs(),
        )
        total = 0
        buffer: list[dict] = []
        buffer_started_at: float | None = None
        last_record_at = time.monotonic()
        try:
            while max_messages is None or total < max_messages:
                now = time.monotonic()
                if (
                    buffer
                    and buffer_started_at is not None
                    and now - buffer_started_at >= flush_interval_seconds
                ):
                    total += self._flush_and_commit(consumer, buffer)
                    buffer.clear()
                    buffer_started_at = None

                if max_messages is not None and total >= max_messages:
                    break

                max_records = batch_size - len(buffer)
                if max_messages is not None:
                    max_records = min(max_records, max_messages - total - len(buffer))
                if max_records < 1:
                    total += self._flush_and_commit(consumer, buffer)
                    buffer.clear()
                    buffer_started_at = None
                    continue

                try:
                    records_by_partition = consumer.poll(
                        timeout_ms=poll_timeout_ms,
                        max_records=max_records,
                    )
                except KeyboardInterrupt:
                    print(
                        "[realtime] shutdown requested; flushing buffered records",
                        file=sys.stderr,
                        flush=True,
                    )
                    break

                received = 0
                for records in records_by_partition.values():
                    for record in records:
                        if not buffer:
                            buffer_started_at = time.monotonic()
                        buffer.append(record.value)
                        received += 1

                now = time.monotonic()
                if received:
                    last_record_at = now

                max_reached = (
                    max_messages is not None
                    and total + len(buffer) >= max_messages
                )
                flush_due = (
                    buffer
                    and buffer_started_at is not None
                    and now - buffer_started_at >= flush_interval_seconds
                )
                if buffer and (len(buffer) >= batch_size or max_reached or flush_due):
                    total += self._flush_and_commit(consumer, buffer)
                    buffer.clear()
                    buffer_started_at = None

                if (
                    max_messages is not None
                    and not received
                    and now - last_record_at >= bounded_idle_timeout_seconds
                ):
                    break

            if buffer:
                total += self._flush_and_commit(consumer, buffer)
        finally:
            consumer.close()
        return total

    def _flush_and_commit(self, consumer, buffer: list[dict]) -> int:
        try:
            processed = self._flush(buffer)
            consumer.commit()
        except Exception:
            print(
                "[realtime] batch failed; Kafka offsets were not committed",
                file=sys.stderr,
                flush=True,
            )
            traceback.print_exc(file=sys.stderr)
            raise
        print(
            f"[realtime] committed Kafka offsets after {processed} persisted hands",
            flush=True,
        )
        return processed

    def _flush(self, buffer: list[dict]) -> int:
        result = self.process_hands(buffer)
        print(
            "[realtime] "
            f"hands={result.hands} features={result.features} "
            f"rules={result.rule_flags} pairs={result.pair_stats} alerts={result.alerts}",
            flush=True,
        )
        return result.hands
