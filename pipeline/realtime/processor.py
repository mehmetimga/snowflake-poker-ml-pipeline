"""Real-time batch processor for live hand events.

The hot path scores each Kafka batch directly from memory. Warehouse writes are
optional persistence for history/training/admin; realtime scoring never reads
from the warehouse.
"""

from __future__ import annotations

import json
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
    ) -> int:
        from kafka import KafkaConsumer

        settings = get_settings()
        consumer = KafkaConsumer(
            topic or settings.kafka_hands_topic,
            bootstrap_servers=(bootstrap_servers or settings.kafka_bootstrap_servers).split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            group_id=group_id,
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=True,
            consumer_timeout_ms=10000,
            **kafka_client_kwargs(),
        )
        total = 0
        buffer: list[dict] = []
        try:
            for msg in consumer:
                buffer.append(msg.value)
                if len(buffer) >= batch_size:
                    total += self._flush(buffer)
                    buffer.clear()
                if max_messages is not None and total + len(buffer) >= max_messages:
                    break
            if buffer:
                total += self._flush(buffer)
        finally:
            consumer.close()
        return total

    def _flush(self, buffer: list[dict]) -> int:
        result = self.process_hands(buffer)
        print(
            "[realtime] "
            f"hands={result.hands} features={result.features} "
            f"rules={result.rule_flags} pairs={result.pair_stats} alerts={result.alerts}"
        )
        return result.hands
