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
from pipeline.features.engineer import compute_features
from pipeline.inference.scorer import score_live_batch
from pipeline.kafka.config import kafka_client_kwargs
from pipeline.realtime.pattern_search import PatternSearchConfig, realtime_pattern_scores
from pipeline.rules.engine import score_dataframe
from pipeline.warehouse import Warehouse, get_warehouse
from pipeline.warehouse.loader import hands_to_dataframes
from pipeline.warehouse.sql import delete_by_values, unique_strings


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
    ) -> None:
        self.warehouse = warehouse
        self.threshold = threshold
        self.persist_history = persist_history
        self.persist_alerts = persist_alerts
        self.pattern_search = pattern_search or PatternSearchConfig()

    def _warehouse(self) -> Warehouse:
        if self.warehouse is None:
            self.warehouse = get_warehouse()
        return self.warehouse

    def process_hands(self, hands: Iterable[dict]) -> RealTimeBatchResult:
        batch = list(hands)
        hand_ids = unique_strings(hand["hand_id"] for hand in batch if "hand_id" in hand)
        if not batch or not hand_ids:
            return RealTimeBatchResult(hands=0, features=0, rule_flags=0, pair_stats=0, alerts=0)

        df_hands, df_actions, df_players = hands_to_dataframes(batch)
        features = compute_features(df_hands, df_actions, df_players)
        flags = score_dataframe(features)
        pattern_scores = {}
        if self.pattern_search.enabled:
            preliminary_alerts = score_live_batch(
                features=features,
                rule_flags=flags,
                hands=df_hands,
                actions=df_actions,
                players=df_players,
                threshold=self.pattern_search.candidate_risk_score,
                log=False,
            )
            pattern_scores = realtime_pattern_scores(
                players=df_players,
                rule_flags=flags,
                preliminary_alerts=preliminary_alerts,
                config=self.pattern_search,
            )
        alerts = score_live_batch(
            features=features,
            rule_flags=flags,
            hands=df_hands,
            actions=df_actions,
            players=df_players,
            threshold=self.threshold,
            pattern_scores=pattern_scores,
        )

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
                delete_by_values(warehouse, table, "hand_id", hand_ids)

            if self.persist_history:
                warehouse.write_pandas(df_hands, "RAW_HANDS")
                warehouse.write_pandas(df_actions, "RAW_ACTIONS")
                warehouse.write_pandas(df_players, "RAW_PLAYERS")
                warehouse.write_pandas(features, "FEATURES")
                warehouse.write_pandas(flags, "RULE_FLAGS")
            if self.persist_alerts:
                warehouse.write_pandas(alerts, "ALERTS")

        return RealTimeBatchResult(
            hands=len(df_hands),
            features=len(features),
            rule_flags=len(flags),
            pair_stats=0,
            alerts=len(alerts),
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
