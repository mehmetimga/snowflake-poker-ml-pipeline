from __future__ import annotations

import math
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pipeline.config import Settings
from pipeline.events import USER_CONTEXT_UPDATED, build_event
from pipeline.generator import (
    FrozenDatasetConfig,
    RealtimeWorldConfig,
    build_realtime_world_dataset,
)
from pipeline.kafka.world_sink import WorldWarehouseSink
from pipeline.replay import iter_world_events
from pipeline.warehouse.duckdb import DuckDBWarehouse
from pipeline.warehouse.events import IngestRecord, load_canonical_events
from pipeline.warehouse.migrate import run_migrations


def _warehouse(tmp_path: Path) -> DuckDBWarehouse:
    warehouse = DuckDBWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="duckdb",
            DUCKDB_PATH=tmp_path / "warehouse.duckdb",
        )
    )
    run_migrations(warehouse)
    return warehouse


def _dataset(tmp_path: Path) -> Path:
    dataset_dir = tmp_path / "world"
    build_realtime_world_dataset(
        dataset_dir,
        RealtimeWorldConfig(
            dataset_id="ingest-test-v1",
            frozen=FrozenDatasetConfig(
                train_hands=3,
                validation_hands=1,
                test_hands=1,
                challenge_hands=1,
                n_players=12,
                n_tables=2,
                n_colluding_pairs=3,
                seed=919,
            ),
        ),
    )
    return dataset_dir


def _records(dataset_dir: Path):
    return [
        IngestRecord(
            envelope=event.envelope,
            topic=event.canonical_topic,
            partition=index % 3,
            offset=index,
            kafka_timestamp_ms=1_700_000_000_000 + index,
        )
        for index, event in enumerate(iter_world_events(dataset_dir, ("train",)))
    ]


def _count(warehouse: DuckDBWarehouse, table: str) -> int:
    return int(warehouse.fetch_df(f"SELECT COUNT(*) AS n FROM {table}").iloc[0]["n"])


def test_canonical_loader_is_idempotent_and_separates_context_history(tmp_path: Path):
    warehouse = _warehouse(tmp_path)
    dataset_dir = _dataset(tmp_path)
    records = _records(dataset_dir)

    first = load_canonical_events(warehouse, records)
    second = load_canonical_events(warehouse, records)

    assert first == second
    assert first.events == len(records)
    assert first.hands == 3
    assert first.contexts == 12
    assert first.sessions == 12
    assert first.account_links == 1
    assert _count(warehouse, "RAW_EVENT_ENVELOPES") == len(records)
    assert _count(warehouse, "RAW_HANDS") == 3
    assert _count(warehouse, "RAW_PLAYERS") == 18
    assert _count(warehouse, "USER_CONTEXT_EVENTS") == 12
    assert _count(warehouse, "USER_CONTEXT_HISTORY") == 12
    assert _count(warehouse, "USER_CONTEXT_CURRENT") == 12
    assert not warehouse.fetch_df("SELECT * FROM RAW_PLAYERS")["is_suspicious"].any()
    warehouse.close()


def test_context_history_uses_effective_time_for_late_versions(tmp_path: Path):
    warehouse = _warehouse(tmp_path)
    dataset_dir = _dataset(tmp_path)
    base_event = next(
        event
        for event in iter_world_events(dataset_dir, ("train",))
        if event.event_type == USER_CONTEXT_UPDATED
    )
    load_canonical_events(
        warehouse,
        [IngestRecord(envelope=base_event.envelope, topic=base_event.canonical_topic)],
    )
    base_payload = dict(base_event.envelope["payload"])
    base_time = datetime.fromisoformat(base_payload["effective_at"])

    later_payload = {
        **base_payload,
        "context_version": 2,
        "effective_at": (base_time + timedelta(days=2)).isoformat(),
        "bankroll_bucket": "high",
    }
    middle_payload = {
        **base_payload,
        "context_version": 3,
        "effective_at": (base_time + timedelta(days=1)).isoformat(),
        "bankroll_bucket": "medium",
    }
    later = build_event(
        event_type=USER_CONTEXT_UPDATED,
        aggregate_id=f"{base_payload['user_id']}:context:2",
        payload=later_payload,
        dataset_id="ingest-test-v1",
        dataset_split="train",
        occurred_at=base_time + timedelta(days=2),
    )
    middle = build_event(
        event_type=USER_CONTEXT_UPDATED,
        aggregate_id=f"{base_payload['user_id']}:context:3",
        payload=middle_payload,
        dataset_id="ingest-test-v1",
        dataset_split="train",
        occurred_at=base_time + timedelta(days=1),
    )
    # Arrive out of event-time order: v2 first, then the effective-time middle v3.
    load_canonical_events(
        warehouse,
        [
            IngestRecord(envelope=later.model_dump(mode="json")),
            IngestRecord(envelope=middle.model_dump(mode="json")),
        ],
    )

    history = warehouse.fetch_df(
        "SELECT context_version, effective_from, effective_to, is_current, bankroll_bucket "
        "FROM USER_CONTEXT_HISTORY ORDER BY effective_from"
    )
    assert history["context_version"].tolist() == [1, 3, 2]
    assert history["bankroll_bucket"].tolist() == [base_payload["bankroll_bucket"], "medium", "high"]
    assert history["is_current"].tolist() == [False, False, True]
    assert history.iloc[0]["effective_to"] == history.iloc[1]["effective_from"]
    assert history.iloc[1]["effective_to"] == history.iloc[2]["effective_from"]
    warehouse.close()


class _Consumer:
    def __init__(self, messages) -> None:
        self.messages = messages
        self.commits = 0
        self.closed = 0

    def __iter__(self):
        return iter(self.messages)

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed += 1


def _messages(dataset_dir: Path):
    return [
        SimpleNamespace(
            value=event.envelope,
            key=event.partition_key,
            topic=event.canonical_topic,
            partition=index % 2,
            offset=index,
            timestamp=1_700_000_000_000 + index,
        )
        for index, event in enumerate(iter_world_events(dataset_dir, ("train",)))
    ]


def test_world_sink_commits_offsets_only_after_each_loaded_batch(tmp_path: Path):
    warehouse = _warehouse(tmp_path)
    dataset_dir = _dataset(tmp_path)
    messages = _messages(dataset_dir)
    consumer = _Consumer(messages)
    sink = WorldWarehouseSink(
        warehouse=warehouse,
        batch_size=5,
        consumer=consumer,
    )

    result = sink.run()
    sink.close()

    assert result.events == len(messages)
    assert consumer.commits == math.ceil(len(messages) / 5)
    assert consumer.closed == 1
    assert _count(warehouse, "RAW_EVENT_ENVELOPES") == len(messages)
    warehouse.close()


def test_world_sink_rejects_wrong_partition_key_before_commit(tmp_path: Path):
    warehouse = _warehouse(tmp_path)
    dataset_dir = _dataset(tmp_path)
    message = _messages(dataset_dir)[0]
    message.key = "wrong-key"
    consumer = _Consumer([message])
    sink = WorldWarehouseSink(warehouse=warehouse, consumer=consumer)

    with pytest.raises(ValueError, match="expected"):
        sink.run()
    assert consumer.commits == 0
    warehouse.close()
