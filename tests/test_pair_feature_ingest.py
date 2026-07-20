from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from datetime import timedelta
import uuid

from pipeline.config import Settings
from pipeline.features import PairFeatureCore
from pipeline.events import PairHandLabel
from pipeline.kafka.pair_feature_sink import PairFeatureWarehouseSink
from pipeline.warehouse.duckdb import DuckDBWarehouse
from pipeline.warehouse.migrate import run_migrations
from pipeline.warehouse.pair_features import (
    PairFeatureIngestRecord,
    load_pair_feature_events,
)
from pipeline.warehouse.pair_labels import load_pair_labels
from tests.test_pair_features_v1 import _enriched_hand, _hand_events


def _warehouse(tmp_path: Path) -> DuckDBWarehouse:
    warehouse = DuckDBWarehouse(
        Settings(
            _env_file=None,
            WAREHOUSE_BACKEND="duckdb",
            DUCKDB_PATH=tmp_path / "pair-features.duckdb",
        )
    )
    run_migrations(warehouse)
    return warehouse


def _events():
    return PairFeatureCore().process_many(_enriched_hand(_hand_events(1)[0]))


def test_pair_feature_loader_is_idempotent_and_latest_view_has_fifteen_rows(tmp_path):
    warehouse = _warehouse(tmp_path)
    records = [
        PairFeatureIngestRecord(
            envelope=event.model_dump(mode="json"),
            topic="poker.pair-features.v1",
            partition=index % 2,
            offset=index,
        )
        for index, event in enumerate(_events())
    ]

    first = load_pair_feature_events(warehouse, records)
    replay = load_pair_feature_events(warehouse, records)

    assert first == replay
    assert first.events == 15
    assert first.hands == 1
    assert first.pairs == 15
    assert int(
        warehouse.fetch_df("SELECT COUNT(*) AS n FROM PAIR_FEATURE_EVENTS").iloc[0]["n"]
    ) == 15
    assert int(
        warehouse.fetch_df("SELECT COUNT(*) AS n FROM PAIR_FEATURE_LATEST").iloc[0]["n"]
    ) == 15
    warehouse.close()


def test_pair_labels_are_idempotent_and_join_point_in_time_features(tmp_path):
    warehouse = _warehouse(tmp_path)
    events = _events()
    load_pair_feature_events(
        warehouse,
        [
            PairFeatureIngestRecord(envelope=event.model_dump(mode="json"))
            for event in events
        ],
    )
    labels = [
        PairHandLabel(
            example_id=uuid.uuid5(uuid.NAMESPACE_URL, f"label:{event.event_id}"),
            dataset_id=event.dataset_id,
            dataset_split=event.dataset_split,
            hand_id=event.payload.hand_id,
            pair_key=event.payload.pair_key,
            player_a=event.payload.player_a,
            player_b=event.payload.player_b,
            is_collusive=False,
            collusion_pair_id=None,
            label_available_at=event.payload.played_at + timedelta(days=7),
        )
        for event in events
    ]

    assert load_pair_labels(warehouse, labels) == 15
    assert load_pair_labels(warehouse, labels) == 15
    assert int(warehouse.fetch_df("SELECT COUNT(*) AS n FROM PAIR_LABELS").iloc[0]["n"]) == 15
    training = warehouse.fetch_df("SELECT * FROM PAIR_TRAINING_EXAMPLES")
    assert len(training) == 15
    assert not training["target"].any()
    assert set(training["feature_definition_version"]) == {"pair-features-v1"}
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


def test_pair_feature_sink_validates_key_and_commits_after_warehouse_write(tmp_path):
    warehouse = _warehouse(tmp_path)
    events = _events()
    messages = [
        SimpleNamespace(
            value=event.model_dump(mode="json"),
            key=event.payload.pair_key,
            topic="poker.pair-features.v1",
            partition=0,
            offset=index,
            timestamp=1_700_000_000_000 + index,
        )
        for index, event in enumerate(events)
    ]
    consumer = _Consumer(messages)
    sink = PairFeatureWarehouseSink(
        warehouse=warehouse,
        topic="poker.pair-features.v1",
        batch_size=20,
        consumer=consumer,
    )

    result = sink.run()
    sink.close()

    assert result.events == 15
    assert consumer.commits == 1
    assert consumer.closed == 1
    warehouse.close()
