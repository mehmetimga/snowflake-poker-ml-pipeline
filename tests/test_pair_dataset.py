from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from pipeline.generator import (
    FrozenDatasetConfig,
    RealtimeWorldConfig,
    build_realtime_world_dataset,
)
from pipeline.ml.pair_dataset import (
    MODEL_FEATURE_COLUMNS,
    PairDatasetBuildConfig,
    build_pair_datasets,
)


def _world(tmp_path: Path) -> Path:
    source = tmp_path / "world"
    build_realtime_world_dataset(
        source,
        RealtimeWorldConfig(
            dataset_id="pair-dataset-test-v1",
            frozen=FrozenDatasetConfig(
                train_hands=24,
                validation_hands=5,
                test_hands=5,
                challenge_hands=4,
                n_players=24,
                n_tables=3,
                n_colluding_pairs=6,
                seed=1421,
            ),
        ),
    )
    return source


def _build(source: Path, output: Path):
    return build_pair_datasets(
        PairDatasetBuildConfig(source_dir=source, output_dir=output)
    )


def test_pair_dataset_is_reproducible_and_keeps_public_features_label_free(tmp_path):
    source = _world(tmp_path)
    first = _build(source, tmp_path / "first")
    second = _build(source, tmp_path / "second")

    assert first == second
    assert first["artifacts"] == second["artifacts"]
    assert first["feature_definition_version"] == "pair-features-v1"
    assert first["challenge_labels_public"] is False

    challenge_root = tmp_path / "first" / "benchmarks" / "challenge" / "challenge"
    public = pd.read_parquet(challenge_root / "features.parquet")
    assert set(MODEL_FEATURE_COLUMNS).issubset(public.columns)
    assert not {
        "is_collusive",
        "collusion_pair_id",
        "label_available_at",
        "target",
    } & set(public.columns)
    assert not (challenge_root / "labels").exists()
    assert (challenge_root / "private_labels" / "pair_labels.parquet").exists()
    assert not (tmp_path / "first" / "dgx" / "challenge").exists()


def test_cold_start_is_hand_atomic_and_player_disjoint(tmp_path):
    source = _world(tmp_path)
    output = tmp_path / "pairs"
    manifest = _build(source, output)
    populations: dict[str, set[str]] = {}

    for split, hands in (("train", 24), ("validation", 5), ("test", 5), ("challenge", 4)):
        details = manifest["benchmarks"]["cold_start"]["splits"][split]
        features = pd.read_parquet(
            output / "benchmarks" / "cold_start" / split / "features.parquet"
        )
        assert details["hands"] == hands
        assert details["feature_rows"] == hands * 15
        assert features.groupby("hand_id")["benchmark_split"].nunique().max() == 1
        populations[split] = set(features["player_a"]) | set(features["player_b"])

    names = list(populations)
    for index, left in enumerate(names):
        for right in names[index + 1 :]:
            assert populations[left].isdisjoint(populations[right])


def test_temporal_and_new_relationship_benchmarks_enforce_leakage_rules(tmp_path):
    source = _world(tmp_path)
    output = tmp_path / "pairs"
    manifest = _build(source, output)

    temporal = {}
    for split in ("train", "validation", "test"):
        temporal[split] = pd.read_parquet(
            output / "benchmarks" / "temporal" / split / "features.parquet"
        )
        assert temporal[split].groupby("hand_id")["benchmark_split"].nunique().max() == 1
    assert temporal["train"]["played_at"].max() < temporal["validation"]["played_at"].min()
    assert temporal["validation"]["played_at"].max() < temporal["test"]["played_at"].min()
    train_users = set(temporal["train"]["player_a"]) | set(temporal["train"]["player_b"])
    test_users = set(temporal["test"]["player_a"]) | set(temporal["test"]["player_b"])
    assert train_users & test_users

    protected = manifest["benchmarks"]["new_relationship"]["protected_positive_pairs"]
    assert not set(protected["train"]) & (
        set(protected["validation"]) | set(protected["test"])
    )
    for split in ("train", "validation", "test"):
        features = pd.read_parquet(
            output / "benchmarks" / "new_relationship" / split / "features.parquet"
        )
        assert features.groupby("hand_id")["benchmark_split"].nunique().max() == 1
        if split in ("validation", "test"):
            train_features = pd.read_parquet(
                output / "benchmarks" / "new_relationship" / "train" / "features.parquet"
            )
            assert not set(protected[split]) & set(train_features["pair_key"])


def test_dgx_exports_match_feature_and_label_counts(tmp_path):
    source = _world(tmp_path)
    output = tmp_path / "pairs"
    manifest = _build(source, output)

    for benchmark in ("cold_start", "temporal", "new_relationship"):
        for split in ("train", "validation", "test"):
            frame = pd.read_parquet(output / "dgx" / benchmark / f"{split}.parquet")
            details = manifest["dgx"][benchmark][split]
            assert len(frame) == details["rows"]
            assert int(frame["target"].sum()) == details["positive_rows"]
            assert set(frame["target"].unique()).issubset({0, 1})
            assert set(MODEL_FEATURE_COLUMNS).issubset(frame.columns)

    schema = json.loads((output / "schema.json").read_text())
    assert schema["challenge_labels_public"] is False
    assert schema["target_column"] == "target"

