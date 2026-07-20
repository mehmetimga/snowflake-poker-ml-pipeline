from __future__ import annotations

import hashlib
import json
from itertools import combinations

import numpy as np
import pandas as pd
import pytest
import torch

from pipeline.dl.graph_dataset import (
    PAIR_GRAPH_FEATURES,
    RESOURCE_NODE_FEATURES,
    RESOURCE_TYPES,
    ROOT_USER_FEATURES,
    USER_EDGE_FEATURES,
    build_source_graph_arrays,
)
from pipeline.dl.graph_models import TemporalHeteroGraphSAGE, masked_mean
from pipeline.dl.graph_train import FeatureNormalizer, GraphTrainingConfig
from pipeline.ml.public_pair_baseline import (
    PublicPairBaselineConfig,
    train_public_pair_baseline,
)


PLAYERS = tuple(f"p{index}" for index in range(6))


def _hand(hand_id: str, played_at: str, table_id: str = "table-1") -> dict:
    return {
        "payload": {
            "hand_id": hand_id,
            "played_at": played_at,
            "table_id": table_id,
            "players": [{"player_id": player_id} for player_id in PLAYERS],
            "actions": [
                {
                    "player_id": player_id,
                    "action_type": "raise" if index == 0 else "fold",
                    "street": "flop" if index < 2 else "preflop",
                }
                for index, player_id in enumerate(PLAYERS)
            ],
        }
    }


def _frame(definitions: list[tuple[str, str]]) -> pd.DataFrame:
    rows = []
    for hand_id, played_at in definitions:
        for player_a, player_b in combinations(PLAYERS, 2):
            rows.append(
                {
                    "event_id": f"{hand_id}-{player_a}-{player_b}",
                    "hand_id": hand_id,
                    "played_at": played_at,
                    "pair_key": f"{player_a}:{player_b}",
                    "benchmark_split": "train",
                    "target": int(player_a == "p0" and player_b == "p1"),
                }
            )
    return pd.DataFrame(rows)


def _context(user: str, effective_at: str, device: str, network: str) -> dict:
    return {
        "payload": {
            "user_id": user,
            "effective_at": effective_at,
            "account_created_at": "2020-01-01T00:00:00Z",
            "skill_rating": 0.5,
            "bankroll_bucket": "medium",
            "preferred_stake_bucket": "low",
            "kyc_level": "basic",
            "account_status": "active",
            "country_bucket": "TR",
            "timezone": "Europe/Istanbul",
            "device_id": device,
            "network_cluster_id": network,
        }
    }


def test_graph_builder_excludes_current_future_and_equal_timestamp_edges():
    definitions = [
        ("h0", "2026-01-01T00:00:00Z"),
        ("h1", "2026-01-01T00:01:00Z"),
        ("h1b", "2026-01-01T00:01:00Z"),
        ("h2", "2026-01-01T00:02:00Z"),
    ]
    frame = _frame(definitions)
    contexts = [
        _context(player, "2025-12-31T23:59:00Z", f"d-{player}", "n-1")
        for player in PLAYERS[1:]
    ]
    contexts.append(_context("p0", "2026-01-01T00:00:30Z", "d-p1", "n-1"))
    sessions = [
        {
            "payload": {
                "user_id": player,
                "session_id": f"s-{player}",
                "started_at": "2025-12-31T23:59:30Z",
            }
        }
        for player in PLAYERS
    ]
    links = [
        {
            "payload": {
                "user_id": "p0",
                "related_user_id": "p1",
                "effective_at": "2026-01-01T00:00:30Z",
                "confidence_bucket": "high",
            }
        }
    ]
    outputs, audits = build_source_graph_arrays(
        {"train": frame},
        [_hand(*definition) for definition in definitions],
        contexts,
        sessions,
        links,
        max_user_neighbors=4,
        max_resource_neighbors=2,
    )
    arrays = outputs["train"]
    pair_indices = frame.index[frame["pair_key"] == "p0:p1"].to_numpy()
    h0, h1, h1b, h2 = pair_indices
    assert arrays["root_features"][h0, 0, 0] == pytest.approx(1.0)
    assert arrays["root_features"][h1, 0, 0] == pytest.approx(0.0)
    assert arrays["user_neighbor_masks"][h0].sum() == 0
    assert arrays["user_neighbor_masks"][h1, 0].sum() == 4
    assert arrays["user_edge_features"][h1, 0, 0, 0] == pytest.approx(np.log(2), rel=1e-3)
    assert arrays["user_edge_features"][h1b, 0, 0, 0] == pytest.approx(np.log(2), rel=1e-3)
    assert arrays["user_edge_features"][h2, 0, 0, 0] == pytest.approx(np.log(4), rel=1e-3)
    same_device_index = PAIR_GRAPH_FEATURES.index("same_device")
    direct_link_index = PAIR_GRAPH_FEATURES.index("direct_account_link")
    assert arrays["pair_graph_features"][h0, same_device_index] == 0
    assert arrays["pair_graph_features"][h1, same_device_index] == 1
    assert arrays["pair_graph_features"][h1, direct_link_index] == 1
    assert np.all(arrays["graph_last_edge_ns"] < arrays["example_played_ns"])
    assert audits["train"]["strictly_prior_edge_check"] is True


def test_graph_normalizer_uses_only_valid_neighbor_rows():
    values = np.asarray([[[1.0, 10.0], [100.0, 1000.0], [3.0, 30.0]]])
    mask = np.asarray([[1, 0, 1]], dtype=np.uint8)
    normalizer = FeatureNormalizer.fit(values, ("a", "b"), mask)
    transformed = normalizer.transform(values, mask)
    assert normalizer.means == pytest.approx((2.0, 20.0))
    assert np.all(transformed[:, 1] == 0)
    assert normalizer.to_dict()["fit_split"] == "train"


def test_masked_mean_and_graphsage_are_finite_and_inductive():
    values = torch.tensor([[[1.0], [3.0], [100.0]]])
    mask = torch.tensor([[1, 1, 0]], dtype=torch.bool)
    assert masked_mean(values, mask, dim=1).item() == pytest.approx(2.0)
    model = TemporalHeteroGraphSAGE(
        numeric_dim=4,
        categorical_cardinalities=(3, 2),
        root_feature_dim=len(ROOT_USER_FEATURES),
        user_edge_dim=len(USER_EDGE_FEATURES),
        resource_feature_dim=len(RESOURCE_NODE_FEATURES),
        pair_graph_dim=len(PAIR_GRAPH_FEATURES),
        width=8,
    )
    batch, user_neighbors, resource_neighbors = 5, 3, 2
    output = model(
        torch.randn(batch, 4),
        torch.tensor([[0, 1]] * batch),
        torch.randn(batch, 2, len(ROOT_USER_FEATURES)),
        torch.randn(batch, 2, user_neighbors, len(ROOT_USER_FEATURES)),
        torch.randn(batch, 2, user_neighbors, len(USER_EDGE_FEATURES)),
        torch.ones(batch, 2, user_neighbors, dtype=torch.bool),
        torch.randn(
            batch,
            2,
            len(RESOURCE_TYPES),
            resource_neighbors,
            len(RESOURCE_NODE_FEATURES),
        ),
        torch.ones(batch, 2, len(RESOURCE_TYPES), resource_neighbors, dtype=torch.bool),
        torch.randn(batch, len(PAIR_GRAPH_FEATURES)),
    )
    assert model.raw_id_embedding_count == 0
    assert output.shape == (batch,)
    assert torch.isfinite(output).all()


def test_graph_training_config_rejects_unknown_or_duplicate_benchmarks():
    with pytest.raises(ValueError, match="selected"):
        GraphTrainingConfig(benchmarks=("unknown",))
    with pytest.raises(ValueError, match="unique"):
        GraphTrainingConfig(benchmarks=("cold_start", "cold_start"))


def _hash(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_public_baseline_never_reads_or_outputs_challenge(tmp_path):
    dataset = tmp_path / "dataset"
    output = tmp_path / "baseline"
    split_dir = dataset / "dgx" / "new_relationship"
    split_dir.mkdir(parents=True)
    schema = {
        "challenge_labels_public": False,
        "numeric_feature_columns": ["amount"],
        "categorical_feature_columns": ["status"],
    }
    (dataset / "schema.json").write_text(json.dumps(schema))
    artifacts = {"schema.json": _hash(dataset / "schema.json")}
    for split, rows in (("train", 40), ("validation", 20), ("test", 20)):
        frame = pd.DataFrame(
            {
                "event_id": [f"{split}-{index}" for index in range(rows)],
                "hand_id": [f"{split}-h-{index // 2}" for index in range(rows)],
                "pair_key": [f"p0:p{index + 1}" for index in range(rows)],
                "benchmark_split": split,
                "amount": np.linspace(0, 1, rows),
                "status": np.where(np.arange(rows) % 2, "matched", "missing"),
                "target": np.asarray([0, 1] * (rows // 2), dtype=np.int8),
            }
        )
        path = split_dir / f"{split}.parquet"
        frame.to_parquet(path, index=False)
        artifacts[f"dgx/new_relationship/{split}.parquet"] = _hash(path)
    manifest = {
        "dataset_id": "unit-graph-v1",
        "feature_definition_version": "pair-features-v1",
        "challenge_labels_public": False,
        "artifacts": artifacts,
    }
    (dataset / "manifest.json").write_text(json.dumps(manifest))
    metrics = train_public_pair_baseline(
        PublicPairBaselineConfig(
            dataset_dir=dataset,
            output_dir=output,
            iterations=2,
            depth=2,
            early_stopping_rounds=1,
        )
    )
    predictions = pd.read_parquet(output / "predictions.parquet")
    assert metrics["challenge_artifacts_read"] is False
    assert metrics["challenge_labels_used"] is False
    assert set(predictions["split"]) == {"validation", "test"}
