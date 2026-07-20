from __future__ import annotations

import hashlib
import json
from itertools import combinations

import numpy as np
import pandas as pd
import pytest
import torch

from pipeline.dl.history_dataset import (
    PAIR_CURRENT_COLUMNS,
    PAIR_HISTORY_FEATURES,
    USER_HISTORY_FEATURES,
    build_split_history_arrays,
    sha256_file,
    write_deterministic_npz,
)
from pipeline.dl.history_models import (
    HistoryEncoder,
    HistoryPretrainer,
    PairHistoryRiskModel,
    self_supervised_history_loss,
)
from pipeline.dl.history_train import (
    PairHistoryConfig,
    SequenceNormalizer,
    train_pair_history_model,
)


PLAYERS = tuple(f"p{index}" for index in range(6))


def _hand(hand_id: str, played_at: str, marker: float) -> dict:
    players = [
        {
            "player_id": player_id,
            "position": ("SB", "BB", "UTG", "MP", "CO", "BTN")[index],
            "stack_start": 200.0,
            "won_amount": marker if index == 0 else 0.0,
        }
        for index, player_id in enumerate(PLAYERS)
    ]
    actions = [
        {
            "player_id": player_id,
            "action_type": "raise" if index == 0 else "fold",
            "street": "preflop",
            "amount": marker + index,
            "sequence_no": index,
        }
        for index, player_id in enumerate(PLAYERS)
    ]
    return {
        "payload": {
            "hand_id": hand_id,
            "played_at": played_at,
            "big_blind": 2.0,
            "pot_size": 20.0 + marker,
            "players": players,
            "actions": actions,
        }
    }


def _pair_frame(hands: list[tuple[str, str, float]], split: str = "train") -> pd.DataFrame:
    rows = []
    for hand_id, played_at, marker in hands:
        for player_a, player_b in combinations(PLAYERS, 2):
            row = {
                "event_id": f"{hand_id}-{player_a}-{player_b}",
                "hand_id": hand_id,
                "played_at": played_at,
                "pair_key": f"{player_a}:{player_b}",
                "benchmark_split": split,
                "target": int(player_a == "p0" and player_b == "p1"),
            }
            row.update({column: marker for column in PAIR_CURRENT_COLUMNS})
            rows.append(row)
    return pd.DataFrame(rows)


def test_history_builder_uses_only_strictly_earlier_timestamp_groups():
    definitions = [
        ("h0", "2026-01-01T00:00:00Z", 1.0),
        ("h1", "2026-01-01T00:01:00Z", 2.0),
        ("h1b", "2026-01-01T00:01:00Z", 3.0),
        ("h2", "2026-01-01T00:02:00Z", 4.0),
    ]
    frame = _pair_frame(definitions)
    arrays, audit = build_split_history_arrays(
        frame,
        [_hand(*definition) for definition in definitions],
        max_history=4,
    )

    h0 = frame.index[frame["hand_id"] == "h0"].to_numpy()
    h1 = frame.index[frame["hand_id"] == "h1"].to_numpy()
    h1b = frame.index[frame["hand_id"] == "h1b"].to_numpy()
    h2 = frame.index[frame["hand_id"] == "h2"].to_numpy()
    assert arrays["pair_masks"][h0].sum() == 0
    assert np.all(arrays["pair_masks"][h1].sum(axis=1) == 1)
    assert np.all(arrays["pair_masks"][h1b].sum(axis=1) == 1)
    assert np.all(arrays["pair_masks"][h2].sum(axis=1) == 3)
    assert arrays["pair_sequences"][h1[0], -1, 0] == pytest.approx(1.0)
    assert arrays["pair_sequences"][h1b[0], -1, 0] == pytest.approx(1.0)
    assert arrays["pair_sequences"][h2[0], -3:, 0].tolist() == pytest.approx(
        [1.0, 2.0, 3.0]
    )
    assert np.all(arrays["pair_last_seen_ns"] < arrays["example_played_ns"])
    assert audit["strictly_prior_timestamp_check"] is True
    assert audit["equal_timestamp_isolation"] is True


def test_deterministic_npz_has_stable_bytes(tmp_path):
    arrays = {
        "z": np.arange(10, dtype=np.int64),
        "a": np.asarray([[1.0, 2.0]], dtype=np.float16),
    }
    first, second = tmp_path / "first.npz", tmp_path / "second.npz"
    write_deterministic_npz(first, arrays)
    write_deterministic_npz(second, arrays)
    assert sha256_file(first) == sha256_file(second)
    with np.load(first, allow_pickle=False) as loaded:
        assert np.array_equal(loaded["z"], arrays["z"])


def test_sequence_normalizer_fits_valid_training_steps_only():
    sequences = np.zeros((2, 3, 2), dtype=np.float16)
    masks = np.asarray([[0, 1, 1], [0, 0, 1]], dtype=np.uint8)
    sequences[0, 1:] = [[1, 10], [3, 30]]
    sequences[1, 2] = [5, 50]
    normalizer = SequenceNormalizer.fit(sequences, masks, ("a", "b"))
    transformed = normalizer.transform(sequences, masks)

    assert normalizer.means == pytest.approx((3.0, 30.0))
    assert normalizer.valid_steps == 3
    assert np.all(transformed[~masks.astype(bool)] == 0)
    assert normalizer.to_dict()["fit_split"] == "train"


def test_history_models_forward_and_self_supervised_objectives_are_finite():
    torch.manual_seed(42)
    user_encoder = HistoryEncoder(5, 4, width=8, heads=2, layers=1, dropout=0.0)
    pair_encoder = HistoryEncoder(3, 4, width=8, heads=2, layers=1, dropout=0.0)
    pretrainer = HistoryPretrainer(user_encoder)
    sequence = torch.randn(6, 4, 5)
    mask = torch.tensor([[0, 0, 1, 1]] * 6, dtype=torch.bool)
    loss, components = self_supervised_history_loss(pretrainer, sequence, mask)
    loss.backward()

    assert torch.isfinite(loss)
    assert set(components) == {
        "masked_reconstruction_loss",
        "next_step_loss",
        "contrastive_loss",
    }
    model = PairHistoryRiskModel(4, (3, 2), user_encoder, pair_encoder)
    output = model(
        torch.randn(6, 4),
        torch.tensor([[0, 1]] * 6),
        sequence,
        mask,
        sequence,
        mask,
        torch.randn(6, 4, 3),
        mask,
    )
    assert output.shape == (6,)
    assert torch.isfinite(output).all()


def test_pair_history_config_rejects_invalid_encoder_shape():
    with pytest.raises(ValueError, match="divisible"):
        PairHistoryConfig(encoder_width=10, encoder_heads=4)


def _hash(path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def test_tiny_phase10_training_writes_label_safe_artifacts(tmp_path):
    pair_root = tmp_path / "pair"
    history_root = tmp_path / "history"
    baseline_root = tmp_path / "baseline"
    output_root = tmp_path / "output"
    (pair_root / "dgx" / "cold_start").mkdir(parents=True)
    (history_root / "splits").mkdir(parents=True)
    baseline_root.mkdir()
    pair_schema = {
        "challenge_labels_public": False,
        "numeric_feature_columns": ["amount"],
        "categorical_feature_columns": ["status"],
    }
    (pair_root / "schema.json").write_text(json.dumps(pair_schema))
    pair_artifacts = {"schema.json": _hash(pair_root / "schema.json")}
    frames = {}
    for split, rows in (("train", 40), ("validation", 20), ("test", 20)):
        frame = pd.DataFrame(
            {
                "event_id": [f"{split}-event-{index}" for index in range(rows)],
                "hand_id": [f"{split}-hand-{index // 2}" for index in range(rows)],
                "pair_key": [f"p0:p{index + 1}" for index in range(rows)],
                "benchmark_split": split,
                "amount": np.linspace(0, 1, rows),
                "status": np.where(np.arange(rows) % 2, "matched", "missing"),
                "target": np.asarray([0, 1] * (rows // 2), dtype=np.int8),
            }
        )
        pair_path = pair_root / "dgx" / "cold_start" / f"{split}.parquet"
        frame.to_parquet(pair_path, index=False)
        pair_artifacts[f"dgx/cold_start/{split}.parquet"] = _hash(pair_path)
        frames[split] = frame
    pair_manifest = {
        "dataset_id": "unit-history-v1",
        "feature_definition_version": "pair-features-v1",
        "challenge_labels_public": False,
        "artifacts": pair_artifacts,
    }
    (pair_root / "manifest.json").write_text(json.dumps(pair_manifest))
    history_schema = {
        "challenge_labels_public": False,
        "user_history_features": list(USER_HISTORY_FEATURES),
        "pair_history_features": list(PAIR_HISTORY_FEATURES),
    }
    (history_root / "schema.json").write_text(json.dumps(history_schema))
    history_artifacts = {"schema.json": _hash(history_root / "schema.json")}
    split_audits = {}
    generator = np.random.default_rng(42)
    for split, frame in frames.items():
        rows = len(frame)
        played = np.arange(rows, dtype=np.int64) + 100
        user_sequences = generator.normal(
            size=(rows * 2, 4, len(USER_HISTORY_FEATURES))
        ).astype(np.float16)
        pair_sequences = generator.normal(
            size=(rows, 4, len(PAIR_HISTORY_FEATURES))
        ).astype(np.float16)
        user_masks = np.ones((rows * 2, 4), dtype=np.uint8)
        pair_masks = np.ones((rows, 4), dtype=np.uint8)
        arrays = {
            "event_ids": frame["event_id"].to_numpy(dtype=np.str_),
            "example_played_ns": played,
            "labels": frame["target"].to_numpy(dtype=np.int8),
            "pair_last_seen_ns": played - 1,
            "pair_masks": pair_masks,
            "pair_sequences": pair_sequences,
            "user_a_indices": np.arange(rows, dtype=np.int32) * 2,
            "user_b_indices": np.arange(rows, dtype=np.int32) * 2 + 1,
            "user_last_seen_ns": np.repeat(played - 1, 2),
            "user_masks": user_masks,
            "user_sequences": user_sequences,
        }
        history_path = history_root / "splits" / f"{split}.npz"
        write_deterministic_npz(history_path, arrays)
        history_artifacts[f"splits/{split}.npz"] = _hash(history_path)
        from pipeline.dl.history_dataset import event_alignment_sha256

        split_audits[split] = {
            "event_alignment_sha256": event_alignment_sha256(frame["event_id"]),
        }
    history_manifest = {
        "phase": 10,
        "dataset_id": "unit-history-v1",
        "benchmark": "cold_start",
        "feature_definition_version": "pair-features-v1",
        "source_pair_manifest_sha256": _hash(pair_root / "manifest.json"),
        "challenge_artifacts_read": False,
        "challenge_labels_public": False,
        "artifacts": history_artifacts,
        "splits": split_audits,
    }
    (history_root / "manifest.json").write_text(json.dumps(history_manifest))
    pd.DataFrame(
        {
            "split": "test",
            "event_id": frames["test"]["event_id"],
            "calibrated_probability": np.linspace(0.05, 0.95, len(frames["test"])),
        }
    ).to_parquet(baseline_root / "predictions.parquet", index=False)
    (baseline_root / "metrics.json").write_text(
        json.dumps(
            {
                "run_id": "catboost-unit",
                "benchmark": "cold_start",
                "dataset_manifest_sha256": _hash(pair_root / "manifest.json"),
                "reports": {
                    "catboost": {
                        "test": {
                            "pr_auc": 0.5,
                            "recall_at_alert_budget": 0.1,
                            "f1": 0.1,
                        }
                    }
                },
            }
        )
    )

    summary = train_pair_history_model(
        PairHistoryConfig(
            history_dataset_dir=history_root,
            pair_dataset_dir=pair_root,
            baseline_dir=baseline_root,
            output_dir=output_root,
            pretrain_epochs=1,
            epochs=1,
            pretrain_batch_size=8,
            batch_size=8,
            patience=1,
            bootstrap_samples=5,
            num_workers=0,
            encoder_width=8,
            encoder_heads=2,
            encoder_layers=1,
            device_name="cpu",
        )
    )

    assert summary["phase"] == 10
    assert summary["pretraining_labels_used"] is False
    assert summary["challenge_labels_used"] is False
    assert (output_root / "pretraining" / "user_encoder.pt").is_file()
    assert (output_root / "history_transformer" / "model.pt").is_file()
    assert (output_root / "artifact_manifest.json").is_file()
