"""Phase 10 self-supervised history pretraining and pair-risk fine-tuning."""

from __future__ import annotations

import copy
import json
import math
import random
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score
from torch import nn
from torch.utils.data import DataLoader, Dataset, Subset

from pipeline.ml.pair_model import (
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)

from .history_dataset import (
    HISTORY_SPLITS,
    PAIR_HISTORY_FEATURES,
    USER_HISTORY_FEATURES,
    event_alignment_sha256,
    load_history_split,
    sha256_file,
)
from .history_models import (
    HistoryEncoder,
    HistoryPretrainer,
    PairHistoryRiskModel,
    self_supervised_history_loss,
)
from .pair_challengers import (
    NeuralPairPreprocessor,
    _baseline,
    _load_inputs,
    challenger_gate,
    paired_hand_bootstrap_pr_auc,
)


DeviceName = Literal["auto", "cpu", "cuda"]
MODEL_NAME = "history_transformer"


@dataclass(frozen=True)
class PairHistoryConfig:
    history_dataset_dir: Path = Path("data/datasets/pair-sequences-full-v2")
    pair_dataset_dir: Path = Path("data/datasets/pair-full-v2")
    baseline_dir: Path = Path("models/pair-catboost-full-v2")
    output_dir: Path = Path("models/pair-history-full-v2")
    pretrain_epochs: int = 5
    epochs: int = 15
    pretrain_batch_size: int = 512
    batch_size: int = 1024
    patience: int = 4
    learning_rate: float = 1e-3
    pretrain_learning_rate: float = 1e-3
    weight_decay: float = 1e-4
    positive_class_weight: float = 100.0
    max_alert_rate: float = 0.02
    minimum_relative_pr_gain: float = 0.02
    bootstrap_samples: int = 200
    random_seed: int = 42
    num_workers: int = 4
    encoder_width: int = 32
    encoder_heads: int = 4
    encoder_layers: int = 2
    device_name: DeviceName = "auto"
    overwrite: bool = False

    def __post_init__(self) -> None:
        positive = (
            self.pretrain_epochs,
            self.epochs,
            self.pretrain_batch_size,
            self.batch_size,
            self.patience,
            self.bootstrap_samples,
            self.encoder_width,
            self.encoder_heads,
            self.encoder_layers,
        )
        if any(value < 1 for value in positive):
            raise ValueError("training counts and model dimensions must be positive")
        if self.encoder_width % self.encoder_heads:
            raise ValueError("encoder width must be divisible by heads")
        if self.learning_rate <= 0 or self.pretrain_learning_rate <= 0:
            raise ValueError("learning rates must be positive")
        if self.weight_decay < 0 or self.positive_class_weight <= 0:
            raise ValueError("invalid weight or class-weight setting")
        if not 0 < self.max_alert_rate <= 1 or self.minimum_relative_pr_gain < 0:
            raise ValueError("invalid promotion-gate setting")
        if self.num_workers < 0:
            raise ValueError("num_workers cannot be negative")


@dataclass(frozen=True)
class SequenceNormalizer:
    feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    valid_steps: int

    @classmethod
    def fit(
        cls,
        sequences: np.ndarray,
        masks: np.ndarray,
        feature_names: tuple[str, ...],
    ) -> "SequenceNormalizer":
        if sequences.ndim != 3 or sequences.shape[:2] != masks.shape:
            raise ValueError("sequence normalizer received incompatible arrays")
        if sequences.shape[2] != len(feature_names):
            raise ValueError("sequence feature names disagree with the tensor")
        valid = masks.astype(bool, copy=False)
        count = int(valid.sum())
        if count < 1:
            raise ValueError("cannot fit a sequence normalizer without valid steps")
        sums = np.zeros(sequences.shape[2], dtype=np.float64)
        squared = np.zeros(sequences.shape[2], dtype=np.float64)
        for start in range(0, len(sequences), 8192):
            chunk = sequences[start : start + 8192].astype(np.float64)
            selected = chunk[valid[start : start + 8192]]
            sums += selected.sum(axis=0)
            squared += np.square(selected).sum(axis=0)
        means = sums / count
        variances = np.maximum(squared / count - np.square(means), 0.0)
        scales = np.sqrt(variances)
        scales[~np.isfinite(scales) | (scales < 1e-6)] = 1.0
        return cls(
            feature_names=feature_names,
            means=tuple(float(value) for value in means),
            scales=tuple(float(value) for value in scales),
            valid_steps=count,
        )

    def transform(self, sequences: np.ndarray, masks: np.ndarray) -> np.ndarray:
        if sequences.shape[:2] != masks.shape or sequences.shape[2] != len(self.means):
            raise ValueError("sequence transform arrays do not match the normalizer")
        output = np.empty_like(sequences, dtype=np.float16)
        means = np.asarray(self.means, dtype=np.float32)
        scales = np.asarray(self.scales, dtype=np.float32)
        for start in range(0, len(sequences), 8192):
            chunk = sequences[start : start + 8192].astype(np.float32)
            chunk = (chunk - means) / scales
            chunk *= masks[start : start + 8192, :, None]
            output[start : start + 8192] = chunk.astype(np.float16)
        if not np.isfinite(output).all():
            raise ValueError("normalized sequence contains non-finite values")
        return output

    def to_dict(self) -> dict[str, Any]:
        return {
            "fit_split": "train",
            "feature_names": list(self.feature_names),
            "means": list(self.means),
            "scales": list(self.scales),
            "valid_steps": self.valid_steps,
        }


class SequenceBankDataset(Dataset):
    def __init__(self, sequences: np.ndarray, masks: np.ndarray) -> None:
        if sequences.shape[:2] != masks.shape:
            raise ValueError("sequence bank arrays are not aligned")
        eligible = np.flatnonzero(masks.sum(axis=1) >= 2)
        if len(eligible) < 2:
            raise ValueError("self-supervised pretraining needs histories of length two")
        self.sequences = torch.from_numpy(sequences)
        self.masks = torch.from_numpy(masks.astype(bool, copy=False))
        self.eligible = eligible

    def __len__(self) -> int:
        return len(self.eligible)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, torch.Tensor]:
        selected = int(self.eligible[index])
        return self.sequences[selected], self.masks[selected]


class PairHistoryDataset(Dataset):
    def __init__(
        self,
        numeric: np.ndarray,
        categorical: np.ndarray,
        labels: np.ndarray,
        history: Mapping[str, np.ndarray],
    ) -> None:
        if len(numeric) != len(categorical) or len(numeric) != len(labels):
            raise ValueError("tabular tensors are not aligned")
        if len(labels) != len(history["pair_sequences"]):
            raise ValueError("pair histories are not aligned with examples")
        self.numeric = torch.from_numpy(numeric)
        self.categorical = torch.from_numpy(categorical)
        self.labels = torch.from_numpy(labels.astype(np.float32, copy=False))
        self.user_sequences = torch.from_numpy(history["user_sequences"])
        self.user_masks = torch.from_numpy(history["user_masks"].astype(bool, copy=False))
        self.user_a_indices = history["user_a_indices"].astype(np.int64, copy=False)
        self.user_b_indices = history["user_b_indices"].astype(np.int64, copy=False)
        self.pair_sequences = torch.from_numpy(history["pair_sequences"])
        self.pair_masks = torch.from_numpy(history["pair_masks"].astype(bool, copy=False))

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, ...]:
        user_a = int(self.user_a_indices[index])
        user_b = int(self.user_b_indices[index])
        return (
            self.numeric[index],
            self.categorical[index],
            self.user_sequences[user_a],
            self.user_masks[user_a],
            self.user_sequences[user_b],
            self.user_masks[user_b],
            self.pair_sequences[index],
            self.pair_masks[index],
            self.labels[index],
        )


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    if hasattr(torch.backends, "cudnn"):
        torch.backends.cudnn.benchmark = False


def _resolve_device(requested: DeviceName) -> torch.device:
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but is unavailable")
    if requested == "auto":
        requested = "cuda" if torch.cuda.is_available() else "cpu"
    return torch.device(requested)


def _load_phase10_inputs(
    config: PairHistoryConfig,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, pd.DataFrame],
    dict[str, dict[str, np.ndarray]],
    dict[str, Any],
    np.ndarray,
]:
    pair_manifest, pair_schema, frames = _load_inputs(
        config.pair_dataset_dir.resolve(), "cold_start"
    )
    pair_manifest_path = config.pair_dataset_dir.resolve() / "manifest.json"
    history_root = config.history_dataset_dir.resolve()
    history_manifest_path = history_root / "manifest.json"
    history_schema_path = history_root / "schema.json"
    if not history_manifest_path.is_file() or not history_schema_path.is_file():
        raise FileNotFoundError("Phase 10 history manifest and schema are required")
    history_manifest = json.loads(history_manifest_path.read_text())
    history_schema = json.loads(history_schema_path.read_text())
    if history_manifest["challenge_artifacts_read"] is not False:
        raise ValueError("Phase 10 dataset must not read challenge artifacts")
    if history_schema["challenge_labels_public"] is not False:
        raise ValueError("Phase 10 dataset exposes challenge labels")
    if history_manifest["source_pair_manifest_sha256"] != sha256_file(pair_manifest_path):
        raise ValueError("Phase 10 and pair dataset manifests disagree")
    if history_manifest["artifacts"]["schema.json"] != sha256_file(history_schema_path):
        raise ValueError("Phase 10 schema hash mismatch")
    histories: dict[str, dict[str, np.ndarray]] = {}
    for split in HISTORY_SPLITS:
        relative = f"splits/{split}.npz"
        path = history_root / relative
        if sha256_file(path) != history_manifest["artifacts"][relative]:
            raise ValueError(f"Phase 10 history hash mismatch: {relative}")
        history = load_history_split(path)
        expected_ids = frames[split]["event_id"].astype(str).to_numpy()
        if not np.array_equal(history["event_ids"].astype(str), expected_ids):
            raise ValueError(f"{split} history event alignment failed")
        if event_alignment_sha256(expected_ids) != history_manifest["splits"][split][
            "event_alignment_sha256"
        ]:
            raise ValueError(f"{split} event alignment hash mismatch")
        expected_labels = frames[split]["target"].astype(np.int8).to_numpy()
        if not np.array_equal(history["labels"], expected_labels):
            raise ValueError(f"{split} history labels are not aligned")
        played = history["example_played_ns"]
        if np.any(history["pair_last_seen_ns"] >= played):
            raise ValueError(f"{split} pair history contains current/future events")
        if np.any(history["user_last_seen_ns"][history["user_a_indices"]] >= played):
            raise ValueError(f"{split} user A history contains current/future events")
        if np.any(history["user_last_seen_ns"][history["user_b_indices"]] >= played):
            raise ValueError(f"{split} user B history contains current/future events")
        histories[split] = history
    baseline_metrics, baseline_test = _baseline(
        config.baseline_dir.resolve(), frames, sha256_file(pair_manifest_path)
    )
    return (
        pair_manifest,
        pair_schema,
        frames,
        histories,
        baseline_metrics,
        baseline_test,
    )


def _loader(
    dataset: Dataset,
    *,
    batch_size: int,
    shuffle: bool,
    num_workers: int,
    pin_memory: bool,
    seed: int,
) -> DataLoader:
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=pin_memory,
        persistent_workers=num_workers > 0,
        generator=torch.Generator().manual_seed(seed) if shuffle else None,
    )


def _pretrain_encoder(
    name: str,
    sequences: np.ndarray,
    masks: np.ndarray,
    *,
    input_dim: int,
    max_history: int,
    config: PairHistoryConfig,
    device: torch.device,
    seed: int,
) -> tuple[HistoryEncoder, dict[str, Any], dict[str, torch.Tensor]]:
    _seed_everything(seed)
    bank = SequenceBankDataset(sequences, masks)
    indices = np.arange(len(bank))
    validation_indices = indices[indices % 20 == 0]
    train_indices = indices[indices % 20 != 0]
    if len(validation_indices) < 2 or len(train_indices) < 2:
        raise ValueError("pretraining bank is too small for a train-only holdout")
    pin_memory = device.type == "cuda"
    train_loader = _loader(
        Subset(bank, train_indices.tolist()),
        batch_size=config.pretrain_batch_size,
        shuffle=True,
        num_workers=config.num_workers,
        pin_memory=pin_memory,
        seed=seed,
    )
    validation_loader = _loader(
        Subset(bank, validation_indices.tolist()),
        batch_size=config.pretrain_batch_size,
        shuffle=False,
        num_workers=config.num_workers,
        pin_memory=pin_memory,
        seed=seed,
    )
    encoder = HistoryEncoder(
        input_dim,
        max_history,
        width=config.encoder_width,
        heads=config.encoder_heads,
        layers=config.encoder_layers,
    ).to(device)
    model = HistoryPretrainer(encoder).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=config.pretrain_learning_rate,
        weight_decay=config.weight_decay,
    )
    best_loss = math.inf
    best_state: dict[str, torch.Tensor] | None = None
    history: list[dict[str, Any]] = []
    started = time.perf_counter()
    for epoch in range(1, config.pretrain_epochs + 1):
        model.train()
        train_totals = np.zeros(4, dtype=np.float64)
        train_rows = 0
        for sequence, mask in train_loader:
            sequence = sequence.to(device, non_blocking=True)
            mask = mask.to(device, non_blocking=True)
            optimizer.zero_grad(set_to_none=True)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                loss, components = self_supervised_history_loss(model, sequence, mask)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 5.0)
            optimizer.step()
            rows = len(sequence)
            train_totals += rows * np.asarray(
                [
                    float(loss.detach()),
                    float(components["masked_reconstruction_loss"].detach()),
                    float(components["next_step_loss"].detach()),
                    float(components["contrastive_loss"].detach()),
                ]
            )
            train_rows += rows
        model.eval()
        validation_total, validation_rows = 0.0, 0
        with torch.no_grad(), torch.random.fork_rng(
            devices=[device] if device.type == "cuda" else []
        ):
            torch.manual_seed(seed + 10_000)
            for sequence, mask in validation_loader:
                sequence = sequence.to(device, non_blocking=True)
                mask = mask.to(device, non_blocking=True)
                with torch.autocast(
                    device_type=device.type,
                    dtype=torch.bfloat16,
                    enabled=device.type == "cuda",
                ):
                    loss, _ = self_supervised_history_loss(model, sequence, mask)
                validation_total += float(loss) * len(sequence)
                validation_rows += len(sequence)
        validation_loss = validation_total / validation_rows
        values = train_totals / train_rows
        epoch_metrics = {
            "epoch": epoch,
            "train_loss": float(values[0]),
            "train_masked_reconstruction_loss": float(values[1]),
            "train_next_step_loss": float(values[2]),
            "train_contrastive_loss": float(values[3]),
            "validation_loss": validation_loss,
        }
        history.append(epoch_metrics)
        print(
            f"[pair-history][pretrain-{name}] epoch={epoch} "
            f"train_loss={values[0]:.6f} validation_loss={validation_loss:.6f}",
            flush=True,
        )
        if validation_loss < best_loss:
            best_loss = validation_loss
            best_state = {
                key: value.detach().cpu().clone()
                for key, value in encoder.state_dict().items()
            }
    if best_state is None:
        raise RuntimeError(f"{name} history pretraining produced no checkpoint")
    encoder.load_state_dict(best_state)
    return (
        encoder,
        {
            "name": name,
            "labels_used": False,
            "objectives": [
                "masked_step_reconstruction",
                "next_step_prediction",
                "contrastive_window_consistency",
            ],
            "train_sequences": len(train_indices),
            "validation_sequences": len(validation_indices),
            "best_validation_loss": best_loss,
            "epochs_ran": len(history),
            "seconds": time.perf_counter() - started,
            "history": history,
        },
        best_state,
    )


def _move_batch(batch: tuple[torch.Tensor, ...], device: torch.device) -> tuple[torch.Tensor, ...]:
    return tuple(value.to(device, non_blocking=True) for value in batch)


def _forward(model: PairHistoryRiskModel, batch: tuple[torch.Tensor, ...]) -> torch.Tensor:
    return model(*batch[:-1])


def _predict(
    model: PairHistoryRiskModel,
    loader: DataLoader,
    loss_function: nn.Module,
    device: torch.device,
) -> tuple[float, np.ndarray]:
    model.eval()
    total, rows = 0.0, 0
    probabilities: list[np.ndarray] = []
    with torch.no_grad():
        for raw_batch in loader:
            batch = _move_batch(raw_batch, device)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                logits = _forward(model, batch)
                loss = loss_function(logits, batch[-1])
            total += float(loss) * len(logits)
            rows += len(logits)
            probabilities.append(torch.sigmoid(logits.float()).cpu().numpy())
    return total / max(rows, 1), np.concatenate(probabilities).astype(np.float64)


def _latency(
    model: PairHistoryRiskModel,
    dataset: PairHistoryDataset,
    device: torch.device,
) -> dict[str, Any]:
    loader = DataLoader(Subset(dataset, list(range(min(15, len(dataset))))), batch_size=15)
    batch = _move_batch(next(iter(loader)), device)
    model.eval()
    timings: list[float] = []
    with torch.no_grad():
        for _ in range(10):
            _forward(model, batch)
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        for _ in range(100):
            started = time.perf_counter()
            _forward(model, batch)
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            timings.append((time.perf_counter() - started) * 1000)
    return {
        "device": str(device),
        "batch_rows": len(batch[-1]),
        "runs": len(timings),
        "p50_ms": float(np.percentile(timings, 50)),
        "p95_ms": float(np.percentile(timings, 95)),
    }


def train_pair_history_model(config: PairHistoryConfig) -> dict[str, Any]:
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (
        pair_manifest,
        pair_schema,
        frames,
        histories,
        baseline_metrics,
        baseline_test,
    ) = _load_phase10_inputs(config)
    counts = {split: class_counts(frame) for split, frame in frames.items()}
    if any(counts[split]["positives"] < 1 for split in HISTORY_SPLITS):
        raise ValueError("every public split needs positive pair examples")

    tabular_preprocessor = NeuralPairPreprocessor.fit(
        frames["train"],
        pair_schema["numeric_feature_columns"],
        pair_schema["categorical_feature_columns"],
    )
    tabular = {
        split: tabular_preprocessor.transform(frames[split]) for split in HISTORY_SPLITS
    }
    labels = {
        split: frames[split]["target"].astype(np.int8).to_numpy()
        for split in HISTORY_SPLITS
    }
    user_normalizer = SequenceNormalizer.fit(
        histories["train"]["user_sequences"],
        histories["train"]["user_masks"],
        USER_HISTORY_FEATURES,
    )
    pair_normalizer = SequenceNormalizer.fit(
        histories["train"]["pair_sequences"],
        histories["train"]["pair_masks"],
        PAIR_HISTORY_FEATURES,
    )
    for split in HISTORY_SPLITS:
        histories[split]["user_sequences"] = user_normalizer.transform(
            histories[split]["user_sequences"], histories[split]["user_masks"]
        )
        histories[split]["pair_sequences"] = pair_normalizer.transform(
            histories[split]["pair_sequences"], histories[split]["pair_masks"]
        )
    preprocessing = {
        "contract_version": 1,
        "fit_split": "train",
        "tabular": tabular_preprocessor.to_dict(),
        "user_history": user_normalizer.to_dict(),
        "pair_history": pair_normalizer.to_dict(),
    }
    _write_json(output_dir / "preprocessing.json", preprocessing)

    device = _resolve_device(config.device_name)
    max_history = int(histories["train"]["user_sequences"].shape[1])
    user_encoder, user_pretraining, user_pretrained_state = _pretrain_encoder(
        "user",
        histories["train"]["user_sequences"],
        histories["train"]["user_masks"],
        input_dim=len(USER_HISTORY_FEATURES),
        max_history=max_history,
        config=config,
        device=device,
        seed=config.random_seed,
    )
    pair_encoder, pair_pretraining, pair_pretrained_state = _pretrain_encoder(
        "pair",
        histories["train"]["pair_sequences"],
        histories["train"]["pair_masks"],
        input_dim=len(PAIR_HISTORY_FEATURES),
        max_history=max_history,
        config=config,
        device=device,
        seed=config.random_seed + 1,
    )
    pretraining_dir = output_dir / "pretraining"
    pretraining_dir.mkdir()
    torch.save(user_pretrained_state, pretraining_dir / "user_encoder.pt")
    torch.save(pair_pretrained_state, pretraining_dir / "pair_encoder.pt")
    _write_json(
        pretraining_dir / "metrics.json",
        {"user": user_pretraining, "pair": pair_pretraining},
    )

    datasets = {
        split: PairHistoryDataset(
            tabular[split][0], tabular[split][1], labels[split], histories[split]
        )
        for split in HISTORY_SPLITS
    }
    pin_memory = device.type == "cuda"
    loaders = {
        split: _loader(
            dataset,
            batch_size=config.batch_size,
            shuffle=split == "train",
            num_workers=config.num_workers,
            pin_memory=pin_memory,
            seed=config.random_seed + 2,
        )
        for split, dataset in datasets.items()
    }
    _seed_everything(config.random_seed + 2)
    model = PairHistoryRiskModel(
        len(tabular_preprocessor.numeric_columns),
        tabular_preprocessor.categorical_cardinalities,
        user_encoder,
        pair_encoder,
    ).to(device)
    optimizer = torch.optim.AdamW(
        model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay
    )
    loss_function = nn.BCEWithLogitsLoss(
        pos_weight=torch.tensor(config.positive_class_weight, device=device)
    )
    best_epoch = 0
    best_validation_pr = -math.inf
    best_validation_loss = math.inf
    best_state: dict[str, torch.Tensor] | None = None
    fine_tuning_history: list[dict[str, Any]] = []
    training_started = time.perf_counter()
    for epoch in range(1, config.epochs + 1):
        model.train()
        total, rows = 0.0, 0
        epoch_started = time.perf_counter()
        for raw_batch in loaders["train"]:
            batch = _move_batch(raw_batch, device)
            optimizer.zero_grad(set_to_none=True)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                logits = _forward(model, batch)
                loss = loss_function(logits, batch[-1])
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), 5.0)
            optimizer.step()
            total += float(loss.detach()) * len(logits)
            rows += len(logits)
        validation_loss, validation_raw = _predict(
            model, loaders["validation"], loss_function, device
        )
        validation_pr = float(
            average_precision_score(labels["validation"], validation_raw)
        )
        fine_tuning_history.append(
            {
                "epoch": epoch,
                "train_loss": total / rows,
                "validation_loss": validation_loss,
                "validation_pr_auc": validation_pr,
                "seconds": time.perf_counter() - epoch_started,
            }
        )
        print(
            f"[pair-history][fine-tune] epoch={epoch} train_loss={total / rows:.6f} "
            f"validation_loss={validation_loss:.6f} "
            f"validation_pr_auc={validation_pr:.6f}",
            flush=True,
        )
        improved = validation_pr > best_validation_pr + 1e-7 or (
            abs(validation_pr - best_validation_pr) <= 1e-7
            and validation_loss < best_validation_loss
        )
        if improved:
            best_epoch = epoch
            best_validation_pr = validation_pr
            best_validation_loss = validation_loss
            best_state = {
                key: value.detach().cpu().clone()
                for key, value in model.state_dict().items()
            }
        elif epoch - best_epoch >= config.patience:
            print(
                f"[pair-history][fine-tune] early_stop best_epoch={best_epoch}",
                flush=True,
            )
            break
    if best_state is None:
        raise RuntimeError("history fine-tuning produced no checkpoint")
    model.load_state_dict(best_state)
    raw_probabilities = {
        split: _predict(model, loaders[split], loss_function, device)[1]
        for split in ("validation", "test")
    }
    calibrator = PlattCalibrator.fit(labels["validation"], raw_probabilities["validation"])
    calibrated = {
        split: calibrator.predict(probabilities)
        for split, probabilities in raw_probabilities.items()
    }
    threshold = select_alert_budget_threshold(
        labels["validation"], calibrated["validation"], config.max_alert_rate
    )
    reports = {
        split: binary_classification_report(
            labels[split],
            calibrated[split],
            threshold=threshold,
            max_alert_rate=config.max_alert_rate,
            hand_count=counts[split]["hands"],
        )
        for split in ("validation", "test")
    }
    bootstrap = paired_hand_bootstrap_pr_auc(
        frames["test"],
        calibrated["test"],
        baseline_test,
        samples=config.bootstrap_samples,
        seed=config.random_seed + 2,
    )
    baseline_report = baseline_metrics["reports"]["catboost"]["test"]
    gate = challenger_gate(
        reports["test"],
        baseline_report,
        bootstrap,
        minimum_relative_pr_gain=config.minimum_relative_pr_gain,
        max_alert_rate=config.max_alert_rate,
    )
    latency = _latency(model, datasets["validation"], device)
    model_dir = output_dir / MODEL_NAME
    model_dir.mkdir()
    torch.save(
        {
            "model_name": MODEL_NAME,
            "state_dict": best_state,
            "numeric_dim": len(tabular_preprocessor.numeric_columns),
            "categorical_cardinalities": tabular_preprocessor.categorical_cardinalities,
            "user_input_dim": len(USER_HISTORY_FEATURES),
            "pair_input_dim": len(PAIR_HISTORY_FEATURES),
            "max_history": max_history,
            "encoder_width": config.encoder_width,
            "encoder_heads": config.encoder_heads,
            "encoder_layers": config.encoder_layers,
            "feature_definition_version": pair_manifest["feature_definition_version"],
        },
        model_dir / "model.pt",
    )
    model_metrics = {
        "model_name": MODEL_NAME,
        "best_epoch": best_epoch,
        "epochs_ran": len(fine_tuning_history),
        "best_validation_pr_auc": best_validation_pr,
        "best_validation_loss": best_validation_loss,
        "fine_tuning_seconds": time.perf_counter() - training_started,
        "parameter_count": sum(parameter.numel() for parameter in model.parameters()),
        "pretraining": {"user": user_pretraining, "pair": pair_pretraining},
        "calibration": calibrator.to_dict(),
        "threshold": threshold,
        "reports": reports,
        "latency": latency,
        "paired_bootstrap": bootstrap,
        "quality_gate": gate,
        "history": fine_tuning_history,
    }
    _write_json(model_dir / "metrics.json", model_metrics)
    predictions = []
    for split in ("validation", "test"):
        predictions.append(
            pd.DataFrame(
                {
                    "model_name": MODEL_NAME,
                    "split": split,
                    "event_id": frames[split]["event_id"].astype(str),
                    "hand_id": frames[split]["hand_id"].astype(str),
                    "pair_key": frames[split]["pair_key"].astype(str),
                    "target": labels[split],
                    "raw_probability": raw_probabilities[split],
                    "calibrated_probability": calibrated[split],
                    "alert": calibrated[split] >= threshold,
                }
            )
        )
    pd.concat(predictions, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )
    run_id = f"pair_history_{uuid.uuid4().hex[:12]}"
    summary = {
        "run_id": run_id,
        "phase": 10,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "dataset_id": pair_manifest["dataset_id"],
        "pair_dataset_manifest_sha256": sha256_file(
            config.pair_dataset_dir.resolve() / "manifest.json"
        ),
        "history_dataset_manifest_sha256": sha256_file(
            config.history_dataset_dir.resolve() / "manifest.json"
        ),
        "feature_definition_version": pair_manifest["feature_definition_version"],
        "benchmark": "cold_start",
        "challenge_artifacts_read": False,
        "challenge_labels_used": False,
        "pretraining_labels_used": False,
        "device": str(device),
        "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
        "counts": counts,
        "training_config": {
            **asdict(config),
            "history_dataset_dir": str(config.history_dataset_dir),
            "pair_dataset_dir": str(config.pair_dataset_dir),
            "baseline_dir": str(config.baseline_dir),
            "output_dir": str(config.output_dir),
        },
        "catboost_baseline": {
            "run_id": baseline_metrics["run_id"],
            "test": baseline_report,
        },
        "model": model_metrics,
    }
    _write_json(output_dir / "summary.json", summary)
    artifacts = {
        str(path.relative_to(output_dir)): sha256_file(path)
        for path in sorted(output_dir.rglob("*"))
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    _write_json(
        output_dir / "artifact_manifest.json",
        {"run_id": run_id, "artifacts": artifacts},
    )
    print(
        f"[pair-history] test_pr_auc={reports['test']['pr_auc']:.6f} "
        f"test_f1={reports['test']['f1']:.6f} "
        f"recall_at_budget={reports['test']['recall_at_alert_budget']:.6f} "
        f"promotion_candidate={gate['promotion_candidate']}",
        flush=True,
    )
    return summary
