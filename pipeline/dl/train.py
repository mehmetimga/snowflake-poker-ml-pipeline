"""Train leakage-safe LSTM and Transformer sequence baselines."""

from __future__ import annotations

import copy
import json
import random
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import numpy as np
import torch
from torch.utils.data import DataLoader

from pipeline.ml.evaluation import evaluate_model, select_optimal_threshold

from .dataset import (
    FEATURE_DIM,
    HandSequenceDataset,
    SequencePartitions,
    build_frozen_sequence_partitions,
)
from .focal_loss import FocalLoss
from .lstm_encoder import LSTMEncoder
from .transformer import TransformerEncoder

if TYPE_CHECKING:
    from pipeline.warehouse.factory import Warehouse


DeviceName = Literal["auto", "cpu", "cuda"]


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


def _resolve_device(requested: DeviceName) -> torch.device:
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but torch.cuda.is_available() is false")
    if requested == "auto":
        requested = "cuda" if torch.cuda.is_available() else "cpu"
    return torch.device(requested)


def _epoch(model, loader, opt, loss_fn, train: bool, device: torch.device) -> float:
    model.train() if train else model.eval()
    total, n = 0.0, 0
    for X, y in loader:
        X = X.to(device, non_blocking=True)
        y = y.to(device, non_blocking=True).float()
        if train:
            opt.zero_grad()
            logits = model(X)
            loss = loss_fn(logits, y)
            loss.backward()
            opt.step()
        else:
            with torch.no_grad():
                logits = model(X)
                loss = loss_fn(logits, y)
        total += float(loss.item()) * len(y)
        n += len(y)
    return total / max(n, 1)


def _predict(model, loader, device: torch.device) -> np.ndarray:
    model.eval()
    probabilities: list[np.ndarray] = []
    with torch.no_grad():
        for X, _ in loader:
            logits = model(X.to(device, non_blocking=True))
            probabilities.append(torch.sigmoid(logits).cpu().numpy())
    return np.concatenate(probabilities).astype(np.float32) if probabilities else np.zeros(0, dtype=np.float32)


def _validate_partitions(partitions: SequencePartitions) -> None:
    if partitions.strategy != "frozen_disjoint_players":
        raise RuntimeError(f"DL training requires frozen splits, found {partitions.strategy!r}")
    for name in ("train", "validation", "test"):
        split = getattr(partitions, name)
        if len(split.X) != len(split.y) or len(split.y) != len(split.ids):
            raise RuntimeError(f"The {name} DL split has inconsistent row counts")
        if len(split.y) == 0 or len(np.unique(split.y)) < 2:
            raise RuntimeError(f"The {name} DL split needs suspicious and normal labels")
        if split.X.ndim != 3 or split.X.shape[2] != FEATURE_DIM:
            raise RuntimeError(
                f"The {name} DL split must have shape (rows, sequence, {FEATURE_DIM})"
            )


def train_sequence_models_from_partitions(
    partitions: SequencePartitions,
    output_dir: Path,
    epochs: int = 20,
    batch_size: int = 512,
    patience: int = 4,
    random_seed: int = 42,
    device_name: DeviceName = "auto",
) -> dict:
    """Train both sequence models from a portable, frozen dataset bundle."""
    _validate_partitions(partitions)
    if epochs < 1 or batch_size < 1 or patience < 1:
        raise ValueError("epochs, batch_size, and patience must all be positive")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    device = _resolve_device(device_name)
    pin_memory = device.type == "cuda"
    positive_alpha = float(
        np.clip(1.0 - partitions.train.y.astype(np.float64).mean(), 0.5, 0.99)
    )
    print(
        f"[dl] device={device} split={partitions.strategy} "
        f"n_train={len(partitions.train.y)} n_val={len(partitions.validation.y)} "
        f"n_test={len(partitions.test.y)} positive_alpha={positive_alpha:.4f}"
    )

    validation_loader = DataLoader(
        HandSequenceDataset(partitions.validation.X, partitions.validation.y),
        batch_size=batch_size,
        pin_memory=pin_memory,
    )
    test_loader = DataLoader(
        HandSequenceDataset(partitions.test.X, partitions.test.y),
        batch_size=batch_size,
        pin_memory=pin_memory,
    )

    results: dict[str, dict] = {}
    model_types = (("lstm", LSTMEncoder), ("transformer", TransformerEncoder))
    for model_index, (name, model_cls) in enumerate(model_types):
        model_seed = random_seed + model_index
        _seed_everything(model_seed)
        train_loader = DataLoader(
            HandSequenceDataset(partitions.train.X, partitions.train.y),
            batch_size=batch_size,
            shuffle=True,
            pin_memory=pin_memory,
            generator=torch.Generator().manual_seed(model_seed),
        )
        model = model_cls(input_dim=FEATURE_DIM).to(device)
        opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
        loss_fn = FocalLoss(alpha=positive_alpha)

        best_epoch = 0
        best_validation_loss = float("inf")
        best_state: dict[str, torch.Tensor] | None = None
        history: list[dict[str, float | int]] = []
        for epoch in range(1, epochs + 1):
            train_loss = _epoch(model, train_loader, opt, loss_fn, train=True, device=device)
            validation_loss = _epoch(
                model,
                validation_loader,
                opt,
                loss_fn,
                train=False,
                device=device,
            )
            history.append(
                {
                    "epoch": epoch,
                    "train_loss": train_loss,
                    "validation_loss": validation_loss,
                }
            )
            print(
                f"[dl][{name}] epoch={epoch} train_loss={train_loss:.5f} "
                f"validation_loss={validation_loss:.5f}"
            )
            if validation_loss < best_validation_loss - 1e-7:
                best_epoch = epoch
                best_validation_loss = validation_loss
                best_state = {
                    key: value.detach().cpu().clone()
                    for key, value in model.state_dict().items()
                }
            elif epoch - best_epoch >= patience:
                print(f"[dl][{name}] early_stop best_epoch={best_epoch}")
                break

        if best_state is None:
            raise RuntimeError(f"{name} training did not produce a checkpoint")
        model.load_state_dict(best_state)
        model_path = out_dir / f"{name}.pt"
        torch.save(best_state, model_path)

        validation_proba = _predict(model, validation_loader, device)
        test_proba = _predict(model, test_loader, device)
        threshold = select_optimal_threshold(partitions.validation.y, validation_proba)
        validation_metrics = evaluate_model(
            f"{name}_validation",
            partitions.validation.y,
            validation_proba,
            threshold=threshold,
        )
        test_metrics = evaluate_model(
            name,
            partitions.test.y,
            test_proba,
            threshold=threshold,
        )
        print(
            f"[dl][{name}] test roc={test_metrics.roc_auc:.4f} "
            f"pr={test_metrics.pr_auc:.4f} f1={test_metrics.f1:.4f} "
            f"threshold={threshold:.4f}"
        )
        results[name] = {
            "path": str(model_path),
            "best_epoch": best_epoch,
            "epochs_ran": len(history),
            "best_validation_loss": best_validation_loss,
            "validation_metrics": validation_metrics.to_dict(),
            "test_metrics": test_metrics.to_dict(),
            "history": history,
        }

    info = {
        "input_dim": FEATURE_DIM,
        "max_len": int(partitions.train.X.shape[1]),
        "models": list(results),
        "device": str(device),
        "random_seed": random_seed,
        "split_strategy": partitions.strategy,
        "amount_scale": partitions.amount_scale,
        "positive_alpha": positive_alpha,
        "splits": {
            name: {
                "rows": int(len(getattr(partitions, name).y)),
                "positive_rows": int(getattr(partitions, name).y.sum()),
            }
            for name in ("train", "validation", "test")
        },
    }
    (out_dir / "dl_info.json").write_text(json.dumps(info, indent=2) + "\n")
    (out_dir / "dl_metrics.json").write_text(json.dumps(results, indent=2) + "\n")
    return results


def train_sequence_models(
    warehouse: Warehouse | None = None,
    output_dir: Path | None = None,
    epochs: int = 20,
    batch_size: int = 512,
    patience: int = 4,
    device_name: DeviceName = "auto",
) -> dict:
    """Build frozen partitions from a warehouse and train sequence models."""
    from pipeline.config import get_settings
    from pipeline.warehouse import get_warehouse

    settings = get_settings()
    wh = warehouse or get_warehouse()
    partitions = build_frozen_sequence_partitions(wh)
    return train_sequence_models_from_partitions(
        partitions,
        output_dir=Path(output_dir or settings.models_dir),
        epochs=epochs,
        batch_size=batch_size,
        patience=patience,
        random_seed=settings.random_seed,
        device_name=device_name,
    )
