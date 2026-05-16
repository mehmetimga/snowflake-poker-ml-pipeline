"""Train LSTM + Transformer sequence models and persist their state_dicts."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import torch
from sklearn.model_selection import train_test_split
from torch.utils.data import DataLoader

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse

from .dataset import FEATURE_DIM, HandSequenceDataset, build_sequences
from .focal_loss import FocalLoss
from .lstm_encoder import LSTMEncoder
from .transformer import TransformerEncoder


def _epoch(model, loader, opt, loss_fn, train: bool, device) -> float:
    model.train() if train else model.eval()
    total, n = 0.0, 0
    for X, y in loader:
        X = X.to(device)
        y = y.to(device).float()
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


def train_sequence_models(
    warehouse: Warehouse | None = None,
    output_dir: Path | None = None,
    epochs: int = 6,
    batch_size: int = 64,
) -> dict:
    settings = get_settings()
    wh = warehouse or get_warehouse()
    out_dir = Path(output_dir or settings.models_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    X, y, _ids = build_sequences(wh)
    if len(X) == 0 or len(np.unique(y)) < 2:
        print("[dl] insufficient sequence data — skipping DL training.")
        return {}

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.20, stratify=y, random_state=settings.random_seed
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    train_loader = DataLoader(HandSequenceDataset(X_train, y_train), batch_size=batch_size, shuffle=True)
    test_loader = DataLoader(HandSequenceDataset(X_test, y_test), batch_size=batch_size)

    results: dict[str, dict] = {}
    for name, model_cls in (("lstm", LSTMEncoder), ("transformer", TransformerEncoder)):
        model = model_cls(input_dim=FEATURE_DIM).to(device)
        opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
        loss_fn = FocalLoss()
        for epoch in range(epochs):
            tr = _epoch(model, train_loader, opt, loss_fn, train=True, device=device)
            te = _epoch(model, test_loader, opt, loss_fn, train=False, device=device)
            print(f"[dl][{name}] epoch={epoch} train_loss={tr:.4f} test_loss={te:.4f}")
        torch.save(model.state_dict(), out_dir / f"{name}.pt")
        results[name] = {"path": str(out_dir / f"{name}.pt")}

    (out_dir / "dl_info.json").write_text(json.dumps({"input_dim": FEATURE_DIM, "models": list(results)}, indent=2))
    return results
