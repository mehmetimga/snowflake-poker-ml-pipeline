"""Train the Wide-and-Deep meta-learner."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F
from sklearn.model_selection import train_test_split

from pipeline.config import get_settings
from pipeline.dl.focal_loss import FocalLoss
from pipeline.ml.evaluation import evaluate_model
from pipeline.warehouse import Warehouse, get_warehouse

from .dataset import assemble_dataset
from .wide_and_deep import WideAndDeep


def train_meta_learner(
    warehouse: Warehouse | None = None,
    output_dir: Path | None = None,
    epochs: int = 30,
    batch_size: int = 64,
) -> dict:
    settings = get_settings()
    wh = warehouse or get_warehouse()
    out_dir = Path(output_dir or settings.models_dir)

    wide, deep, y, _ids = assemble_dataset(wh, out_dir)
    if len(y) == 0 or len(np.unique(y)) < 2:
        print("[meta] insufficient data — skipping meta-learner training.")
        return {}

    Xw_train, Xw_test, Xd_train, Xd_test, y_train, y_test = train_test_split(
        wide, deep, y, test_size=0.20, stratify=y, random_state=settings.random_seed
    )

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = WideAndDeep(wide_dim=wide.shape[1], deep_dim=deep.shape[1]).to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-4)
    loss_fn = FocalLoss()

    Xw_train_t = torch.from_numpy(Xw_train).to(device)
    Xd_train_t = torch.from_numpy(Xd_train).to(device)
    y_train_t = torch.from_numpy(y_train).float().to(device)

    for epoch in range(epochs):
        perm = torch.randperm(len(y_train))
        epoch_loss = 0.0
        for i in range(0, len(perm), batch_size):
            idx = perm[i : i + batch_size]
            logits = model(Xw_train_t[idx], Xd_train_t[idx])
            loss = loss_fn(logits, y_train_t[idx])
            opt.zero_grad()
            loss.backward()
            opt.step()
            epoch_loss += float(loss.item()) * len(idx)
        if epoch % 5 == 0:
            print(f"[meta] epoch={epoch} loss={epoch_loss / len(y_train):.4f}")

    # Evaluate
    model.eval()
    with torch.no_grad():
        proba = torch.sigmoid(model(torch.from_numpy(Xw_test).to(device), torch.from_numpy(Xd_test).to(device))).cpu().numpy()
    metrics = evaluate_model("meta_wide_and_deep", y_test, proba)
    print(f"[meta] roc={metrics.roc_auc:.3f} pr={metrics.pr_auc:.3f} f1={metrics.f1:.3f} thr={metrics.optimal_threshold:.3f}")

    torch.save(model.state_dict(), out_dir / "meta_wide_and_deep.pt")
    (out_dir / "meta_metrics.json").write_text(json.dumps(metrics.to_dict(), indent=2))
    (out_dir / "meta_shapes.json").write_text(
        json.dumps({"wide_dim": int(wide.shape[1]), "deep_dim": int(deep.shape[1])}, indent=2)
    )
    return {"path": str(out_dir / "meta_wide_and_deep.pt"), **metrics.to_dict()}
