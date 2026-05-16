"""Train VGAE (unsupervised) and SimpleHGT (supervised) on the player graph."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import torch
import torch.nn.functional as F

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse

from .graph_builder import build_player_graph
from .hgt import SimpleHGT
from .vgae import VGAE


def _node_labels(warehouse: Warehouse, node_ids: list[str]) -> np.ndarray:
    rows = warehouse.fetch_df("SELECT player_id, MAX(CAST(is_suspicious AS INT)) AS y FROM RAW_PLAYERS GROUP BY player_id")
    label_map = dict(zip(rows["player_id"], rows["y"].astype(int))) if not rows.empty else {}
    return np.array([label_map.get(pid, 0) for pid in node_ids], dtype=np.int64)


def train_gnn(
    warehouse: Warehouse | None = None,
    output_dir: Path | None = None,
    epochs: int = 80,
) -> dict:
    settings = get_settings()
    wh = warehouse or get_warehouse()
    out_dir = Path(output_dir or settings.models_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    graph = build_player_graph(wh)
    if graph is None:
        print("[gnn] PAIR_STATS empty — skipping GNN training.")
        return {}

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    data = graph.data.to(device)
    in_dim = data.x.size(1)

    # VGAE (unsupervised, reconstruction-based anomaly scores)
    vgae = VGAE(in_dim=in_dim).to(device)
    opt_v = torch.optim.AdamW(vgae.parameters(), lr=1e-2, weight_decay=1e-4)
    for epoch in range(epochs):
        vgae.train()
        opt_v.zero_grad()
        z, mu, logstd = vgae.encode(data.x, data.edge_index)
        loss = vgae.recon_loss(z, data.edge_index) + (1.0 / data.x.size(0)) * vgae.kl_loss(mu, logstd)
        loss.backward()
        opt_v.step()
        if epoch % 20 == 0:
            print(f"[gnn][vgae] epoch={epoch} loss={float(loss):.4f}")
    torch.save(vgae.state_dict(), out_dir / "vgae.pt")

    # Cache per-node anomaly scores for the meta-learner + Streamlit graph explorer
    anomaly = vgae.anomaly_scores(data.x, data.edge_index).cpu().numpy()
    score_map = {pid: float(a) for pid, a in zip(graph.node_to_id, anomaly)}
    (out_dir / "vgae_scores.json").write_text(json.dumps(score_map))

    # Simple HGT (supervised on pair_score >= median as a coarse pseudo-label)
    y = _node_labels(wh, graph.node_to_id)
    if len(np.unique(y)) > 1:
        y_t = torch.tensor(y, dtype=torch.float32, device=device)
        hgt = SimpleHGT(in_dim=in_dim, edge_dim=data.edge_attr.size(1)).to(device)
        opt_h = torch.optim.AdamW(hgt.parameters(), lr=1e-2, weight_decay=1e-4)
        for epoch in range(epochs):
            hgt.train()
            opt_h.zero_grad()
            logits = hgt(data.x, data.edge_index, data.edge_attr)
            loss = F.binary_cross_entropy_with_logits(logits, y_t)
            loss.backward()
            opt_h.step()
            if epoch % 20 == 0:
                print(f"[gnn][hgt] epoch={epoch} loss={float(loss):.4f}")
        torch.save(hgt.state_dict(), out_dir / "hgt.pt")
        with torch.no_grad():
            hgt_scores = torch.sigmoid(hgt(data.x, data.edge_index, data.edge_attr)).cpu().numpy()
        (out_dir / "hgt_scores.json").write_text(
            json.dumps({pid: float(s) for pid, s in zip(graph.node_to_id, hgt_scores)})
        )

    return {"vgae": str(out_dir / "vgae.pt"), "hgt": str(out_dir / "hgt.pt")}
