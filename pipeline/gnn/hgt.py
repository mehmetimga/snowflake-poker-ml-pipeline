"""A simplified single-relation HGT-style attention block.

The original project ships a full Heterogeneous Graph Transformer with multiple
node/edge types. For the demo we keep one node type (Player) and use a single
attention layer over edges weighted by their PAIR_STATS attributes.
"""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GATv2Conv


class SimpleHGT(nn.Module):
    def __init__(self, in_dim: int, hidden: int = 32, n_heads: int = 2, edge_dim: int = 5) -> None:
        super().__init__()
        self.gat1 = GATv2Conv(in_dim, hidden, heads=n_heads, edge_dim=edge_dim, add_self_loops=True, concat=False)
        self.gat2 = GATv2Conv(hidden, hidden, heads=n_heads, edge_dim=edge_dim, add_self_loops=True, concat=False)
        self.classifier = nn.Linear(hidden, 1)

    def embed(self, x: torch.Tensor, edge_index: torch.Tensor, edge_attr: torch.Tensor) -> torch.Tensor:
        h = F.elu(self.gat1(x, edge_index, edge_attr))
        h = F.elu(self.gat2(h, edge_index, edge_attr))
        return h

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor, edge_attr: torch.Tensor) -> torch.Tensor:
        return self.classifier(self.embed(x, edge_index, edge_attr)).squeeze(-1)
