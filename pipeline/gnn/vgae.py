"""Variational Graph AutoEncoder for anomaly detection on the player graph."""

from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F
from torch_geometric.nn import GCNConv


class _Encoder(nn.Module):
    def __init__(self, in_dim: int, hidden: int = 32, latent: int = 16) -> None:
        super().__init__()
        self.shared = GCNConv(in_dim, hidden)
        self.mu = GCNConv(hidden, latent)
        self.logstd = GCNConv(hidden, latent)

    def forward(self, x: torch.Tensor, edge_index: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        h = F.relu(self.shared(x, edge_index))
        return self.mu(h, edge_index), self.logstd(h, edge_index)


class VGAE(nn.Module):
    def __init__(self, in_dim: int, hidden: int = 32, latent: int = 16) -> None:
        super().__init__()
        self.encoder = _Encoder(in_dim, hidden, latent)

    def reparam(self, mu: torch.Tensor, logstd: torch.Tensor) -> torch.Tensor:
        if self.training:
            return mu + torch.randn_like(mu) * logstd.exp()
        return mu

    def encode(self, x: torch.Tensor, edge_index: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        mu, logstd = self.encoder(x, edge_index)
        z = self.reparam(mu, logstd)
        return z, mu, logstd

    @staticmethod
    def decode(z: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        return (z[edge_index[0]] * z[edge_index[1]]).sum(dim=-1)

    def recon_loss(self, z: torch.Tensor, pos_edge_index: torch.Tensor) -> torch.Tensor:
        pos = torch.sigmoid(self.decode(z, pos_edge_index))
        # negative sampling: uniform random pairs
        num_nodes = z.size(0)
        neg_src = torch.randint(0, num_nodes, (pos_edge_index.size(1),), device=z.device)
        neg_dst = torch.randint(0, num_nodes, (pos_edge_index.size(1),), device=z.device)
        neg_edge = torch.stack([neg_src, neg_dst])
        neg = torch.sigmoid(self.decode(z, neg_edge))
        eps = 1e-8
        return -(torch.log(pos + eps).mean() + torch.log(1 - neg + eps).mean())

    def kl_loss(self, mu: torch.Tensor, logstd: torch.Tensor) -> torch.Tensor:
        return -0.5 * torch.mean(torch.sum(1 + 2 * logstd - mu ** 2 - logstd.exp() ** 2, dim=1))

    def anomaly_scores(self, x: torch.Tensor, edge_index: torch.Tensor) -> torch.Tensor:
        """Per-node anomaly score: 1 - mean reconstruction prob over incident edges."""
        self.eval()
        with torch.no_grad():
            z, _, _ = self.encode(x, edge_index)
            recon = torch.sigmoid(self.decode(z, edge_index))
            num_nodes = x.size(0)
            scores = torch.zeros(num_nodes, device=z.device)
            counts = torch.zeros(num_nodes, device=z.device)
            for i in range(edge_index.size(1)):
                u = int(edge_index[0, i])
                scores[u] += float(recon[i])
                counts[u] += 1
            mean_recon = scores / counts.clamp(min=1)
            return 1.0 - mean_recon
