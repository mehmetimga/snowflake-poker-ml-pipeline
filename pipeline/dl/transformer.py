from __future__ import annotations

import math

import torch
import torch.nn as nn


class _PositionalEncoding(nn.Module):
    def __init__(self, d_model: int, max_len: int = 1024) -> None:
        super().__init__()
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len).unsqueeze(1).float()
        div = torch.exp(torch.arange(0, d_model, 2).float() * (-math.log(10000.0) / d_model))
        pe[:, 0::2] = torch.sin(position * div)
        pe[:, 1::2] = torch.cos(position * div)
        self.register_buffer("pe", pe.unsqueeze(0))

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return x + self.pe[:, : x.size(1), :]


class TransformerEncoder(nn.Module):
    """Small Transformer encoder over per-hand action sequences."""

    def __init__(self, input_dim: int = 4, d_model: int = 32, n_heads: int = 4, n_layers: int = 2, dropout: float = 0.2) -> None:
        super().__init__()
        self.input_proj = nn.Linear(input_dim, d_model)
        self.pos = _PositionalEncoding(d_model)
        layer = nn.TransformerEncoderLayer(
            d_model=d_model, nhead=n_heads, dim_feedforward=d_model * 4, dropout=dropout, batch_first=True
        )
        self.encoder = nn.TransformerEncoder(layer, num_layers=n_layers)
        self.embed_dim = d_model
        self.classifier = nn.Sequential(
            nn.Linear(d_model, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1),
        )

    def embed(self, x: torch.Tensor) -> torch.Tensor:
        h = self.pos(self.input_proj(x))
        h = self.encoder(h)
        return h.mean(dim=1)

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.classifier(self.embed(x)).squeeze(-1)
