from __future__ import annotations

import torch
import torch.nn as nn


class LSTMEncoder(nn.Module):
    """Bidirectional LSTM over per-hand action sequences with a final classifier head.

    Exposes `embed(x)` for use as a feature extractor by the meta-learner.
    """

    def __init__(self, input_dim: int = 4, hidden_dim: int = 32, num_layers: int = 2, dropout: float = 0.2) -> None:
        super().__init__()
        self.lstm = nn.LSTM(
            input_size=input_dim,
            hidden_size=hidden_dim,
            num_layers=num_layers,
            batch_first=True,
            bidirectional=True,
            dropout=dropout if num_layers > 1 else 0.0,
        )
        self.embed_dim = hidden_dim * 2
        self.classifier = nn.Sequential(
            nn.Linear(self.embed_dim, 32),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(32, 1),
        )

    def embed(self, x: torch.Tensor) -> torch.Tensor:
        out, _ = self.lstm(x)
        return out.mean(dim=1)  # mean-pool over time

    def forward(self, x: torch.Tensor) -> torch.Tensor:
        return self.classifier(self.embed(x)).squeeze(-1)
