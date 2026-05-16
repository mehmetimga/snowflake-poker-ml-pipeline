from __future__ import annotations

import torch
import torch.nn as nn


class WideAndDeep(nn.Module):
    """Stacks upstream model outputs into a final risk probability.

    Wide branch: linear over [xgb, catboost, lightgbm, vgae_anomaly, qdrant_min_distance, rule_score]
    Deep branch: 2-layer MLP over [lstm_embedding (64d), transformer_embedding (32d)]
    """

    def __init__(self, wide_dim: int = 6, deep_dim: int = 96, hidden: int = 64) -> None:
        super().__init__()
        self.wide = nn.Linear(wide_dim, 1)
        self.deep = nn.Sequential(
            nn.Linear(deep_dim, hidden),
            nn.ReLU(),
            nn.Dropout(0.1),
            nn.Linear(hidden, hidden // 2),
            nn.ReLU(),
            nn.Linear(hidden // 2, 1),
        )

    def forward(self, wide: torch.Tensor, deep: torch.Tensor) -> torch.Tensor:
        return (self.wide(wide) + self.deep(deep)).squeeze(-1)
