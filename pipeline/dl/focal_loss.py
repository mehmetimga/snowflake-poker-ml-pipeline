from __future__ import annotations

import torch
import torch.nn as nn
import torch.nn.functional as F


class FocalLoss(nn.Module):
    def __init__(self, alpha: float = 0.75, gamma: float = 2.0) -> None:
        super().__init__()
        self.alpha = alpha
        self.gamma = gamma

    def forward(self, logits: torch.Tensor, targets: torch.Tensor) -> torch.Tensor:
        probs = torch.sigmoid(logits)
        targets = targets.float()
        pt = torch.where(targets == 1, probs, 1 - probs)
        alpha_t = torch.where(targets == 1, torch.tensor(self.alpha, device=logits.device), torch.tensor(1 - self.alpha, device=logits.device))
        loss = -alpha_t * (1 - pt) ** self.gamma * torch.log(pt.clamp(min=1e-8))
        return loss.mean()
