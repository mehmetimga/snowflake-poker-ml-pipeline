"""Self-supervised history encoders and the Phase 10 pair-risk model."""

from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn
from torch.nn import functional as F

from .tabular_models import CategoricalInput, ResidualBlock


class HistoryEncoder(nn.Module):
    def __init__(
        self,
        input_dim: int,
        max_history: int,
        *,
        width: int = 32,
        heads: int = 4,
        layers: int = 2,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        if input_dim < 1 or max_history < 2 or width < 1 or layers < 1:
            raise ValueError("history dimensions and layers must be positive")
        if width % heads:
            raise ValueError("history width must be divisible by attention heads")
        self.input_dim = input_dim
        self.max_history = max_history
        self.width = width
        self.input_projection = nn.Linear(input_dim, width)
        self.position = nn.Parameter(torch.empty(1, max_history + 1, width))
        self.cls = nn.Parameter(torch.empty(1, 1, width))
        layer = nn.TransformerEncoderLayer(
            d_model=width,
            nhead=heads,
            dim_feedforward=width * 3,
            dropout=dropout,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.transformer = nn.TransformerEncoder(
            layer, num_layers=layers, enable_nested_tensor=False
        )
        self.output_norm = nn.LayerNorm(width)
        nn.init.normal_(self.position, std=0.02)
        nn.init.normal_(self.cls, std=0.02)

    def forward(
        self,
        sequence: torch.Tensor,
        mask: torch.Tensor,
        *,
        return_tokens: bool = False,
    ) -> torch.Tensor | tuple[torch.Tensor, torch.Tensor]:
        if sequence.ndim != 3 or sequence.shape[1:] != (
            self.max_history,
            self.input_dim,
        ):
            raise ValueError("history sequence shape disagrees with the encoder")
        if mask.shape != sequence.shape[:2]:
            raise ValueError("history mask shape disagrees with the sequence")
        valid = mask.bool()
        projected = self.input_projection(sequence.float())
        cls = self.cls.expand(len(sequence), -1, -1)
        tokens = torch.cat([cls, projected], dim=1) + self.position
        padding = torch.cat(
            [torch.zeros((len(sequence), 1), dtype=torch.bool, device=mask.device), ~valid],
            dim=1,
        )
        encoded = self.transformer(tokens, src_key_padding_mask=padding)
        representation = self.output_norm(encoded[:, 0])
        if return_tokens:
            return representation, self.output_norm(encoded[:, 1:])
        return representation


class HistoryPretrainer(nn.Module):
    def __init__(self, encoder: HistoryEncoder) -> None:
        super().__init__()
        self.encoder = encoder
        self.reconstruction_head = nn.Linear(encoder.width, encoder.input_dim)
        self.next_step_head = nn.Sequential(
            nn.LayerNorm(encoder.width),
            nn.Linear(encoder.width, encoder.input_dim),
        )


def _last_valid_indices(mask: torch.Tensor) -> torch.Tensor:
    positions = torch.arange(mask.shape[1], device=mask.device).unsqueeze(0)
    return (positions * mask.long()).max(dim=1).values


def self_supervised_history_loss(
    model: HistoryPretrainer,
    sequence: torch.Tensor,
    mask: torch.Tensor,
    *,
    mask_probability: float = 0.15,
    contrastive_temperature: float = 0.2,
) -> tuple[torch.Tensor, dict[str, torch.Tensor]]:
    """Masked-step, next-step, and contrastive train-only objectives."""
    if not 0 < mask_probability < 1 or contrastive_temperature <= 0:
        raise ValueError("invalid self-supervised objective parameters")
    valid = mask.bool()
    if not bool(valid.any()):
        raise ValueError("self-supervised batches need at least one valid history step")

    selected = (torch.rand(valid.shape, device=valid.device) < mask_probability) & valid
    empty_rows = valid.any(dim=1) & ~selected.any(dim=1)
    last_indices = _last_valid_indices(valid)
    selected[empty_rows, last_indices[empty_rows]] = True
    masked_input = sequence.clone()
    masked_input[selected] = 0
    _, encoded_tokens = model.encoder(masked_input, valid, return_tokens=True)
    reconstruction = model.reconstruction_head(encoded_tokens)
    masked_loss = F.mse_loss(reconstruction[selected], sequence.float()[selected])

    next_eligible = valid.sum(dim=1) >= 2
    prefix = sequence.clone()
    prefix_mask = valid.clone()
    next_targets = sequence[
        torch.arange(len(sequence), device=sequence.device), last_indices
    ].float()
    prefix[
        torch.arange(len(sequence), device=sequence.device), last_indices
    ] = 0
    prefix_mask[
        torch.arange(len(sequence), device=sequence.device), last_indices
    ] = False
    prefix_representation = model.encoder(prefix, prefix_mask)
    next_prediction = model.next_step_head(prefix_representation)
    next_loss = F.mse_loss(
        next_prediction[next_eligible], next_targets[next_eligible]
    )

    def augmented_view() -> tuple[torch.Tensor, torch.Tensor]:
        keep = ~((torch.rand(valid.shape, device=valid.device) < 0.10) & valid)
        view_mask = valid & keep
        view = sequence.clone()
        view[~view_mask] = 0
        return view, view_mask

    view_a, mask_a = augmented_view()
    view_b, mask_b = augmented_view()
    representation_a = F.normalize(model.encoder(view_a, mask_a), dim=1)
    representation_b = F.normalize(model.encoder(view_b, mask_b), dim=1)
    logits = representation_a @ representation_b.transpose(0, 1)
    logits = logits / contrastive_temperature
    targets = torch.arange(len(sequence), device=sequence.device)
    contrastive_loss = 0.5 * (
        F.cross_entropy(logits, targets) + F.cross_entropy(logits.transpose(0, 1), targets)
    )
    total = masked_loss + next_loss + 0.1 * contrastive_loss
    return total, {
        "masked_reconstruction_loss": masked_loss,
        "next_step_loss": next_loss,
        "contrastive_loss": contrastive_loss,
    }


class PairHistoryRiskModel(nn.Module):
    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        user_encoder: HistoryEncoder,
        pair_encoder: HistoryEncoder,
        *,
        tabular_width: int = 128,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        if user_encoder.width != pair_encoder.width:
            raise ValueError("user and pair history widths must match")
        self.user_encoder = user_encoder
        self.pair_encoder = pair_encoder
        self.categories = CategoricalInput(categorical_cardinalities, embedding_dim=8)
        tabular_input = numeric_dim + self.categories.output_dim
        self.tabular = nn.Sequential(
            nn.Linear(tabular_input, tabular_width),
            ResidualBlock(tabular_width, dropout),
            nn.LayerNorm(tabular_width),
        )
        history_width = user_encoder.width * 4
        combined_width = tabular_width + history_width
        self.head = nn.Sequential(
            nn.Linear(combined_width, 256),
            nn.LayerNorm(256),
            nn.GELU(),
            nn.Dropout(dropout),
            ResidualBlock(256, dropout),
            nn.LayerNorm(256),
            nn.Linear(256, 1),
        )

    def forward(
        self,
        numeric: torch.Tensor,
        categorical: torch.Tensor,
        user_a_sequence: torch.Tensor,
        user_a_mask: torch.Tensor,
        user_b_sequence: torch.Tensor,
        user_b_mask: torch.Tensor,
        pair_sequence: torch.Tensor,
        pair_mask: torch.Tensor,
    ) -> torch.Tensor:
        user_a = self.user_encoder(user_a_sequence, user_a_mask)
        user_b = self.user_encoder(user_b_sequence, user_b_mask)
        pair = self.pair_encoder(pair_sequence, pair_mask)
        symmetric_users = torch.cat(
            [user_a + user_b, torch.abs(user_a - user_b), user_a * user_b], dim=1
        )
        tabular = self.tabular(
            torch.cat([numeric, self.categories(categorical)], dim=1)
        )
        return self.head(
            torch.cat([tabular, symmetric_users, pair], dim=1)
        ).squeeze(-1)
