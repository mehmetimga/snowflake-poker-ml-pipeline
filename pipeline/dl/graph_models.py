"""Inductive relation-aware temporal GraphSAGE for pair-risk scoring."""

from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn

from .graph_dataset import RESOURCE_TYPES
from .tabular_models import CategoricalInput, ResidualBlock


def masked_mean(values: torch.Tensor, mask: torch.Tensor, dim: int) -> torch.Tensor:
    weights = mask.to(values.dtype).unsqueeze(-1)
    total = (values * weights).sum(dim=dim)
    count = weights.sum(dim=dim).clamp_min(1.0)
    return total / count


class TemporalHeteroGraphSAGE(nn.Module):
    """Feature-only heterogeneous message passing; contains no raw-ID embeddings."""

    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        root_feature_dim: int,
        user_edge_dim: int,
        resource_feature_dim: int,
        pair_graph_dim: int,
        *,
        width: int = 64,
        tabular_width: int = 128,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        if min(
            numeric_dim,
            root_feature_dim,
            user_edge_dim,
            resource_feature_dim,
            pair_graph_dim,
            width,
        ) < 1:
            raise ValueError("graph model dimensions must be positive")
        self.raw_id_embedding_count = 0
        self.categories = CategoricalInput(categorical_cardinalities, embedding_dim=8)
        self.tabular = nn.Sequential(
            nn.Linear(numeric_dim + self.categories.output_dim, tabular_width),
            ResidualBlock(tabular_width, dropout),
            nn.LayerNorm(tabular_width),
        )
        self.root_projection = nn.Sequential(
            nn.Linear(root_feature_dim, width), nn.LayerNorm(width), nn.GELU()
        )
        self.user_message = nn.Sequential(
            nn.Linear(root_feature_dim + user_edge_dim, width),
            nn.LayerNorm(width),
            nn.GELU(),
        )
        self.resource_messages = nn.ModuleList(
            nn.Sequential(
                nn.Linear(resource_feature_dim, width),
                nn.LayerNorm(width),
                nn.GELU(),
            )
            for _ in RESOURCE_TYPES
        )
        relation_count = 2 + len(RESOURCE_TYPES)
        self.root_update = nn.Sequential(
            nn.Linear(width * relation_count, width),
            nn.LayerNorm(width),
            nn.GELU(),
            nn.Dropout(dropout),
            ResidualBlock(width, dropout),
        )
        self.pair_projection = nn.Sequential(
            nn.Linear(pair_graph_dim, width), nn.LayerNorm(width), nn.GELU()
        )
        combined_dim = tabular_width + width * 4
        self.head = nn.Sequential(
            nn.Linear(combined_dim, 256),
            nn.LayerNorm(256),
            nn.GELU(),
            nn.Dropout(dropout),
            ResidualBlock(256, dropout),
            nn.LayerNorm(256),
            nn.Linear(256, 1),
        )

    def _encode_endpoints(
        self,
        root_features: torch.Tensor,
        user_neighbor_features: torch.Tensor,
        user_edge_features: torch.Tensor,
        user_neighbor_masks: torch.Tensor,
        resource_features: torch.Tensor,
        resource_masks: torch.Tensor,
    ) -> torch.Tensor:
        root = self.root_projection(root_features.float())
        user_input = torch.cat(
            [user_neighbor_features.float(), user_edge_features.float()], dim=-1
        )
        user_messages = masked_mean(
            self.user_message(user_input), user_neighbor_masks, dim=2
        )
        resource_messages = []
        for relation_index, projection in enumerate(self.resource_messages):
            values = projection(resource_features[:, :, relation_index].float())
            resource_messages.append(
                masked_mean(values, resource_masks[:, :, relation_index], dim=2)
            )
        return self.root_update(torch.cat([root, user_messages, *resource_messages], dim=-1))

    def forward(
        self,
        numeric: torch.Tensor,
        categorical: torch.Tensor,
        root_features: torch.Tensor,
        user_neighbor_features: torch.Tensor,
        user_edge_features: torch.Tensor,
        user_neighbor_masks: torch.Tensor,
        resource_features: torch.Tensor,
        resource_masks: torch.Tensor,
        pair_graph_features: torch.Tensor,
    ) -> torch.Tensor:
        endpoints = self._encode_endpoints(
            root_features,
            user_neighbor_features,
            user_edge_features,
            user_neighbor_masks,
            resource_features,
            resource_masks,
        )
        left, right = endpoints[:, 0], endpoints[:, 1]
        symmetric = torch.cat(
            [left + right, torch.abs(left - right), left * right], dim=1
        )
        pair = self.pair_projection(pair_graph_features.float())
        tabular = self.tabular(
            torch.cat([numeric, self.categories(categorical)], dim=1)
        )
        return self.head(torch.cat([tabular, symmetric, pair], dim=1)).squeeze(-1)
