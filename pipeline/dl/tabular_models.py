"""GPU-friendly tabular challenger architectures for pair-risk scoring."""

from __future__ import annotations

from collections.abc import Sequence

import torch
from torch import nn


class CategoricalInput(nn.Module):
    def __init__(self, cardinalities: Sequence[int], embedding_dim: int = 8) -> None:
        super().__init__()
        if any(value < 1 for value in cardinalities):
            raise ValueError("categorical cardinalities must be positive")
        self.embeddings = nn.ModuleList(
            nn.Embedding(int(cardinality), embedding_dim)
            for cardinality in cardinalities
        )
        self.output_dim = len(cardinalities) * embedding_dim

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        if values.ndim != 2 or values.shape[1] != len(self.embeddings):
            raise ValueError("categorical tensor shape disagrees with the model")
        if not self.embeddings:
            return values.new_zeros((len(values), 0), dtype=torch.float32)
        return torch.cat(
            [embedding(values[:, index]) for index, embedding in enumerate(self.embeddings)],
            dim=1,
        )


class ResidualBlock(nn.Module):
    def __init__(self, width: int, dropout: float) -> None:
        super().__init__()
        self.block = nn.Sequential(
            nn.LayerNorm(width),
            nn.Linear(width, width * 2),
            nn.GELU(),
            nn.Dropout(dropout),
            nn.Linear(width * 2, width),
            nn.Dropout(dropout),
        )

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        return values + self.block(values)


class ResidualMLP(nn.Module):
    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        *,
        width: int = 256,
        depth: int = 4,
        dropout: float = 0.1,
        embedding_dim: int = 8,
    ) -> None:
        super().__init__()
        if numeric_dim < 1 or width < 1 or depth < 1:
            raise ValueError("numeric_dim, width, and depth must be positive")
        self.categories = CategoricalInput(categorical_cardinalities, embedding_dim)
        self.input = nn.Linear(numeric_dim + self.categories.output_dim, width)
        self.blocks = nn.Sequential(
            *(ResidualBlock(width, dropout) for _ in range(depth))
        )
        self.head = nn.Sequential(nn.LayerNorm(width), nn.Linear(width, 1))

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        values = torch.cat([numeric, self.categories(categorical)], dim=1)
        return self.head(self.blocks(self.input(values))).squeeze(-1)


class FeatureTokenizer(nn.Module):
    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        token_dim: int,
    ) -> None:
        super().__init__()
        self.numeric_weight = nn.Parameter(torch.empty(numeric_dim, token_dim))
        self.numeric_bias = nn.Parameter(torch.empty(numeric_dim, token_dim))
        self.category_embeddings = nn.ModuleList(
            nn.Embedding(int(cardinality), token_dim)
            for cardinality in categorical_cardinalities
        )
        self.cls = nn.Parameter(torch.empty(1, 1, token_dim))
        nn.init.xavier_uniform_(self.numeric_weight)
        nn.init.zeros_(self.numeric_bias)
        nn.init.normal_(self.cls, std=0.02)

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        numeric_tokens = (
            numeric.unsqueeze(-1) * self.numeric_weight.unsqueeze(0)
            + self.numeric_bias.unsqueeze(0)
        )
        category_tokens = [
            embedding(categorical[:, index]).unsqueeze(1)
            for index, embedding in enumerate(self.category_embeddings)
        ]
        cls = self.cls.expand(len(numeric), -1, -1)
        return torch.cat([cls, numeric_tokens, *category_tokens], dim=1)


class FTTransformer(nn.Module):
    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        *,
        token_dim: int = 32,
        heads: int = 4,
        layers: int = 3,
        dropout: float = 0.1,
    ) -> None:
        super().__init__()
        if token_dim % heads:
            raise ValueError("token_dim must be divisible by heads")
        self.tokenizer = FeatureTokenizer(
            numeric_dim, categorical_cardinalities, token_dim
        )
        layer = nn.TransformerEncoderLayer(
            d_model=token_dim,
            nhead=heads,
            dim_feedforward=token_dim * 2,
            dropout=dropout,
            activation="gelu",
            batch_first=True,
            norm_first=True,
        )
        self.transformer = nn.TransformerEncoder(
            layer, num_layers=layers, enable_nested_tensor=False
        )
        self.head = nn.Sequential(
            nn.LayerNorm(token_dim), nn.GELU(), nn.Linear(token_dim, 1)
        )

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        tokens = self.transformer(self.tokenizer(numeric, categorical))
        return self.head(tokens[:, 0]).squeeze(-1)


class CrossNetV2(nn.Module):
    """Full-matrix DCN-V2 cross layers; practical at this feature width."""

    def __init__(self, input_dim: int, layers: int) -> None:
        super().__init__()
        if input_dim < 1 or layers < 1:
            raise ValueError("cross input dimension and layers must be positive")
        self.layers = nn.ModuleList(nn.Linear(input_dim, input_dim) for _ in range(layers))

    def forward(self, values: torch.Tensor) -> torch.Tensor:
        original = values
        crossed = values
        for layer in self.layers:
            crossed = original * layer(crossed) + crossed
        return crossed


class DCNV2(nn.Module):
    def __init__(
        self,
        numeric_dim: int,
        categorical_cardinalities: Sequence[int],
        *,
        width: int = 256,
        cross_layers: int = 3,
        deep_layers: int = 3,
        dropout: float = 0.1,
        embedding_dim: int = 8,
    ) -> None:
        super().__init__()
        self.categories = CategoricalInput(categorical_cardinalities, embedding_dim)
        input_dim = numeric_dim + self.categories.output_dim
        self.cross = CrossNetV2(input_dim, cross_layers)
        deep: list[nn.Module] = []
        previous = input_dim
        for _ in range(deep_layers):
            deep.extend(
                [nn.Linear(previous, width), nn.LayerNorm(width), nn.GELU(), nn.Dropout(dropout)]
            )
            previous = width
        self.deep = nn.Sequential(*deep)
        self.head = nn.Linear(input_dim + width, 1)

    def forward(self, numeric: torch.Tensor, categorical: torch.Tensor) -> torch.Tensor:
        values = torch.cat([numeric, self.categories(categorical)], dim=1)
        return self.head(torch.cat([self.cross(values), self.deep(values)], dim=1)).squeeze(-1)


MODEL_NAMES = ("residual_mlp", "ft_transformer", "dcn_v2")


def build_tabular_model(
    name: str,
    numeric_dim: int,
    categorical_cardinalities: Sequence[int],
) -> nn.Module:
    if name == "residual_mlp":
        return ResidualMLP(numeric_dim, categorical_cardinalities)
    if name == "ft_transformer":
        return FTTransformer(numeric_dim, categorical_cardinalities)
    if name == "dcn_v2":
        return DCNV2(numeric_dim, categorical_cardinalities)
    raise ValueError(f"unsupported tabular model: {name}")
