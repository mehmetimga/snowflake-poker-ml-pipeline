"""Lightweight deterministic embedding for player-pair behavioral patterns.

Maps a 5-dim pair feature vector (from PAIR_STATS) into a 32-dim embedding via
a fixed random projection. Replaces the sentence-transformers dependency for
the demo without losing the similarity-search story.
"""

from __future__ import annotations

import hashlib

import numpy as np

EMBED_DIM = 32
INPUT_DIM = 5


def _projection_matrix() -> np.ndarray:
    rng = np.random.default_rng(seed=2026)
    return rng.standard_normal((INPUT_DIM, EMBED_DIM)).astype(np.float32) / np.sqrt(INPUT_DIM)


class PairFeatureEmbedder:
    def __init__(self) -> None:
        self.W = _projection_matrix()

    def encode(self, features: np.ndarray) -> np.ndarray:
        if features.ndim == 1:
            features = features[None, :]
        x = features.astype(np.float32)
        # Standardize per-call by clipping to a sensible range
        x = np.clip(x, -10.0, 10.0)
        z = x @ self.W
        # L2 normalize for cosine search
        norm = np.linalg.norm(z, axis=1, keepdims=True) + 1e-9
        return z / norm


def embed_pair(features: np.ndarray) -> np.ndarray:
    return PairFeatureEmbedder().encode(features)
