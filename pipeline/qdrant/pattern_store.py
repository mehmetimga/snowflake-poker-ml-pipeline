"""Wraps Qdrant client for pair-pattern similarity search."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import numpy as np
import pandas as pd
from qdrant_client import QdrantClient
from qdrant_client.http import models as qm

from pipeline.config import get_settings
from pipeline.warehouse import Warehouse, get_warehouse

from .embedding_model import EMBED_DIM, PairFeatureEmbedder


PAIR_FEATURE_COLUMNS = [
    "hands_together",
    "chip_transfer_ratio",
    "soft_play_frequency",
    "fold_benefit_ratio",
    "showdown_avoidance_rate",
]


@dataclass
class PatternStore:
    client: QdrantClient
    collusion_collection: str
    normal_collection: str

    def ensure_collections(self) -> None:
        for name in (self.collusion_collection, self.normal_collection):
            if not self.client.collection_exists(name):
                self.client.create_collection(
                    collection_name=name,
                    vectors_config=qm.VectorParams(size=EMBED_DIM, distance=qm.Distance.COSINE),
                )

    def upsert(self, collection: str, points: Iterable[qm.PointStruct], batch_size: int = 500) -> None:
        batch: list[qm.PointStruct] = []
        for p in points:
            batch.append(p)
            if len(batch) >= batch_size:
                self.client.upsert(collection_name=collection, points=batch, wait=True)
                batch = []
        if batch:
            self.client.upsert(collection_name=collection, points=batch, wait=True)

    def query(self, collection: str, vector: np.ndarray, limit: int = 5):
        return self.client.query_points(
            collection_name=collection, query=vector.tolist(), limit=limit, with_payload=True
        ).points


def _make_store() -> PatternStore:
    s = get_settings()
    client = QdrantClient(url=s.qdrant_url, timeout=60, check_compatibility=False)
    return PatternStore(
        client=client,
        collusion_collection=s.qdrant_collusion_collection,
        normal_collection=s.qdrant_normal_collection,
    )


def seed_patterns(warehouse: Warehouse | None = None) -> dict:
    """Seed Qdrant from PAIR_STATS — high pair_score → collusion collection, low → normal."""
    wh = warehouse or get_warehouse()
    pair_stats = wh.fetch_df("SELECT * FROM PAIR_STATS")
    if pair_stats.empty:
        print("[qdrant] PAIR_STATS empty — nothing to seed.")
        return {}

    store = _make_store()
    store.ensure_collections()

    threshold = pair_stats["pair_score"].quantile(0.75)
    embedder = PairFeatureEmbedder()
    vecs = embedder.encode(pair_stats[PAIR_FEATURE_COLUMNS].to_numpy(dtype=np.float32))

    colluding, normal = [], []
    for i, row in enumerate(pair_stats.itertuples()):
        payload = {
            "player_a": row.player_a,
            "player_b": row.player_b,
            "hands_together": int(row.hands_together),
            "pair_score": float(row.pair_score),
            "chip_transfer_ratio": float(row.chip_transfer_ratio),
        }
        point = qm.PointStruct(id=i, vector=vecs[i].tolist(), payload=payload)
        if row.pair_score >= threshold:
            colluding.append(point)
        else:
            normal.append(point)

    if colluding:
        store.upsert(store.collusion_collection, colluding)
    if normal:
        store.upsert(store.normal_collection, normal)

    print(f"[qdrant] seeded collusion={len(colluding)} normal={len(normal)} (threshold={threshold:.2f})")
    return {"collusion": len(colluding), "normal": len(normal)}
