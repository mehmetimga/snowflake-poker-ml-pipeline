from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np
import streamlit as st

from admin import data_access as da
from pipeline.config import get_settings
from pipeline.qdrant.embedding_model import PairFeatureEmbedder
from pipeline.qdrant.pattern_store import PAIR_FEATURE_COLUMNS

st.set_page_config(page_title="Similarity Search", layout="wide")
st.title("Similarity search (Qdrant)")

wh = da.warehouse()
pair_df = da.pair_stats(wh, limit=200)
if pair_df.empty:
    st.info("PAIR_STATS is empty.")
    st.stop()

selection = st.selectbox(
    "Pick a pair to compare",
    options=[f"{r.player_a} | {r.player_b}" for r in pair_df.itertuples()],
)
row = pair_df.iloc[[i for i, r in enumerate(pair_df.itertuples()) if f"{r.player_a} | {r.player_b}" == selection][0]]
features = np.array([row[c] for c in PAIR_FEATURE_COLUMNS], dtype=np.float32)
vec = PairFeatureEmbedder().encode(features)[0]

st.write("Pair features:", dict(zip(PAIR_FEATURE_COLUMNS, features.tolist())))

try:
    from qdrant_client import QdrantClient

    s = get_settings()
    client = QdrantClient(url=s.qdrant_url, timeout=60, check_compatibility=False)

    def _top(collection: str):
        return client.query_points(
            collection_name=collection, query=vec.tolist(), limit=5, with_payload=True
        ).points

    st.subheader("Top matches in collusion_patterns")
    st.write([{"score": r.score, "payload": r.payload} for r in _top(s.qdrant_collusion_collection)])

    st.subheader("Top matches in normal_patterns")
    st.write([{"score": r.score, "payload": r.payload} for r in _top(s.qdrant_normal_collection)])
except Exception as e:
    st.error(f"Qdrant unreachable: {e}")
