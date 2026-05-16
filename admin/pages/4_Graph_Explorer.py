from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import networkx as nx
import streamlit as st
from pyvis.network import Network

from admin import data_access as da

st.set_page_config(page_title="Graph Explorer", layout="wide")
st.title("Graph explorer")

wh = da.warehouse()
pair_df = da.pair_stats(wh, limit=400)
vgae = da.vgae_scores()

if pair_df.empty:
    st.info("PAIR_STATS is empty. Run feature engineering + GNN training first.")
else:
    G = nx.Graph()
    for _, row in pair_df.iterrows():
        a, b = row["player_a"], row["player_b"]
        G.add_node(a, anomaly=vgae.get(a, 0.0))
        G.add_node(b, anomaly=vgae.get(b, 0.0))
        G.add_edge(a, b, weight=float(row["pair_score"]))

    net = Network(height="650px", width="100%", bgcolor="#ffffff", font_color="#222222", notebook=False)
    for node, attrs in G.nodes(data=True):
        anomaly = attrs.get("anomaly", 0.0)
        color = "#d62728" if anomaly > 0.6 else "#1f77b4" if anomaly < 0.3 else "#ff7f0e"
        net.add_node(node, label=node[:8], title=f"anomaly={anomaly:.2f}", color=color, size=8 + 20 * anomaly)
    for u, v, attrs in G.edges(data=True):
        net.add_edge(u, v, value=attrs["weight"])
    net.toggle_physics(True)
    html = net.generate_html(notebook=False)
    st.components.v1.html(html, height=700, scrolling=True)
    st.caption("Red nodes have higher VGAE anomaly scores.")
