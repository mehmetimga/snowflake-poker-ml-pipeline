"""Streamlit admin home page — KPIs + recent high-risk alerts."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from admin import data_access as da

st.set_page_config(page_title="Poker Collusion Demo", page_icon=":spades:", layout="wide")
st.title("Poker collusion ML demo")
st.caption("Synthetic data only. Toggle Snowflake vs DuckDB via WAREHOUSE_BACKEND.")

wh = da.warehouse()
counts = da.kpi_counts(wh)

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Hands", counts.get("raw_hands", 0))
c2.metric("Player rows", counts.get("raw_players", 0))
c3.metric("Alerts", counts.get("alerts", 0))
c4.metric("High risk", counts.get("alerts_high", 0))
c5.metric("Backend", wh.kind.upper())

st.subheader("Top 25 alerts by risk score")
top = da.alerts(wh, limit=25)
if top.empty:
    st.info("No alerts yet. Run `make demo` (or `python scripts/train.py && python -m pipeline.inference.scorer`).")
else:
    st.dataframe(
        top[["alert_id", "hand_id", "suspicious_player_id", "risk_level", "risk_score", "status"]],
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Pipeline pages")
st.markdown(
    """
- **1 — Alerts**: filter & triage the alert backlog
- **2 — Hand viewer**: inspect a single hand + live model scoring
- **3 — Model metrics**: ROC/PR/F1 for every model run
- **4 — Graph explorer**: VGAE anomaly view of the player graph
- **5 — Retrain**: run the training pipeline from the UI
- **6 — Similarity search**: find Qdrant nearest-neighbour patterns
"""
)
