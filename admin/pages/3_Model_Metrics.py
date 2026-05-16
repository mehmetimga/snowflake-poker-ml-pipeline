from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import plotly.express as px
import streamlit as st

from admin import data_access as da

st.set_page_config(page_title="Model Metrics", layout="wide")
st.title("Model metrics")

wh = da.warehouse()
df = da.model_metrics(wh)
if df.empty:
    st.info("No model runs yet. Run `python scripts/train.py`.")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)
    fig = px.bar(df, x="model_name", y=["roc_auc", "pr_auc", "f1"], barmode="group", facet_col="run_id")
    st.plotly_chart(fig, use_container_width=True)
