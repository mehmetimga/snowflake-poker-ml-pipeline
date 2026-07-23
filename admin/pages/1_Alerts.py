from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

from admin import data_access as da

st.set_page_config(page_title="Alerts", layout="wide")
st.title("Alerts")

wh = da.warehouse()
col1, col2, col3 = st.columns(3)
risk = col1.selectbox("Risk level", ["", "HIGH", "MEDIUM", "LOW"], index=0) or None
status = col2.selectbox("Status", ["", "pending"], index=0) or None
limit = col3.slider("Limit", 50, 1000, 200, 50)

df = da.alerts(wh, status=status, risk=risk, limit=limit)
if df.empty:
    st.info("No alerts match the selected filters.")
else:
    st.dataframe(df, use_container_width=True, hide_index=True)
    st.caption(f"{len(df)} alert(s) shown.")
