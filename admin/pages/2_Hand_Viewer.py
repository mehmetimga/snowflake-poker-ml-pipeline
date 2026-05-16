from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import numpy as np
import pandas as pd
import streamlit as st

from admin import data_access as da
from admin import onnx_runtime as ort_rt
from pipeline.features.engineer import FEATURE_COLUMNS

st.set_page_config(page_title="Hand Viewer", layout="wide")
st.title("Hand viewer")

wh = da.warehouse()
hand_id = st.text_input("Hand ID", value="H-00000000")

if hand_id:
    detail = da.hand_detail(wh, hand_id)
    hands_df = detail["raw_hands"]
    players_df = detail["raw_players"]
    actions_df = detail["raw_actions"]

    if hands_df.empty:
        st.warning("Hand not found.")
    else:
        st.subheader("Hand")
        st.dataframe(hands_df, use_container_width=True, hide_index=True)

        st.subheader("Players")
        st.dataframe(players_df, use_container_width=True, hide_index=True)

        st.subheader("Action timeline")
        if not actions_df.empty:
            for street in ("preflop", "flop", "turn", "river"):
                sub = actions_df[actions_df["street"] == street]
                if not sub.empty:
                    st.markdown(f"**{street}**")
                    st.dataframe(sub[["sequence_no", "player_id", "action_type", "amount"]], use_container_width=True, hide_index=True)

        st.subheader("Live ONNX scoring")
        try:
            feats = wh.fetch_df(f"SELECT * FROM FEATURES WHERE hand_id = '{hand_id}'")
        except Exception:
            feats = pd.DataFrame()
        if feats.empty:
            st.info("No FEATURES rows for this hand yet.")
        else:
            X = feats[FEATURE_COLUMNS].astype("float32").to_numpy()
            scores = {}
            for name in ("xgboost", "catboost", "lightgbm"):
                try:
                    scores[name] = ort_rt.predict_proba(name, X)
                except Exception as e:
                    scores[name] = None
                    st.caption(f"{name}: unavailable ({e})")
            out = feats[["player_id"]].copy()
            for k, v in scores.items():
                if v is not None:
                    out[k] = np.round(v, 4)
            st.dataframe(out, use_container_width=True, hide_index=True)
