import streamlit as st

st.set_page_config(page_title="Retrain", layout="wide")
st.title("Retrain")
st.info(
    "Training is isolated from the persistent admin service. "
    "Submit the governed POKER_TRAIN_JOB through the deployment workflow."
)
st.code("make r7-train-run", language="bash")
st.caption(
    "The training job uses a dedicated immutable poker-train image, exits "
    "after artifact upload, and never runs inside this admin container."
)
