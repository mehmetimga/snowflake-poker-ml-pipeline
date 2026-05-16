"""Assemble per-(hand_id, player_id) stacking rows for the meta-learner.

Gathers booster predictions (via ONNX runtime), LSTM/Transformer embeddings,
VGAE anomaly scores, and Qdrant min-distance into wide + deep feature vectors.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Tuple

import numpy as np
import onnxruntime as ort
import pandas as pd
import torch

from pipeline.dl.dataset import FEATURE_DIM, build_sequences
from pipeline.dl.lstm_encoder import LSTMEncoder
from pipeline.dl.transformer import TransformerEncoder
from pipeline.features.engineer import FEATURE_COLUMNS
from pipeline.warehouse import Warehouse


def _onnx_predict(path: Path, X: np.ndarray) -> np.ndarray:
    sess = ort.InferenceSession(str(path), providers=["CPUExecutionProvider"])
    input_name = sess.get_inputs()[0].name
    outputs = sess.run(None, {input_name: X.astype(np.float32)})
    for out in outputs:
        if isinstance(out, list) and out and isinstance(out[0], dict):
            return np.array([float(d.get(1, d.get("1", 0.0))) for d in out], dtype=np.float32)
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 1].astype(np.float32)
    return np.asarray(outputs[0]).reshape(-1).astype(np.float32)


def assemble_dataset(
    warehouse: Warehouse,
    models_dir: Path,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, list[tuple[str, str]]]:
    features = warehouse.fetch_df("SELECT * FROM FEATURES")
    flags = warehouse.fetch_df("SELECT hand_id, player_id, rule_score FROM RULE_FLAGS")
    df = features.merge(flags, on=["hand_id", "player_id"], how="inner")
    if df.empty:
        return np.zeros((0, 6)), np.zeros((0, 96)), np.zeros((0,)), []

    X = df[FEATURE_COLUMNS].astype("float32").to_numpy()
    y = df["is_suspicious"].astype("int64").to_numpy()

    # Booster probabilities via ONNX
    xgb_p = _onnx_predict(models_dir / "xgboost.onnx", X)
    cat_p = _onnx_predict(models_dir / "catboost.onnx", X)
    lgbm_p = _onnx_predict(models_dir / "lightgbm.onnx", X)

    # VGAE anomaly per player
    vgae_path = models_dir / "vgae_scores.json"
    vgae_map: dict[str, float] = {}
    if vgae_path.exists():
        vgae_map = {k: float(v) for k, v in json.loads(vgae_path.read_text()).items()}
    vgae_scores = np.array([vgae_map.get(pid, 0.0) for pid in df["player_id"]], dtype=np.float32)

    rule_scores = df["rule_score"].astype(np.float32).to_numpy()

    # Sequence embeddings via LSTM + Transformer (zeros if models or data are missing)
    Xs, _, ids = build_sequences(warehouse)
    lstm_path = models_dir / "lstm.pt"
    trans_path = models_dir / "transformer.pt"
    lstm_default_dim = 64
    trans_default_dim = 32
    if len(Xs) == 0 or not lstm_path.exists() or not trans_path.exists():
        lstm_emb = np.zeros((len(df), lstm_default_dim), dtype=np.float32)
        trans_emb = np.zeros((len(df), trans_default_dim), dtype=np.float32)
    else:
        device = torch.device("cpu")
        lstm = LSTMEncoder(input_dim=FEATURE_DIM)
        lstm.load_state_dict(torch.load(lstm_path, map_location=device))
        lstm.eval()
        trans = TransformerEncoder(input_dim=FEATURE_DIM)
        trans.load_state_dict(torch.load(trans_path, map_location=device))
        trans.eval()
        with torch.no_grad():
            lstm_all = lstm.embed(torch.from_numpy(Xs)).cpu().numpy()
            trans_all = trans.embed(torch.from_numpy(Xs)).cpu().numpy()
        idx_map = {ids_pair: i for i, ids_pair in enumerate(ids)}
        lstm_emb = np.zeros((len(df), lstm_all.shape[1]), dtype=np.float32)
        trans_emb = np.zeros((len(df), trans_all.shape[1]), dtype=np.float32)
        for j, (hid, pid) in enumerate(zip(df["hand_id"], df["player_id"])):
            i = idx_map.get((hid, pid))
            if i is not None:
                lstm_emb[j] = lstm_all[i]
                trans_emb[j] = trans_all[i]

    wide = np.stack([xgb_p, cat_p, lgbm_p, vgae_scores, np.zeros_like(xgb_p), rule_scores], axis=1).astype(np.float32)
    deep = np.concatenate([lstm_emb, trans_emb], axis=1).astype(np.float32)
    ids_pairs = list(zip(df["hand_id"].tolist(), df["player_id"].tolist()))
    return wide, deep, y, ids_pairs
