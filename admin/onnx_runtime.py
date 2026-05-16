"""Lazy ONNX model loader for live scoring inside Streamlit."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path

import numpy as np
import onnxruntime as ort

from admin.data_access import models_dir


@lru_cache(maxsize=8)
def _session(name: str) -> ort.InferenceSession:
    path = models_dir() / f"{name}.onnx"
    return ort.InferenceSession(str(path), providers=["CPUExecutionProvider"])


def predict_proba(name: str, X: np.ndarray) -> np.ndarray:
    sess = _session(name)
    input_name = sess.get_inputs()[0].name
    outputs = sess.run(None, {input_name: X.astype(np.float32)})
    for out in outputs:
        if isinstance(out, list) and out and isinstance(out[0], dict):
            return np.array([float(d.get(1, d.get("1", 0.0))) for d in out], dtype=np.float32)
        arr = np.asarray(out)
        if arr.ndim == 2 and arr.shape[1] == 2:
            return arr[:, 1].astype(np.float32)
    return np.asarray(outputs[0]).reshape(-1).astype(np.float32)
