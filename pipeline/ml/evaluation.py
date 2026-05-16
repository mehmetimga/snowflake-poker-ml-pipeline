from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np
from sklearn.metrics import (
    average_precision_score,
    f1_score,
    precision_recall_curve,
    roc_auc_score,
)


@dataclass
class ModelMetrics:
    model_name: str
    roc_auc: float
    pr_auc: float
    f1: float
    optimal_threshold: float

    def to_dict(self) -> dict:
        return asdict(self)


def evaluate_model(model_name: str, y_true: np.ndarray, y_proba: np.ndarray) -> ModelMetrics:
    if len(np.unique(y_true)) < 2:
        return ModelMetrics(model_name=model_name, roc_auc=0.5, pr_auc=0.0, f1=0.0, optimal_threshold=0.5)

    roc = roc_auc_score(y_true, y_proba)
    pr = average_precision_score(y_true, y_proba)

    precision, recall, thresholds = precision_recall_curve(y_true, y_proba)
    f1_scores = 2 * precision * recall / (precision + recall + 1e-9)
    best_idx = int(np.argmax(f1_scores))
    best_thr = float(thresholds[min(best_idx, len(thresholds) - 1)]) if len(thresholds) else 0.5
    f1 = float(f1_score(y_true, (y_proba >= best_thr).astype(int), zero_division=0))

    return ModelMetrics(
        model_name=model_name,
        roc_auc=float(roc),
        pr_auc=float(pr),
        f1=f1,
        optimal_threshold=best_thr,
    )
