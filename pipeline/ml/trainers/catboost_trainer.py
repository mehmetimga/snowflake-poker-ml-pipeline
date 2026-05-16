from __future__ import annotations

import os

import numpy as np
from catboost import CatBoostClassifier


def train_catboost(X_train: np.ndarray, y_train: np.ndarray, X_val: np.ndarray, y_val: np.ndarray) -> CatBoostClassifier:
    model = CatBoostClassifier(
        iterations=300,
        depth=6,
        learning_rate=0.1,
        loss_function="Logloss",
        eval_metric="AUC",
        task_type=os.environ.get("CAT_TASK_TYPE", "CPU"),
        auto_class_weights="Balanced",
        random_seed=42,
        verbose=False,
    )
    model.fit(X_train, y_train, eval_set=(X_val, y_val), verbose=False)
    return model
