from __future__ import annotations

import numpy as np
from lightgbm import LGBMClassifier


def train_lightgbm(X_train: np.ndarray, y_train: np.ndarray, X_val: np.ndarray, y_val: np.ndarray) -> LGBMClassifier:
    model = LGBMClassifier(
        n_estimators=300,
        max_depth=6,
        num_leaves=31,
        learning_rate=0.1,
        objective="binary",
        class_weight="balanced",
        random_state=42,
        n_jobs=-1,
        verbose=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_val, y_val)])
    return model
