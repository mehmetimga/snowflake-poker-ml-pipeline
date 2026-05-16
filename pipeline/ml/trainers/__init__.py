"""Lazy imports — keeps a slim image that only has e.g. XGBoost installed
from blowing up on `import pipeline.ml.trainers` because CatBoost/LightGBM
aren't present."""

from __future__ import annotations


def __getattr__(name):
    if name == "train_xgboost":
        from .xgboost_trainer import train_xgboost

        return train_xgboost
    if name == "train_catboost":
        from .catboost_trainer import train_catboost

        return train_catboost
    if name == "train_lightgbm":
        from .lightgbm_trainer import train_lightgbm

        return train_lightgbm
    raise AttributeError(f"module 'pipeline.ml.trainers' has no attribute {name!r}")


__all__ = ["train_xgboost", "train_catboost", "train_lightgbm"]
