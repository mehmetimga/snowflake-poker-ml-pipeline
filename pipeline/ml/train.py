"""End-to-end classical-ML training orchestrator.

Reads FEATURES + RULE_FLAGS from the warehouse, splits 80/20 stratified,
trains XGBoost/CatBoost/LightGBM, evaluates, and exports ONNX.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from pipeline.config import get_settings
from pipeline.features.engineer import FEATURE_COLUMNS, prepare_matrix
from pipeline.ml.evaluation import evaluate_model
from pipeline.ml.export_onnx import export_all
from pipeline.ml.trainers import train_catboost, train_lightgbm, train_xgboost
from pipeline.warehouse import Warehouse, get_warehouse


def _load_dataset(warehouse: Warehouse) -> pd.DataFrame:
    features = warehouse.fetch_df("SELECT * FROM FEATURES")
    flags = warehouse.fetch_df(
        "SELECT hand_id, player_id, flag_eligible, rule_score FROM RULE_FLAGS"
    )
    if features.empty or flags.empty:
        raise RuntimeError("FEATURES or RULE_FLAGS table is empty — run feature engineering first.")
    df = features.merge(flags, on=["hand_id", "player_id"], how="inner")
    df = df[df["flag_eligible"].astype(bool)]
    return df


def train_all(warehouse: Warehouse | None = None, output_dir: Path | None = None) -> dict:
    settings = get_settings()
    wh = warehouse or get_warehouse()
    out_dir = Path(output_dir or settings.models_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    df = _load_dataset(wh)
    X, y = prepare_matrix(df)

    if len(np.unique(y)) < 2:
        raise RuntimeError("Need both suspicious and non-suspicious labels in the dataset.")

    X_train_full, X_test, y_train_full, y_test = train_test_split(
        X, y, test_size=0.20, stratify=y, random_state=settings.random_seed
    )
    X_train, X_val, y_train, y_val = train_test_split(
        X_train_full,
        y_train_full,
        test_size=0.10,
        stratify=y_train_full,
        random_state=settings.random_seed,
    )

    print(f"[train] n_train={len(X_train)} n_val={len(X_val)} n_test={len(X_test)} pos_rate={y.mean():.3f}")

    xgb_model = train_xgboost(X_train, y_train, X_val, y_val)
    cat_model = train_catboost(X_train, y_train, X_val, y_val)
    lgbm_model = train_lightgbm(X_train, y_train, X_val, y_val)

    metrics = {
        "xgboost": evaluate_model("xgboost", y_test, xgb_model.predict_proba(X_test)[:, 1]),
        "catboost": evaluate_model("catboost", y_test, cat_model.predict_proba(X_test)[:, 1]),
        "lightgbm": evaluate_model("lightgbm", y_test, lgbm_model.predict_proba(X_test)[:, 1]),
    }
    for name, m in metrics.items():
        print(f"[train] {name}: roc={m.roc_auc:.3f} pr={m.pr_auc:.3f} f1={m.f1:.3f} thr={m.optimal_threshold:.3f}")

    paths = export_all(out_dir, xgb_model, cat_model, lgbm_model, n_features=X.shape[1])
    print(f"[train] ONNX exports: {[str(p) for p in paths.values()]}")

    feature_info = {
        "columns": FEATURE_COLUMNS,
        "n_features": int(X.shape[1]),
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "n_test": int(len(X_test)),
    }
    (out_dir / "feature_info.json").write_text(json.dumps(feature_info, indent=2))
    thresholds = {name: m.optimal_threshold for name, m in metrics.items()}
    (out_dir / "optimal_thresholds.json").write_text(json.dumps(thresholds, indent=2))
    comparison_rows = [m.to_dict() for m in metrics.values()]
    pd.DataFrame(comparison_rows).to_csv(out_dir / "model_comparison.csv", index=False)

    run_id = f"run_{uuid.uuid4().hex[:8]}"
    trained_at = datetime.now(tz=timezone.utc).isoformat()
    metrics_rows = pd.DataFrame(
        [
            {
                "run_id": run_id,
                "model_name": name,
                "roc_auc": m.roc_auc,
                "pr_auc": m.pr_auc,
                "f1": m.f1,
                "optimal_threshold": m.optimal_threshold,
                "n_train": int(len(X_train)),
                "n_test": int(len(X_test)),
                "trained_at": trained_at,
            }
            for name, m in metrics.items()
        ]
    )
    wh.write_pandas(metrics_rows, "MODEL_METRICS")

    return {
        "run_id": run_id,
        "metrics": {name: m.to_dict() for name, m in metrics.items()},
        "paths": {k: str(p) for k, p in paths.items()},
    }
