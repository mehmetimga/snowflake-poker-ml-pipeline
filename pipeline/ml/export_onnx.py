"""Export trained models to ONNX."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np


def _initial_types(n_features: int):
    from onnxmltools.convert.common.data_types import FloatTensorType  # type: ignore

    return [("input", FloatTensorType([None, n_features]))]


def export_xgboost(model: Any, n_features: int, output: Path) -> None:
    from onnxmltools import convert_xgboost  # type: ignore

    onnx_model = convert_xgboost(model, initial_types=_initial_types(n_features))
    output.write_bytes(onnx_model.SerializeToString())


def export_lightgbm(model: Any, n_features: int, output: Path) -> None:
    from onnxmltools import convert_lightgbm  # type: ignore

    onnx_model = convert_lightgbm(model, initial_types=_initial_types(n_features))
    output.write_bytes(onnx_model.SerializeToString())


def export_catboost(model: Any, n_features: int, output: Path) -> None:
    # CatBoost can save itself directly to ONNX.
    output.parent.mkdir(parents=True, exist_ok=True)
    model.save_model(
        str(output),
        format="onnx",
        export_parameters={
            "onnx_domain": "ai.catboost",
            "onnx_model_version": 1,
            "onnx_doc_string": "snowflake-poker-ml-pipeline catboost",
            "onnx_graph_name": "CatBoostModel",
        },
    )


def export_all(
    output_dir: Path,
    xgb_model: Any,
    cat_model: Any,
    lgbm_model: Any,
    n_features: int,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "xgboost": output_dir / "xgboost.onnx",
        "catboost": output_dir / "catboost.onnx",
        "lightgbm": output_dir / "lightgbm.onnx",
    }
    export_xgboost(xgb_model, n_features, paths["xgboost"])
    export_catboost(cat_model, n_features, paths["catboost"])
    export_lightgbm(lgbm_model, n_features, paths["lightgbm"])
    return paths
