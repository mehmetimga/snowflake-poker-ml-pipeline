"""Reference-based feature and score drift monitoring."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _distribution(values: np.ndarray, edges: np.ndarray) -> np.ndarray:
    counts, _ = np.histogram(values, bins=edges)
    return counts.astype(np.float64) / max(int(counts.sum()), 1)


def _finite(values: Sequence[Any]) -> np.ndarray:
    numeric = pd.to_numeric(pd.Series(values), errors="coerce").to_numpy(dtype=np.float64)
    return numeric[np.isfinite(numeric)]


def numeric_reference(values: Sequence[Any], bins: int = 10) -> dict[str, Any]:
    clean = _finite(values)
    if not len(clean):
        edges = np.asarray([-np.inf, np.inf], dtype=np.float64)
    else:
        quantiles = np.quantile(clean, np.linspace(0, 1, bins + 1))
        internal = np.unique(quantiles[1:-1])
        edges = np.concatenate(([-np.inf], internal, [np.inf])).astype(np.float64)
    proportions = _distribution(clean, edges)
    return {
        "edges": [float(value) for value in edges],
        "proportions": [float(value) for value in proportions],
        "finite_rows": int(len(clean)),
    }


def population_stability_index(
    reference_proportions: Sequence[float], current_proportions: Sequence[float]
) -> float:
    expected = np.clip(np.asarray(reference_proportions, dtype=np.float64), 1e-6, None)
    actual = np.clip(np.asarray(current_proportions, dtype=np.float64), 1e-6, None)
    if expected.shape != actual.shape:
        raise ValueError("PSI distributions must have the same shape")
    expected /= expected.sum()
    actual /= actual.sum()
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def build_drift_reference(
    dataset_dir: Path,
    model_dir: Path,
    output_path: Path,
    *,
    benchmark: str = "cold_start",
    reference_split: str = "validation",
    bins: int = 10,
) -> dict[str, Any]:
    dataset_dir, model_dir = dataset_dir.resolve(), model_dir.resolve()
    manifest = json.loads((dataset_dir / "manifest.json").read_text())
    schema = json.loads((dataset_dir / "schema.json").read_text())
    if reference_split not in {"train", "validation"}:
        raise ValueError("drift reference split must be train or validation")
    reference_path = dataset_dir / "dgx" / benchmark / f"{reference_split}.parquet"
    reference_frame = pd.read_parquet(reference_path)
    predictions = pd.read_parquet(model_dir / "predictions.parquet")
    validation_scores = predictions.loc[
        predictions["split"] == "validation", "calibrated_probability"
    ].to_numpy(dtype=np.float64)
    numeric = {
        column: numeric_reference(reference_frame[column], bins=bins)
        for column in schema["numeric_feature_columns"]
    }
    categorical: dict[str, Any] = {}
    for column in schema["categorical_feature_columns"]:
        proportions = (
            reference_frame[column].fillna("__MISSING__").astype(str).value_counts(normalize=True)
        )
        categorical[column] = {
            "proportions": {
                str(key): float(value) for key, value in proportions.sort_index().items()
            }
        }
    reference = {
        "contract_version": 1,
        "created_at": datetime.now(tz=timezone.utc).isoformat(),
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest_sha256": _sha256(dataset_dir / "manifest.json"),
        "benchmark": benchmark,
        "reference_split": reference_split,
        "feature_definition_version": manifest["feature_definition_version"],
        "numeric_features": numeric,
        "categorical_features": categorical,
        "score": {
            "reference_split": "validation",
            **numeric_reference(validation_scores, bins=bins),
        },
        "thresholds": {"warning_psi": 0.10, "critical_psi": 0.25},
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(reference, indent=2, sort_keys=True) + "\n")
    return reference


def _categorical_tvd(reference: Mapping[str, float], current: pd.Series) -> float:
    actual = current.fillna("__MISSING__").astype(str).value_counts(normalize=True).to_dict()
    categories = set(reference) | set(actual)
    return float(
        0.5
        * sum(abs(float(reference.get(key, 0.0)) - float(actual.get(key, 0.0))) for key in categories)
    )


def evaluate_drift(
    reference: Mapping[str, Any],
    frame: pd.DataFrame,
    scores: Sequence[float],
    *,
    split: str,
    model_name: str,
    model_run_id: str,
) -> dict[str, Any]:
    warning = float(reference["thresholds"]["warning_psi"])
    critical = float(reference["thresholds"]["critical_psi"])
    numeric_results: dict[str, Any] = {}
    for column, contract in reference["numeric_features"].items():
        clean = _finite(frame[column])
        edges = np.asarray(contract["edges"], dtype=np.float64)
        current = _distribution(clean, edges)
        psi = population_stability_index(contract["proportions"], current)
        numeric_results[column] = {
            "psi": psi,
            "status": "critical" if psi >= critical else "warning" if psi >= warning else "ok",
            "finite_rows": int(len(clean)),
        }
    categorical_results: dict[str, Any] = {}
    for column, contract in reference["categorical_features"].items():
        tvd = _categorical_tvd(contract["proportions"], frame[column])
        categorical_results[column] = {
            "total_variation_distance": tvd,
            "status": "critical" if tvd >= critical else "warning" if tvd >= warning else "ok",
        }
    score_contract = reference["score"]
    score_values = _finite(scores)
    score_distribution = _distribution(
        score_values, np.asarray(score_contract["edges"], dtype=np.float64)
    )
    score_psi = population_stability_index(score_contract["proportions"], score_distribution)
    statuses = [item["status"] for item in numeric_results.values()] + [
        item["status"] for item in categorical_results.values()
    ]
    statuses.append("critical" if score_psi >= critical else "warning" if score_psi >= warning else "ok")
    overall = "critical" if "critical" in statuses else "warning" if "warning" in statuses else "ok"
    return {
        "contract_version": 1,
        "evaluated_at": datetime.now(tz=timezone.utc).isoformat(),
        "dataset_id": reference["dataset_id"],
        "benchmark": reference["benchmark"],
        "current_split": split,
        "model_name": model_name,
        "model_run_id": model_run_id,
        "rows": len(frame),
        "status": overall,
        "numeric_features": numeric_results,
        "categorical_features": categorical_results,
        "score": {
            "psi": score_psi,
            "status": "critical" if score_psi >= critical else "warning" if score_psi >= warning else "ok",
        },
        "summary": {
            "critical_checks": statuses.count("critical"),
            "warning_checks": statuses.count("warning"),
            "ok_checks": statuses.count("ok"),
            "max_numeric_psi": max(item["psi"] for item in numeric_results.values()),
        },
    }
