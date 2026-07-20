"""Training and inference primitives for the pair-level CatBoost model.

The preprocessor is deliberately small and serializable. Numeric fill values and
categorical vocabularies are fitted on the training split only, then reused for
validation, test, challenge, ONNX, and future Go/Triton inference.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    brier_score_loss,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)


MISSING_CATEGORY = "__MISSING__"
UNKNOWN_CATEGORY = "__UNKNOWN__"


@dataclass(frozen=True)
class PairPreprocessor:
    """A train-fitted, language-neutral transformation contract."""

    numeric_columns: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    numeric_fill_values: Mapping[str, float]
    categorical_values: Mapping[str, tuple[str, ...]]

    @classmethod
    def fit(
        cls,
        frame: pd.DataFrame,
        numeric_columns: Sequence[str],
        categorical_columns: Sequence[str],
    ) -> "PairPreprocessor":
        required = set(numeric_columns) | set(categorical_columns)
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"missing model columns: {missing}")

        fill_values: dict[str, float] = {}
        for column in numeric_columns:
            values = pd.to_numeric(frame[column], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            median = values.median(skipna=True)
            fill_values[column] = 0.0 if pd.isna(median) else float(median)

        categories: dict[str, tuple[str, ...]] = {}
        for column in categorical_columns:
            values = frame[column].fillna(MISSING_CATEGORY).astype(str)
            observed = sorted(
                value for value in set(values) if value != UNKNOWN_CATEGORY
            )
            categories[column] = tuple(observed + [UNKNOWN_CATEGORY])

        return cls(
            numeric_columns=tuple(numeric_columns),
            categorical_columns=tuple(categorical_columns),
            numeric_fill_values=fill_values,
            categorical_values=categories,
        )

    @property
    def output_columns(self) -> tuple[str, ...]:
        names = list(self.numeric_columns)
        for column in self.categorical_columns:
            names.extend(
                f"{column}=={value}" for value in self.categorical_values[column]
            )
        return tuple(names)

    def transform(self, frame: pd.DataFrame) -> np.ndarray:
        required = set(self.numeric_columns) | set(self.categorical_columns)
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"missing model columns: {missing}")

        blocks: list[np.ndarray] = []
        numeric = pd.DataFrame(index=frame.index)
        for column in self.numeric_columns:
            numeric[column] = (
                pd.to_numeric(frame[column], errors="coerce")
                .replace([np.inf, -np.inf], np.nan)
                .fillna(float(self.numeric_fill_values[column]))
                .astype("float32")
            )
        blocks.append(numeric.to_numpy(dtype=np.float32, copy=True))

        for column in self.categorical_columns:
            known = self.categorical_values[column]
            known_without_unknown = set(known) - {UNKNOWN_CATEGORY}
            values = frame[column].fillna(MISSING_CATEGORY).astype(str)
            values = values.where(values.isin(known_without_unknown), UNKNOWN_CATEGORY)
            block = np.column_stack(
                [(values == category).to_numpy(dtype=np.float32) for category in known]
            )
            blocks.append(block.astype(np.float32, copy=False))

        matrix = np.concatenate(blocks, axis=1)
        if not np.isfinite(matrix).all():
            raise ValueError("preprocessed pair matrix contains non-finite values")
        return matrix

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": 1,
            "numeric_columns": list(self.numeric_columns),
            "categorical_columns": list(self.categorical_columns),
            "numeric_fill_values": {
                key: float(value) for key, value in self.numeric_fill_values.items()
            },
            "categorical_values": {
                key: list(values) for key, values in self.categorical_values.items()
            },
            "output_columns": list(self.output_columns),
            "output_dtype": "float32",
        }

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PairPreprocessor":
        return cls(
            numeric_columns=tuple(raw["numeric_columns"]),
            categorical_columns=tuple(raw["categorical_columns"]),
            numeric_fill_values={
                str(key): float(value)
                for key, value in raw["numeric_fill_values"].items()
            },
            categorical_values={
                str(key): tuple(str(value) for value in values)
                for key, values in raw["categorical_values"].items()
            },
        )


def _logit(probabilities: np.ndarray) -> np.ndarray:
    clipped = np.clip(np.asarray(probabilities, dtype=np.float64), 1e-6, 1 - 1e-6)
    return np.log(clipped / (1.0 - clipped))


def _sigmoid(values: np.ndarray) -> np.ndarray:
    values = np.asarray(values, dtype=np.float64)
    output = np.empty_like(values)
    positive = values >= 0
    output[positive] = 1.0 / (1.0 + np.exp(-values[positive]))
    exp_values = np.exp(values[~positive])
    output[~positive] = exp_values / (1.0 + exp_values)
    return output


@dataclass(frozen=True)
class PlattCalibrator:
    """Serializable sigmoid calibration fitted only on validation predictions."""

    slope: float = 1.0
    intercept: float = 0.0
    method: str = "identity"

    @classmethod
    def fit(cls, labels: Sequence[int], probabilities: Sequence[float]) -> "PlattCalibrator":
        y = np.asarray(labels, dtype=np.int8)
        p = np.asarray(probabilities, dtype=np.float64)
        if len(np.unique(y)) < 2 or len(np.unique(p)) < 2:
            return cls(method="identity_insufficient_validation_classes")
        model = LogisticRegression(
            class_weight="balanced",
            random_state=42,
            solver="lbfgs",
        )
        model.fit(_logit(p).reshape(-1, 1), y)
        slope = float(model.coef_[0, 0])
        # Calibration may change probability scale but must not reverse model
        # ranking. A negative fit can occur on a tiny/noisy validation sample.
        if slope <= 0:
            return cls(method="identity_non_monotonic_validation_fit")
        return cls(
            slope=slope,
            intercept=float(model.intercept_[0]),
            method="platt_validation",
        )

    def predict(self, probabilities: Sequence[float]) -> np.ndarray:
        return _sigmoid(self.slope * _logit(np.asarray(probabilities)) + self.intercept)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "PlattCalibrator":
        return cls(
            slope=float(raw["slope"]),
            intercept=float(raw["intercept"]),
            method=str(raw["method"]),
        )


def select_alert_budget_threshold(
    labels: Sequence[int],
    probabilities: Sequence[float],
    max_alert_rate: float,
) -> float:
    """Maximize validation F1 without exceeding the configured alert budget."""
    if not 0 < max_alert_rate <= 1:
        raise ValueError("max_alert_rate must be in (0, 1]")
    y = np.asarray(labels, dtype=np.int8)
    p = np.asarray(probabilities, dtype=np.float64)
    if len(y) == 0 or len(y) != len(p):
        raise ValueError("labels and probabilities must be non-empty and aligned")

    allowed = max(1, int(math.floor(len(y) * max_alert_rate)))
    best: tuple[float, float, float, float] | None = None
    for threshold in sorted(set(float(value) for value in p), reverse=True):
        predicted = p >= threshold
        if int(predicted.sum()) > allowed:
            continue
        f1 = float(f1_score(y, predicted, zero_division=0))
        recall = float(recall_score(y, predicted, zero_division=0))
        precision = float(precision_score(y, predicted, zero_division=0))
        candidate = (f1, recall, precision, threshold)
        if best is None or candidate > best:
            best = candidate
    if best is None:
        return float(np.nextafter(float(p.max()), math.inf))
    return best[3]


def _top_budget_metrics(
    labels: np.ndarray,
    probabilities: np.ndarray,
    max_alert_rate: float,
) -> tuple[float | None, float]:
    allowed = max(1, int(math.floor(len(labels) * max_alert_rate)))
    ranked = np.argsort(-probabilities, kind="stable")[:allowed]
    precision = float(labels[ranked].mean()) if len(ranked) else 0.0
    positives = int(labels.sum())
    recall = float(labels[ranked].sum() / positives) if positives else None
    return recall, precision


def binary_classification_report(
    labels: Sequence[int],
    probabilities: Sequence[float],
    *,
    threshold: float,
    max_alert_rate: float,
    hand_count: int,
) -> dict[str, Any]:
    """Return statistical and operational pair-ranking metrics."""
    y = np.asarray(labels, dtype=np.int8)
    p = np.asarray(probabilities, dtype=np.float64)
    if len(y) == 0 or len(y) != len(p):
        raise ValueError("labels and probabilities must be non-empty and aligned")
    predicted = p >= threshold
    positives = int(y.sum())
    negatives = int(len(y) - positives)
    recall_at_budget, precision_at_budget = _top_budget_metrics(
        y, p, max_alert_rate
    )

    precision_at: dict[str, dict[str, float | int]] = {}
    ranking = np.argsort(-p, kind="stable")
    for requested in (100, 1000):
        effective = min(requested, len(y))
        precision_at[str(requested)] = {
            "effective_k": effective,
            "precision": float(y[ranking[:effective]].mean()) if effective else 0.0,
        }

    false_positives = int(((predicted == 1) & (y == 0)).sum())
    return {
        "rows": int(len(y)),
        "hands": int(hand_count),
        "positives": positives,
        "negatives": negatives,
        "positive_rate": float(y.mean()),
        "roc_auc": float(roc_auc_score(y, p)) if len(np.unique(y)) == 2 else None,
        "pr_auc": float(average_precision_score(y, p)) if positives else None,
        "brier_score": float(brier_score_loss(y, p)),
        "threshold": float(threshold),
        "alerts": int(predicted.sum()),
        "alert_rate": float(predicted.mean()),
        "precision": float(precision_score(y, predicted, zero_division=0)),
        "recall": float(recall_score(y, predicted, zero_division=0)),
        "f1": float(f1_score(y, predicted, zero_division=0)),
        "recall_at_alert_budget": recall_at_budget,
        "precision_at_alert_budget": precision_at_budget,
        "max_alert_rate": float(max_alert_rate),
        "precision_at": precision_at,
        "false_positives_per_1000_hands": (
            float(false_positives * 1000 / hand_count) if hand_count else None
        ),
    }


def rules_only_score(frame: pd.DataFrame) -> np.ndarray:
    """A deterministic pair-rule baseline using only inference-safe features."""
    required = {
        "current_one_folded_other_won",
        "context_same_device",
        "context_same_network",
        "pair_outcome_asymmetry",
        "pair_a_fold_b_win_rate",
        "pair_b_fold_a_win_rate",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"missing rule baseline columns: {missing}")

    def numeric(column: str) -> np.ndarray:
        return (
            pd.to_numeric(frame[column], errors="coerce")
            .fillna(0.0)
            .clip(0.0, 1.0)
            .to_numpy(dtype=np.float64)
        )

    return np.clip(
        0.20 * numeric("current_one_folded_other_won")
        + 0.20 * numeric("context_same_device")
        + 0.20 * numeric("context_same_network")
        + 0.15 * numeric("pair_outcome_asymmetry")
        + 0.25
        * np.maximum(
            numeric("pair_a_fold_b_win_rate"),
            numeric("pair_b_fold_a_win_rate"),
        ),
        0.0,
        1.0,
    )


def class_counts(frame: pd.DataFrame) -> dict[str, int]:
    labels = frame["target"].astype(int)
    positives = int(labels.sum())
    return {
        "rows": int(len(labels)),
        "positives": positives,
        "negatives": int(len(labels) - positives),
        "hands": int(frame["hand_id"].nunique()),
    }
