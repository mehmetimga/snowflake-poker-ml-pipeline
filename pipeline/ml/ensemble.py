"""Leakage-safe out-of-fold ensemble training for pair-risk models.

The meta learner sees only predictions made by models that did not train on
the corresponding hand.  Validation is reserved for calibration and decision
policy selection; test is reserved for the public promotion comparison.  The
private challenge split is deliberately never loaded by this module.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score
from sklearn.model_selection import StratifiedGroupKFold
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    rules_only_score,
    select_alert_budget_threshold,
)


ENSEMBLE_VERSION = "pair-oof-stack-v1"
BASE_FEATURES = ("catboost_raw", "rules_raw", "player_raw")


@dataclass(frozen=True)
class EnsembleTrainingConfig:
    dataset_dir: Path = Path("data/datasets/pair-full-v2")
    champion_dir: Path = Path("models/pair-catboost-full-v2")
    output_dir: Path = Path("models/pair-ensemble-full-v2")
    folds: int = 5
    catboost_iterations: int | None = None
    depth: int = 3
    learning_rate: float = 0.03
    positive_class_weight: float = 100.0
    max_alert_rate: float = 0.02
    minimum_relative_pr_gain: float = 0.02
    bootstrap_samples: int = 500
    random_seed: int = 42
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.folds < 2:
            raise ValueError("folds must be at least two")
        if self.catboost_iterations is not None and self.catboost_iterations < 1:
            raise ValueError("catboost_iterations must be positive")
        if self.depth < 1 or not 0 < self.learning_rate <= 1:
            raise ValueError("invalid CatBoost configuration")
        if self.positive_class_weight <= 0:
            raise ValueError("positive_class_weight must be positive")
        if not 0 < self.max_alert_rate <= 1:
            raise ValueError("max_alert_rate must be in (0, 1]")
        if self.minimum_relative_pr_gain < 0 or self.bootstrap_samples < 1:
            raise ValueError("invalid promotion-gate configuration")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _verify_file(root: Path, artifacts: Mapping[str, str], relative: str) -> Path:
    expected = artifacts.get(relative)
    if expected is None:
        raise ValueError(f"manifest does not track {relative}")
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    if _sha256(path) != expected:
        raise ValueError(f"artifact hash mismatch: {relative}")
    return path


def _load_public_splits(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, pd.DataFrame]]:
    manifest_path, schema_path = root / "manifest.json", root / "schema.json"
    manifest = json.loads(manifest_path.read_text())
    schema = json.loads(schema_path.read_text())
    if manifest.get("challenge_labels_public") or schema.get("challenge_labels_public"):
        raise ValueError("challenge labels must remain private")
    if manifest.get("feature_definition_version") != "pair-features-v1":
        raise ValueError("ensemble only supports pair-features-v1")
    _verify_file(root, manifest["artifacts"], "schema.json")
    frames: dict[str, pd.DataFrame] = {}
    for split in ("train", "validation", "test"):
        relative = f"dgx/cold_start/{split}.parquet"
        path = _verify_file(root, manifest["artifacts"], relative)
        frame = pd.read_parquet(path)
        required = {
            "event_id", "hand_id", "pair_key", "target",
            *schema["numeric_feature_columns"],
            *schema["categorical_feature_columns"],
        }
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{split} is missing columns: {missing}")
        if frame["event_id"].astype(str).duplicated().any():
            raise ValueError(f"{split} contains duplicate event IDs")
        frames[split] = frame
    return manifest, schema, frames


def make_hand_grouped_folds(
    labels: Sequence[int], hand_ids: Sequence[str], folds: int, seed: int
) -> np.ndarray:
    """Return deterministic stratified fold IDs without splitting a hand."""
    y = np.asarray(labels, dtype=np.int8)
    groups = np.asarray(hand_ids, dtype=str)
    if len(y) == 0 or len(y) != len(groups):
        raise ValueError("labels and hand IDs must be non-empty and aligned")
    assignments = np.full(len(y), -1, dtype=np.int16)
    splitter = StratifiedGroupKFold(n_splits=folds, shuffle=True, random_state=seed)
    for fold, (fit_indices, holdout_indices) in enumerate(
        splitter.split(np.zeros(len(y), dtype=np.int8), y, groups)
    ):
        if set(groups[fit_indices]) & set(groups[holdout_indices]):
            raise RuntimeError("a hand crossed an OOF fold boundary")
        assignments[holdout_indices] = fold
    if np.any(assignments < 0):
        raise RuntimeError("OOF fold assignment is incomplete")
    return assignments


def _player_indices(feature_names: Sequence[str]) -> np.ndarray:
    indices = np.asarray(
        [
            index
            for index, name in enumerate(feature_names)
            if name.startswith(("context_", "user_a_", "user_b_"))
        ],
        dtype=np.int64,
    )
    if not len(indices):
        raise ValueError("no player-only features were selected")
    return indices


def _fit_player(matrix: np.ndarray, labels: np.ndarray, indices: np.ndarray, seed: int) -> Pipeline:
    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(
            class_weight="balanced", max_iter=500, random_state=seed, solver="lbfgs"
        ),
    )
    model.fit(matrix[:, indices], labels)
    return model


def _fit_catboost(matrix: np.ndarray, labels: np.ndarray, config: EnsembleTrainingConfig, seed: int, iterations: int) -> CatBoostClassifier:
    model = CatBoostClassifier(
        iterations=iterations,
        depth=config.depth,
        learning_rate=config.learning_rate,
        loss_function="Logloss",
        eval_metric="PRAUC",
        class_weights=[1.0, config.positive_class_weight],
        random_seed=seed,
        allow_writing_files=False,
        verbose=False,
        thread_count=-1,
    )
    model.fit(matrix, labels, verbose=False)
    return model


def _positive_probability(model: Any, matrix: np.ndarray) -> np.ndarray:
    return np.asarray(model.predict_proba(matrix), dtype=np.float64)[:, 1]


def _portable_logistic(model: Pipeline, feature_names: Sequence[str]) -> dict[str, Any]:
    scaler = model.named_steps["standardscaler"]
    logistic = model.named_steps["logisticregression"]
    return {
        "contract_version": 1,
        "input_features": list(feature_names),
        "standard_scaler": {
            "mean": [float(value) for value in scaler.mean_],
            "scale": [float(value) for value in scaler.scale_],
        },
        "logistic_regression": {
            "coefficient": [float(value) for value in logistic.coef_[0]],
            "intercept": float(logistic.intercept_[0]),
            "positive_class": 1,
        },
    }


def portable_logistic_predict(contract: Mapping[str, Any], matrix: np.ndarray) -> np.ndarray:
    """Score a language-neutral exported binary logistic model."""
    values = np.asarray(matrix, dtype=np.float64)
    scaler = contract["standard_scaler"]
    mean = np.asarray(scaler["mean"], dtype=np.float64)
    scale = np.asarray(scaler["scale"], dtype=np.float64)
    coefficient = np.asarray(
        contract["logistic_regression"]["coefficient"], dtype=np.float64
    )
    intercept = float(contract["logistic_regression"]["intercept"])
    if values.ndim != 2 or values.shape[1] != len(mean):
        raise ValueError("portable logistic input shape does not match contract")
    logits = ((values - mean) / scale) @ coefficient + intercept
    output = np.empty_like(logits)
    positive = logits >= 0
    output[positive] = 1.0 / (1.0 + np.exp(-logits[positive]))
    exp_values = np.exp(logits[~positive])
    output[~positive] = exp_values / (1.0 + exp_values)
    return output


def _load_champion_predictions(
    champion_dir: Path,
    frames: Mapping[str, pd.DataFrame],
    manifest: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, np.ndarray]]:
    artifact_manifest = json.loads((champion_dir / "artifact_manifest.json").read_text())
    for relative, expected in artifact_manifest["artifacts"].items():
        path = champion_dir / relative
        if not path.is_file() or _sha256(path) != expected:
            raise ValueError(f"champion artifact failed verification: {relative}")
    metrics = json.loads((champion_dir / "metrics.json").read_text())
    contract = json.loads((champion_dir / "scoring_contract.json").read_text())
    if metrics["run_id"] != contract["run_id"] or metrics["run_id"] != artifact_manifest["run_id"]:
        raise ValueError("champion run IDs do not agree")
    if metrics["dataset_id"] != manifest["dataset_id"]:
        raise ValueError("champion and ensemble datasets do not agree")
    predictions = pd.read_parquet(champion_dir / "predictions.parquet")
    output: dict[str, np.ndarray] = {}
    for split in ("validation", "test"):
        available = predictions[predictions["split"] == split][
            ["event_id", "raw_probability"]
        ].copy()
        if available["event_id"].astype(str).duplicated().any():
            raise ValueError(f"champion {split} predictions contain duplicate event IDs")
        available["event_id"] = available["event_id"].astype(str)
        aligned = frames[split][["event_id"]].copy()
        aligned["event_id"] = aligned["event_id"].astype(str)
        aligned = aligned.merge(available, on="event_id", how="left", validate="one_to_one")
        if aligned["raw_probability"].isna().any():
            raise ValueError(f"champion {split} predictions are incomplete")
        output[split] = aligned["raw_probability"].to_numpy(dtype=np.float64)
    return metrics, output


def paired_hand_bootstrap_pr_auc(
    frame: pd.DataFrame,
    candidate: Sequence[float],
    baseline: Sequence[float],
    *,
    samples: int,
    seed: int,
) -> dict[str, float | int]:
    labels = frame["target"].astype(int).to_numpy(dtype=np.int8)
    candidate_array = np.asarray(candidate, dtype=np.float64)
    baseline_array = np.asarray(baseline, dtype=np.float64)
    if len(labels) != len(candidate_array) or len(labels) != len(baseline_array):
        raise ValueError("paired bootstrap inputs are not aligned")
    codes, hands = pd.factorize(frame["hand_id"].astype(str), sort=True)
    generator = np.random.default_rng(seed)
    differences: list[float] = []
    for _ in range(samples):
        sampled = generator.integers(0, len(hands), size=len(hands))
        weights = np.bincount(sampled, minlength=len(hands))[codes]
        if int(np.dot(labels, weights)) == 0:
            continue
        differences.append(
            float(
                average_precision_score(labels, candidate_array, sample_weight=weights)
                - average_precision_score(labels, baseline_array, sample_weight=weights)
            )
        )
    if not differences:
        raise RuntimeError("paired bootstrap produced no samples containing positives")
    return {
        "unit": "hand",
        "requested_samples": samples,
        "effective_samples": len(differences),
        "pr_auc_difference_p2_5": float(np.percentile(differences, 2.5)),
        "pr_auc_difference_median": float(np.percentile(differences, 50)),
        "pr_auc_difference_p97_5": float(np.percentile(differences, 97.5)),
    }


def _report(frame: pd.DataFrame, probabilities: np.ndarray, threshold: float, max_alert_rate: float) -> dict[str, Any]:
    return binary_classification_report(
        frame["target"].astype(int).to_numpy(), probabilities,
        threshold=threshold, max_alert_rate=max_alert_rate,
        hand_count=int(frame["hand_id"].nunique()),
    )


def train_oof_ensemble(config: EnsembleTrainingConfig) -> dict[str, Any]:
    dataset_dir, champion_dir = config.dataset_dir.resolve(), config.champion_dir.resolve()
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}; pass --overwrite")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest, schema, frames = _load_public_splits(dataset_dir)
    champion_metrics, champion_raw = _load_champion_predictions(
        champion_dir, frames, manifest
    )
    iterations = config.catboost_iterations or int(champion_metrics["best_iteration"]) + 1
    train = frames["train"]
    labels = {
        split: frame["target"].astype(int).to_numpy(dtype=np.int8)
        for split, frame in frames.items()
    }
    fold_ids = make_hand_grouped_folds(
        labels["train"], train["hand_id"].astype(str), config.folds, config.random_seed
    )
    oof = np.full((len(train), len(BASE_FEATURES)), np.nan, dtype=np.float64)
    fold_manifest: list[dict[str, Any]] = []
    numeric, categorical = schema["numeric_feature_columns"], schema["categorical_feature_columns"]

    for fold in range(config.folds):
        holdout_indices = np.flatnonzero(fold_ids == fold)
        fit_indices = np.flatnonzero(fold_ids != fold)
        fit_frame, holdout_frame = train.iloc[fit_indices], train.iloc[holdout_indices]
        preprocessor = PairPreprocessor.fit(fit_frame, numeric, categorical)
        fit_matrix = preprocessor.transform(fit_frame)
        holdout_matrix = preprocessor.transform(holdout_frame)
        fit_labels = labels["train"][fit_indices]
        catboost = _fit_catboost(
            fit_matrix, fit_labels, config, config.random_seed + fold, iterations
        )
        player_indices = _player_indices(preprocessor.output_columns)
        player = _fit_player(
            fit_matrix, fit_labels, player_indices, config.random_seed + fold
        )
        oof[holdout_indices, 0] = _positive_probability(catboost, holdout_matrix)
        oof[holdout_indices, 1] = rules_only_score(holdout_frame)
        oof[holdout_indices, 2] = _positive_probability(
            player, holdout_matrix[:, player_indices]
        )
        fit_hands = sorted(fit_frame["hand_id"].astype(str).unique())
        holdout_hands = sorted(holdout_frame["hand_id"].astype(str).unique())
        fold_manifest.append(
            {
                "fold": fold,
                "fit_rows": len(fit_indices),
                "holdout_rows": len(holdout_indices),
                "fit_hands": len(fit_hands),
                "holdout_hands": len(holdout_hands),
                "fit_positives": int(fit_labels.sum()),
                "holdout_positives": int(labels["train"][holdout_indices].sum()),
                "fit_hand_ids_sha256": _json_sha256(fit_hands),
                "holdout_hand_ids_sha256": _json_sha256(holdout_hands),
                "hand_overlap": 0,
            }
        )
        print(
            f"[pair-ensemble][fold={fold}] fit_rows={len(fit_indices)} "
            f"holdout_rows={len(holdout_indices)} positives={fold_manifest[-1]['holdout_positives']}"
        )
    if not np.isfinite(oof).all():
        raise RuntimeError("OOF base-prediction matrix is incomplete or non-finite")

    meta_model = make_pipeline(
        StandardScaler(),
        LogisticRegression(
            class_weight="balanced", max_iter=500, random_state=config.random_seed,
            solver="lbfgs",
        ),
    )
    meta_model.fit(oof, labels["train"])
    oof_raw = _positive_probability(meta_model, oof)

    full_preprocessor = PairPreprocessor.fit(train, numeric, categorical)
    train_matrix = full_preprocessor.transform(train)
    player_indices = _player_indices(full_preprocessor.output_columns)
    full_player = _fit_player(
        train_matrix, labels["train"], player_indices, config.random_seed
    )
    base_predictions: dict[str, np.ndarray] = {}
    ensemble_raw: dict[str, np.ndarray] = {}
    for split in ("validation", "test"):
        matrix = full_preprocessor.transform(frames[split])
        bases = np.column_stack(
            [
                champion_raw[split],
                rules_only_score(frames[split]),
                _positive_probability(full_player, matrix[:, player_indices]),
            ]
        )
        base_predictions[split] = bases
        ensemble_raw[split] = _positive_probability(meta_model, bases)

    calibrator = PlattCalibrator.fit(labels["validation"], ensemble_raw["validation"])
    calibrated = {
        split: calibrator.predict(probabilities)
        for split, probabilities in ensemble_raw.items()
    }
    threshold = select_alert_budget_threshold(
        labels["validation"], calibrated["validation"], config.max_alert_rate
    )
    reports = {
        split: _report(frames[split], calibrated[split], threshold, config.max_alert_rate)
        for split in ("validation", "test")
    }
    champion_test = champion_metrics["reports"]["catboost"]["test"]
    bootstrap = paired_hand_bootstrap_pr_auc(
        frames["test"], calibrated["test"], champion_raw["test"],
        samples=config.bootstrap_samples, seed=config.random_seed + 1000,
    )
    candidate_pr, champion_pr = float(reports["test"]["pr_auc"]), float(champion_test["pr_auc"])
    relative_gain = (candidate_pr - champion_pr) / max(champion_pr, 1e-12)
    reasons: list[str] = []
    if relative_gain < config.minimum_relative_pr_gain:
        reasons.append(
            f"test PR-AUC relative gain {relative_gain:.6f} is below {config.minimum_relative_pr_gain:.6f}"
        )
    if float(bootstrap["pr_auc_difference_p2_5"]) <= 0:
        reasons.append("paired hand-bootstrap PR-AUC lower bound is not positive")
    if float(reports["test"]["recall_at_alert_budget"]) < float(champion_test["recall_at_alert_budget"]):
        reasons.append("test recall at alert budget is below the champion")
    promotion_candidate = not reasons
    quality_gate = {
        "promotion_candidate": promotion_candidate,
        "promotion_eligible": False,
        "reasons": reasons + (["sealed challenge and manual approval are still required"] if promotion_candidate else []),
        "minimum_relative_pr_gain": config.minimum_relative_pr_gain,
        "test_pr_auc_relative_gain": relative_gain,
        "private_challenge_loaded": False,
        "manual_approval_required": True,
    }

    meta_contract = _portable_logistic(meta_model, BASE_FEATURES)
    player_contract = _portable_logistic(
        full_player, [full_preprocessor.output_columns[index] for index in player_indices]
    )
    _write_json(output_dir / "stacker.json", meta_contract)
    _write_json(output_dir / "player_baseline.json", player_contract)
    _write_json(output_dir / "calibration.json", calibrator.to_dict())
    _write_json(
        output_dir / "decision_policy.json",
        {
            "policy_version": 1,
            "probability": "platt_calibrated_oof_stack_probability",
            "threshold": threshold,
            "validation_max_alert_rate": config.max_alert_rate,
            "promotion_requires_private_challenge_and_manual_approval": True,
        },
    )
    _write_json(
        output_dir / "fold_manifest.json",
        {
            "splitter": "StratifiedGroupKFold",
            "group": "hand_id",
            "folds": config.folds,
            "random_seed": config.random_seed,
            "rows_assigned_once": int((fold_ids >= 0).sum()),
            "private_challenge_loaded": False,
            "fold_manifest": fold_manifest,
        },
    )
    pd.DataFrame(
        {
            "split": "train_oof", "fold": fold_ids,
            "event_id": train["event_id"].astype(str),
            "hand_id": train["hand_id"].astype(str),
            "pair_key": train["pair_key"].astype(str),
            "target": labels["train"],
            **{name: oof[:, index] for index, name in enumerate(BASE_FEATURES)},
            "ensemble_raw_probability": oof_raw,
        }
    ).to_parquet(output_dir / "oof_predictions.parquet", index=False)
    prediction_rows: list[pd.DataFrame] = []
    for split in ("validation", "test"):
        prediction_rows.append(
            pd.DataFrame(
                {
                    "split": split,
                    "event_id": frames[split]["event_id"].astype(str),
                    "hand_id": frames[split]["hand_id"].astype(str),
                    "pair_key": frames[split]["pair_key"].astype(str),
                    **{
                        name: base_predictions[split][:, index]
                        for index, name in enumerate(BASE_FEATURES)
                    },
                    "raw_probability": ensemble_raw[split],
                    "calibrated_probability": calibrated[split],
                    "alert": calibrated[split] >= threshold,
                }
            )
        )
    pd.concat(prediction_rows, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )

    run_id = f"pair_ensemble_{uuid.uuid4().hex[:12]}"
    metrics = {
        "run_id": run_id,
        "model_name": ENSEMBLE_VERSION,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "benchmark": "cold_start",
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest_sha256": _sha256(dataset_dir / "manifest.json"),
        "feature_definition_version": manifest["feature_definition_version"],
        "champion": {
            "model_name": champion_metrics["model_name"],
            "run_id": champion_metrics["run_id"],
            "test_report": champion_test,
        },
        "base_models": list(BASE_FEATURES),
        "excluded_models": {
            "tabular_neural": "failed Phase 9 public promotion gates",
            "history_transformer": "failed Phase 10 public promotion gate",
            "graphsage": "failed Phase 11 public promotion gates",
        },
        "counts": {split: class_counts(frame) for split, frame in frames.items()},
        "training_config": {
            "folds": config.folds,
            "catboost_iterations": iterations,
            "depth": config.depth,
            "learning_rate": config.learning_rate,
            "positive_class_weight": config.positive_class_weight,
            "max_alert_rate": config.max_alert_rate,
            "random_seed": config.random_seed,
        },
        "calibration": calibrator.to_dict(),
        "threshold": threshold,
        "reports": reports,
        "oof_diagnostic": {
            "pr_auc": float(average_precision_score(labels["train"], oof_raw)),
            "rows": len(train),
        },
        "paired_bootstrap": bootstrap,
        "quality_gate": quality_gate,
    }
    _write_json(output_dir / "metrics.json", metrics)
    artifact_paths = [
        path for path in output_dir.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    ]
    _write_json(
        output_dir / "artifact_manifest.json",
        {
            "run_id": run_id,
            "model_name": ENSEMBLE_VERSION,
            "artifacts": {
                str(path.relative_to(output_dir)): _sha256(path)
                for path in sorted(artifact_paths)
            },
        },
    )
    return metrics
