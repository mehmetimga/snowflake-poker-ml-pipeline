"""Leakage-safe Phase 9 GPU tabular challengers for pair-risk scoring."""

from __future__ import annotations

import copy
import hashlib
import json
import math
import random
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score
from torch import nn
from torch.utils.data import DataLoader, Dataset

from pipeline.ml.pair_model import (
    MISSING_CATEGORY,
    UNKNOWN_CATEGORY,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)

from .tabular_models import MODEL_NAMES, build_tabular_model


DeviceName = Literal["auto", "cpu", "cuda"]
SUPPORTED_BENCHMARKS = ("cold_start", "temporal", "new_relationship")


@dataclass(frozen=True)
class PairChallengerConfig:
    dataset_dir: Path = Path("data/datasets/pair-full-v2")
    baseline_dir: Path = Path("models/pair-catboost-full-v2")
    output_dir: Path = Path("models/pair-challengers-full-v2")
    benchmark: str = "cold_start"
    models: tuple[str, ...] = MODEL_NAMES
    epochs: int = 20
    batch_size: int = 1024
    patience: int = 4
    learning_rate: float = 1e-3
    weight_decay: float = 1e-4
    positive_class_weight: float = 100.0
    max_alert_rate: float = 0.02
    minimum_relative_pr_gain: float = 0.02
    bootstrap_samples: int = 200
    random_seed: int = 42
    num_workers: int = 4
    device_name: DeviceName = "auto"
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.benchmark not in SUPPORTED_BENCHMARKS:
            raise ValueError(f"unsupported benchmark: {self.benchmark}")
        if not self.models or any(name not in MODEL_NAMES for name in self.models):
            raise ValueError(f"models must be selected from {MODEL_NAMES}")
        if len(set(self.models)) != len(self.models):
            raise ValueError("challenger model names must be unique")
        if self.epochs < 1 or self.batch_size < 1 or self.patience < 1:
            raise ValueError("epochs, batch_size, and patience must be positive")
        if self.learning_rate <= 0 or self.weight_decay < 0:
            raise ValueError("learning rate must be positive and weight decay non-negative")
        if self.positive_class_weight <= 0:
            raise ValueError("positive class weight must be positive")
        if not 0 < self.max_alert_rate <= 1:
            raise ValueError("max alert rate must be in (0, 1]")
        if self.minimum_relative_pr_gain < 0 or self.bootstrap_samples < 1:
            raise ValueError("promotion gain must be non-negative and bootstrap positive")
        if self.num_workers < 0:
            raise ValueError("num_workers cannot be negative")


@dataclass(frozen=True)
class NeuralPairPreprocessor:
    numeric_columns: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    numeric_fill_values: Mapping[str, float]
    numeric_means: Mapping[str, float]
    numeric_scales: Mapping[str, float]
    categorical_values: Mapping[str, tuple[str, ...]]

    @classmethod
    def fit(
        cls,
        frame: pd.DataFrame,
        numeric_columns: Sequence[str],
        categorical_columns: Sequence[str],
    ) -> "NeuralPairPreprocessor":
        required = set(numeric_columns) | set(categorical_columns)
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"missing neural model columns: {missing}")
        fill_values: dict[str, float] = {}
        means: dict[str, float] = {}
        scales: dict[str, float] = {}
        for column in numeric_columns:
            values = pd.to_numeric(frame[column], errors="coerce").replace(
                [np.inf, -np.inf], np.nan
            )
            median = values.median(skipna=True)
            fill = 0.0 if pd.isna(median) else float(median)
            filled = values.fillna(fill).to_numpy(dtype=np.float64)
            mean = float(filled.mean())
            scale = float(filled.std())
            fill_values[column] = fill
            means[column] = mean
            scales[column] = scale if math.isfinite(scale) and scale > 1e-8 else 1.0
        categories: dict[str, tuple[str, ...]] = {}
        for column in categorical_columns:
            observed = sorted(
                value
                for value in set(frame[column].fillna(MISSING_CATEGORY).astype(str))
                if value != UNKNOWN_CATEGORY
            )
            categories[column] = tuple(observed + [UNKNOWN_CATEGORY])
        return cls(
            numeric_columns=tuple(numeric_columns),
            categorical_columns=tuple(categorical_columns),
            numeric_fill_values=fill_values,
            numeric_means=means,
            numeric_scales=scales,
            categorical_values=categories,
        )

    @property
    def categorical_cardinalities(self) -> tuple[int, ...]:
        return tuple(len(self.categorical_values[column]) for column in self.categorical_columns)

    def transform(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        numeric = np.empty((len(frame), len(self.numeric_columns)), dtype=np.float32)
        for index, column in enumerate(self.numeric_columns):
            values = (
                pd.to_numeric(frame[column], errors="coerce")
                .replace([np.inf, -np.inf], np.nan)
                .fillna(float(self.numeric_fill_values[column]))
                .to_numpy(dtype=np.float64)
            )
            numeric[:, index] = (
                (values - float(self.numeric_means[column]))
                / float(self.numeric_scales[column])
            ).astype(np.float32)
        categorical = np.empty(
            (len(frame), len(self.categorical_columns)), dtype=np.int64
        )
        for index, column in enumerate(self.categorical_columns):
            vocabulary = self.categorical_values[column]
            lookup = {value: value_index for value_index, value in enumerate(vocabulary)}
            unknown = lookup[UNKNOWN_CATEGORY]
            values = frame[column].fillna(MISSING_CATEGORY).astype(str)
            categorical[:, index] = np.fromiter(
                (lookup.get(value, unknown) for value in values),
                dtype=np.int64,
                count=len(values),
            )
        if not np.isfinite(numeric).all():
            raise ValueError("neural numeric matrix contains non-finite values")
        return numeric, categorical

    def to_dict(self) -> dict[str, Any]:
        return {
            "contract_version": 1,
            "fit_split": "train",
            "numeric_columns": list(self.numeric_columns),
            "categorical_columns": list(self.categorical_columns),
            "numeric_fill_values": dict(self.numeric_fill_values),
            "numeric_means": dict(self.numeric_means),
            "numeric_scales": dict(self.numeric_scales),
            "categorical_values": {
                column: list(values) for column, values in self.categorical_values.items()
            },
        }


class PairTensorDataset(Dataset):
    def __init__(
        self, numeric: np.ndarray, categorical: np.ndarray, labels: np.ndarray
    ) -> None:
        if len(numeric) != len(categorical) or len(numeric) != len(labels):
            raise ValueError("pair tensors must have aligned row counts")
        self.numeric = torch.from_numpy(numeric)
        self.categorical = torch.from_numpy(categorical)
        self.labels = torch.from_numpy(labels.astype(np.float32, copy=False))

    def __len__(self) -> int:
        return len(self.labels)

    def __getitem__(self, index: int) -> tuple[torch.Tensor, torch.Tensor, torch.Tensor]:
        return self.numeric[index], self.categorical[index], self.labels[index]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _seed_everything(seed: int) -> None:
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)
    if hasattr(torch.backends, "cudnn"):
        torch.backends.cudnn.benchmark = False


def _resolve_device(requested: DeviceName) -> torch.device:
    if requested == "cuda" and not torch.cuda.is_available():
        raise RuntimeError("CUDA was requested but is unavailable")
    if requested == "auto":
        requested = "cuda" if torch.cuda.is_available() else "cpu"
    return torch.device(requested)


def _load_inputs(
    root: Path, benchmark: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, pd.DataFrame]]:
    manifest_path, schema_path = root / "manifest.json", root / "schema.json"
    if not manifest_path.is_file() or not schema_path.is_file():
        raise FileNotFoundError("pair challenger requires manifest.json and schema.json")
    manifest = json.loads(manifest_path.read_text())
    schema = json.loads(schema_path.read_text())
    if manifest["feature_definition_version"] != "pair-features-v1":
        raise ValueError("challengers only accept pair-features-v1")
    if manifest["challenge_labels_public"] or schema["challenge_labels_public"]:
        raise ValueError("private challenge labels cannot enter DGX challenger training")
    if _sha256(schema_path) != manifest["artifacts"]["schema.json"]:
        raise ValueError("pair schema hash mismatch")
    frames: dict[str, pd.DataFrame] = {}
    for split in ("train", "validation", "test"):
        relative = f"dgx/{benchmark}/{split}.parquet"
        path = root / relative
        if _sha256(path) != manifest["artifacts"][relative]:
            raise ValueError(f"pair dataset hash mismatch: {relative}")
        frame = pd.read_parquet(path)
        if set(frame["benchmark_split"].astype(str)) != {split}:
            raise ValueError(f"{split} parquet contains another benchmark split")
        frames[split] = frame
    required = (
        set(schema["numeric_feature_columns"])
        | set(schema["categorical_feature_columns"])
        | {"event_id", "hand_id", "pair_key", "target"}
    )
    for split, frame in frames.items():
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{split} is missing columns: {missing}")
        if not set(frame["target"].astype(int).unique()).issubset({0, 1}):
            raise ValueError(f"{split} target is not binary")
        if frame["event_id"].astype(str).duplicated().any():
            raise ValueError(f"{split} contains duplicate event IDs")
    return manifest, schema, frames


def _loader(
    arrays: tuple[np.ndarray, np.ndarray],
    labels: np.ndarray,
    *,
    batch_size: int,
    shuffle: bool,
    num_workers: int,
    pin_memory: bool,
    seed: int,
) -> DataLoader:
    return DataLoader(
        PairTensorDataset(arrays[0], arrays[1], labels),
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=pin_memory,
        persistent_workers=num_workers > 0,
        generator=torch.Generator().manual_seed(seed) if shuffle else None,
    )


def _train_epoch(
    model: nn.Module,
    loader: DataLoader,
    optimizer: torch.optim.Optimizer,
    loss_function: nn.Module,
    device: torch.device,
) -> float:
    model.train()
    total_loss, rows = 0.0, 0
    for numeric, categorical, labels in loader:
        numeric = numeric.to(device, non_blocking=True)
        categorical = categorical.to(device, non_blocking=True)
        labels = labels.to(device, non_blocking=True)
        optimizer.zero_grad(set_to_none=True)
        with torch.autocast(
            device_type=device.type,
            dtype=torch.bfloat16,
            enabled=device.type == "cuda",
        ):
            logits = model(numeric, categorical)
            loss = loss_function(logits, labels)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
        optimizer.step()
        total_loss += float(loss.detach()) * len(labels)
        rows += len(labels)
    return total_loss / max(rows, 1)


def _predict(
    model: nn.Module,
    loader: DataLoader,
    loss_function: nn.Module,
    device: torch.device,
) -> tuple[float, np.ndarray]:
    model.eval()
    total_loss, rows = 0.0, 0
    probabilities: list[np.ndarray] = []
    with torch.no_grad():
        for numeric, categorical, labels in loader:
            numeric = numeric.to(device, non_blocking=True)
            categorical = categorical.to(device, non_blocking=True)
            labels = labels.to(device, non_blocking=True)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                logits = model(numeric, categorical)
                loss = loss_function(logits, labels)
            total_loss += float(loss) * len(labels)
            rows += len(labels)
            probabilities.append(torch.sigmoid(logits.float()).cpu().numpy())
    output = np.concatenate(probabilities).astype(np.float64, copy=False)
    return total_loss / max(rows, 1), output


def _latency(
    model: nn.Module,
    arrays: tuple[np.ndarray, np.ndarray],
    device: torch.device,
) -> dict[str, Any]:
    rows = min(15, len(arrays[0]))
    numeric = torch.from_numpy(arrays[0][:rows]).to(device)
    categorical = torch.from_numpy(arrays[1][:rows]).to(device)
    model.eval()
    with torch.no_grad():
        for _ in range(10):
            model(numeric, categorical)
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        timings: list[float] = []
        for _ in range(100):
            started = time.perf_counter()
            model(numeric, categorical)
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            timings.append((time.perf_counter() - started) * 1000)
    return {
        "device": str(device),
        "batch_rows": rows,
        "runs": len(timings),
        "p50_ms": float(np.percentile(timings, 50)),
        "p95_ms": float(np.percentile(timings, 95)),
    }


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
    if len(hands) < 2:
        raise ValueError("paired hand bootstrap needs at least two hands")
    generator = np.random.default_rng(seed)
    differences: list[float] = []
    for _ in range(samples):
        sampled = generator.integers(0, len(hands), size=len(hands))
        hand_weights = np.bincount(sampled, minlength=len(hands))
        row_weights = hand_weights[codes]
        if int(np.dot(labels, row_weights)) == 0:
            continue
        candidate_pr = average_precision_score(
            labels, candidate_array, sample_weight=row_weights
        )
        baseline_pr = average_precision_score(
            labels, baseline_array, sample_weight=row_weights
        )
        differences.append(float(candidate_pr - baseline_pr))
    if not differences:
        raise RuntimeError("paired bootstrap produced no samples with positives")
    return {
        "unit": "hand",
        "requested_samples": samples,
        "effective_samples": len(differences),
        "pr_auc_difference_p2_5": float(np.percentile(differences, 2.5)),
        "pr_auc_difference_median": float(np.percentile(differences, 50)),
        "pr_auc_difference_p97_5": float(np.percentile(differences, 97.5)),
    }


def challenger_gate(
    candidate_report: Mapping[str, Any],
    baseline_report: Mapping[str, Any],
    bootstrap: Mapping[str, Any],
    *,
    minimum_relative_pr_gain: float,
    max_alert_rate: float,
) -> dict[str, Any]:
    reasons: list[str] = []
    candidate_pr = float(candidate_report["pr_auc"])
    baseline_pr = float(baseline_report["pr_auc"])
    relative_gain = (candidate_pr - baseline_pr) / baseline_pr
    if relative_gain < minimum_relative_pr_gain:
        reasons.append(
            f"test PR-AUC relative gain {relative_gain:.4f} is below "
            f"{minimum_relative_pr_gain:.4f}"
        )
    if float(bootstrap["pr_auc_difference_p2_5"]) <= 0:
        reasons.append("paired hand-bootstrap PR-AUC lower bound is not positive")
    candidate_budget_recall = float(candidate_report["recall_at_alert_budget"])
    baseline_budget_recall = float(baseline_report["recall_at_alert_budget"])
    if candidate_budget_recall < baseline_budget_recall:
        reasons.append("test recall at alert budget is below CatBoost")
    if float(candidate_report["f1"]) < float(baseline_report["f1"]):
        reasons.append("validation-threshold test F1 is below CatBoost")
    if float(candidate_report["alert_rate"]) > max_alert_rate:
        reasons.append("test alert rate exceeds the operational budget")
    candidate_passed = not reasons
    return {
        "promotion_candidate": candidate_passed,
        "promotion_eligible": False,
        "requires_private_challenge_evaluation": candidate_passed,
        "minimum_relative_pr_gain": minimum_relative_pr_gain,
        "test_pr_auc_relative_gain": relative_gain,
        "reasons": reasons,
    }


def _baseline(
    baseline_dir: Path, frames: dict[str, pd.DataFrame], dataset_manifest_sha: str
) -> tuple[dict[str, Any], np.ndarray]:
    metrics_path = baseline_dir / "metrics.json"
    predictions_path = baseline_dir / "predictions.parquet"
    if not metrics_path.is_file() or not predictions_path.is_file():
        raise FileNotFoundError("CatBoost metrics.json and predictions.parquet are required")
    metrics = json.loads(metrics_path.read_text())
    if metrics["dataset_manifest_sha256"] != dataset_manifest_sha:
        raise ValueError("CatBoost and challenger dataset manifests differ")
    if metrics["benchmark"] != "cold_start":
        raise ValueError("the promoted baseline is not the cold-start benchmark")
    baseline = pd.read_parquet(predictions_path)
    baseline = baseline[baseline["split"] == "test"][
        ["event_id", "calibrated_probability"]
    ]
    candidate_ids = frames["test"][["event_id"]].copy()
    candidate_ids["event_id"] = candidate_ids["event_id"].astype(str)
    baseline["event_id"] = baseline["event_id"].astype(str)
    aligned = candidate_ids.merge(
        baseline, on="event_id", how="left", validate="one_to_one"
    )
    if aligned["calibrated_probability"].isna().any():
        raise ValueError("CatBoost predictions do not cover the challenger test split")
    return metrics, aligned["calibrated_probability"].to_numpy(dtype=np.float64)


def train_pair_challengers(config: PairChallengerConfig) -> dict[str, Any]:
    dataset_dir = config.dataset_dir.resolve()
    baseline_dir = config.baseline_dir.resolve()
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(
                f"output directory is not empty: {output_dir}; pass --overwrite"
            )
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest, schema, frames = _load_inputs(dataset_dir, config.benchmark)
    manifest_sha = _sha256(dataset_dir / "manifest.json")
    if config.benchmark != "cold_start":
        raise ValueError(
            "initial promoted CatBoost comparison is cold_start; run other benchmarks "
            "with a matching baseline artifact"
        )
    baseline_metrics, baseline_test_probabilities = _baseline(
        baseline_dir, frames, manifest_sha
    )
    counts = {split: class_counts(frame) for split, frame in frames.items()}
    for split in ("train", "validation", "test"):
        if counts[split]["positives"] == 0 or counts[split]["negatives"] == 0:
            raise RuntimeError(f"{split} needs both target classes")

    preprocessor = NeuralPairPreprocessor.fit(
        frames["train"],
        schema["numeric_feature_columns"],
        schema["categorical_feature_columns"],
    )
    arrays = {
        split: preprocessor.transform(frame) for split, frame in frames.items()
    }
    labels = {
        split: frame["target"].astype(int).to_numpy(dtype=np.int8)
        for split, frame in frames.items()
    }
    _write_json(output_dir / "preprocessing.json", preprocessor.to_dict())

    device = _resolve_device(config.device_name)
    pin_memory = device.type == "cuda"
    evaluation_loaders = {
        split: _loader(
            arrays[split], labels[split], batch_size=config.batch_size,
            shuffle=False, num_workers=config.num_workers,
            pin_memory=pin_memory, seed=config.random_seed,
        )
        for split in ("validation", "test")
    }
    results: dict[str, Any] = {}
    all_predictions: list[pd.DataFrame] = []
    baseline_report = baseline_metrics["reports"]["catboost"]["test"]
    for model_index, model_name in enumerate(config.models):
        model_seed = config.random_seed + model_index
        _seed_everything(model_seed)
        model = build_tabular_model(
            model_name,
            len(preprocessor.numeric_columns),
            preprocessor.categorical_cardinalities,
        ).to(device)
        train_loader = _loader(
            arrays["train"], labels["train"], batch_size=config.batch_size,
            shuffle=True, num_workers=config.num_workers, pin_memory=pin_memory,
            seed=model_seed,
        )
        optimizer = torch.optim.AdamW(
            model.parameters(),
            lr=config.learning_rate,
            weight_decay=config.weight_decay,
        )
        loss_function = nn.BCEWithLogitsLoss(
            pos_weight=torch.tensor(config.positive_class_weight, device=device)
        )
        best_epoch = 0
        best_validation_pr = -math.inf
        best_validation_loss = math.inf
        best_state: dict[str, torch.Tensor] | None = None
        history: list[dict[str, Any]] = []
        training_started = time.perf_counter()
        for epoch in range(1, config.epochs + 1):
            epoch_started = time.perf_counter()
            train_loss = _train_epoch(
                model, train_loader, optimizer, loss_function, device
            )
            validation_loss, validation_raw = _predict(
                model, evaluation_loaders["validation"], loss_function, device
            )
            validation_pr = float(
                average_precision_score(labels["validation"], validation_raw)
            )
            history.append(
                {
                    "epoch": epoch,
                    "train_loss": train_loss,
                    "validation_loss": validation_loss,
                    "validation_pr_auc": validation_pr,
                    "seconds": time.perf_counter() - epoch_started,
                }
            )
            print(
                f"[pair-challenger][{model_name}] epoch={epoch} "
                f"train_loss={train_loss:.6f} validation_loss={validation_loss:.6f} "
                f"validation_pr_auc={validation_pr:.6f}",
                flush=True,
            )
            improved = validation_pr > best_validation_pr + 1e-7 or (
                abs(validation_pr - best_validation_pr) <= 1e-7
                and validation_loss < best_validation_loss
            )
            if improved:
                best_epoch = epoch
                best_validation_pr = validation_pr
                best_validation_loss = validation_loss
                best_state = {
                    key: value.detach().cpu().clone()
                    for key, value in model.state_dict().items()
                }
            elif epoch - best_epoch >= config.patience:
                print(
                    f"[pair-challenger][{model_name}] early_stop "
                    f"best_epoch={best_epoch}",
                    flush=True,
                )
                break
        if best_state is None:
            raise RuntimeError(f"{model_name} produced no checkpoint")
        model.load_state_dict(best_state)
        raw_probabilities = {
            split: _predict(model, loader, loss_function, device)[1]
            for split, loader in evaluation_loaders.items()
        }
        calibrator = PlattCalibrator.fit(
            labels["validation"], raw_probabilities["validation"]
        )
        calibrated = {
            split: calibrator.predict(probabilities)
            for split, probabilities in raw_probabilities.items()
        }
        threshold = select_alert_budget_threshold(
            labels["validation"], calibrated["validation"], config.max_alert_rate
        )
        reports = {
            split: binary_classification_report(
                labels[split], probabilities, threshold=threshold,
                max_alert_rate=config.max_alert_rate,
                hand_count=counts[split]["hands"],
            )
            for split, probabilities in calibrated.items()
        }
        bootstrap = paired_hand_bootstrap_pr_auc(
            frames["test"], calibrated["test"], baseline_test_probabilities,
            samples=config.bootstrap_samples, seed=model_seed,
        )
        gate = challenger_gate(
            reports["test"], baseline_report, bootstrap,
            minimum_relative_pr_gain=config.minimum_relative_pr_gain,
            max_alert_rate=config.max_alert_rate,
        )
        latency = _latency(model, arrays["validation"], device)
        model_dir = output_dir / model_name
        model_dir.mkdir(parents=True, exist_ok=True)
        model_path = model_dir / "model.pt"
        torch.save(
            {
                "model_name": model_name,
                "state_dict": best_state,
                "numeric_dim": len(preprocessor.numeric_columns),
                "categorical_cardinalities": preprocessor.categorical_cardinalities,
                "feature_definition_version": manifest["feature_definition_version"],
            },
            model_path,
        )
        model_metrics = {
            "model_name": model_name,
            "best_epoch": best_epoch,
            "epochs_ran": len(history),
            "best_validation_pr_auc": best_validation_pr,
            "best_validation_loss": best_validation_loss,
            "training_seconds": time.perf_counter() - training_started,
            "parameter_count": sum(parameter.numel() for parameter in model.parameters()),
            "calibration": calibrator.to_dict(),
            "threshold": threshold,
            "reports": reports,
            "latency": latency,
            "paired_bootstrap": bootstrap,
            "quality_gate": gate,
            "history": history,
        }
        _write_json(model_dir / "metrics.json", model_metrics)
        results[model_name] = model_metrics
        for split in ("validation", "test"):
            all_predictions.append(
                pd.DataFrame(
                    {
                        "model_name": model_name,
                        "split": split,
                        "event_id": frames[split]["event_id"].astype(str),
                        "hand_id": frames[split]["hand_id"].astype(str),
                        "pair_key": frames[split]["pair_key"].astype(str),
                        "target": labels[split],
                        "raw_probability": raw_probabilities[split],
                        "calibrated_probability": calibrated[split],
                        "alert": calibrated[split] >= threshold,
                    }
                )
            )
        print(
            f"[pair-challenger][{model_name}] test_pr_auc="
            f"{reports['test']['pr_auc']:.6f} test_f1={reports['test']['f1']:.6f} "
            f"recall_at_budget={reports['test']['recall_at_alert_budget']:.6f} "
            f"promotion_candidate={gate['promotion_candidate']}",
            flush=True,
        )

    pd.concat(all_predictions, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )
    run_id = f"pair_challengers_{uuid.uuid4().hex[:12]}"
    summary = {
        "run_id": run_id,
        "phase": 9,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "dataset_id": manifest["dataset_id"],
        "dataset_manifest_sha256": manifest_sha,
        "feature_definition_version": manifest["feature_definition_version"],
        "benchmark": config.benchmark,
        "challenge_labels_used": False,
        "device": str(device),
        "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
        "counts": counts,
        "training_config": {
            **asdict(config),
            "dataset_dir": str(config.dataset_dir),
            "baseline_dir": str(config.baseline_dir),
            "output_dir": str(config.output_dir),
        },
        "catboost_baseline": {
            "run_id": baseline_metrics["run_id"],
            "test": baseline_report,
        },
        "models": results,
    }
    _write_json(output_dir / "summary.json", summary)
    artifact_paths = [
        path
        for path in output_dir.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    ]
    _write_json(
        output_dir / "artifact_manifest.json",
        {
            "run_id": run_id,
            "artifacts": {
                str(path.relative_to(output_dir)): _sha256(path)
                for path in sorted(artifact_paths)
            },
        },
    )
    return summary
