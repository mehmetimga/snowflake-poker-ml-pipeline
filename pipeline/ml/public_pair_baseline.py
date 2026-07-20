"""Public-split-only CatBoost baselines for non-production benchmarks."""

from __future__ import annotations

import json
import shutil
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool

from pipeline.dl.history_dataset import sha256_file
from pipeline.dl.pair_challengers import _load_inputs
from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)


@dataclass(frozen=True)
class PublicPairBaselineConfig:
    dataset_dir: Path = Path("data/datasets/pair-full-v2")
    output_dir: Path = Path("models/pair-catboost-new-relationship-v2")
    benchmark: str = "new_relationship"
    iterations: int = 500
    depth: int = 3
    learning_rate: float = 0.03
    early_stopping_rounds: int = 80
    positive_class_weight: float = 100.0
    max_alert_rate: float = 0.02
    random_seed: int = 42
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.benchmark not in ("cold_start", "temporal", "new_relationship"):
            raise ValueError("unsupported public baseline benchmark")
        if self.iterations < 1 or self.depth < 1 or self.early_stopping_rounds < 1:
            raise ValueError("baseline training counts must be positive")
        if not 0 < self.learning_rate <= 1 or self.positive_class_weight <= 0:
            raise ValueError("invalid public baseline training setting")
        if not 0 < self.max_alert_rate <= 1:
            raise ValueError("invalid public baseline alert budget")


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def train_public_pair_baseline(config: PublicPairBaselineConfig) -> dict[str, Any]:
    dataset_dir = config.dataset_dir.resolve()
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest, schema, frames = _load_inputs(dataset_dir, config.benchmark)
    preprocessor = PairPreprocessor.fit(
        frames["train"],
        schema["numeric_feature_columns"],
        schema["categorical_feature_columns"],
    )
    matrices = {split: preprocessor.transform(frame) for split, frame in frames.items()}
    labels = {
        split: frame["target"].astype(np.int8).to_numpy() for split, frame in frames.items()
    }
    model = CatBoostClassifier(
        iterations=config.iterations,
        depth=config.depth,
        learning_rate=config.learning_rate,
        loss_function="Logloss",
        eval_metric="PRAUC",
        class_weights=[1.0, config.positive_class_weight],
        random_seed=config.random_seed,
        allow_writing_files=False,
        verbose=False,
    )
    model.fit(
        Pool(matrices["train"], labels["train"]),
        eval_set=Pool(matrices["validation"], labels["validation"]),
        early_stopping_rounds=config.early_stopping_rounds,
        use_best_model=True,
        verbose=False,
    )
    raw = {
        split: np.asarray(model.predict_proba(matrix), dtype=np.float64)[:, 1]
        for split, matrix in matrices.items()
    }
    calibrator = PlattCalibrator.fit(labels["validation"], raw["validation"])
    calibrated = {split: calibrator.predict(values) for split, values in raw.items()}
    threshold = select_alert_budget_threshold(
        labels["validation"], calibrated["validation"], config.max_alert_rate
    )
    counts = {split: class_counts(frame) for split, frame in frames.items()}
    reports = {
        split: binary_classification_report(
            labels[split],
            calibrated[split],
            threshold=threshold,
            max_alert_rate=config.max_alert_rate,
            hand_count=counts[split]["hands"],
        )
        for split in ("validation", "test")
    }
    model.save_model(output_dir / "model.cbm")
    _write_json(output_dir / "preprocessing.json", preprocessor.to_dict())
    _write_json(output_dir / "calibration.json", calibrator.to_dict())
    prediction_frames = []
    for split in ("validation", "test"):
        prediction_frames.append(
            pd.DataFrame(
                {
                    "split": split,
                    "event_id": frames[split]["event_id"].astype(str),
                    "hand_id": frames[split]["hand_id"].astype(str),
                    "pair_key": frames[split]["pair_key"].astype(str),
                    "raw_probability": raw[split],
                    "calibrated_probability": calibrated[split],
                    "alert": calibrated[split] >= threshold,
                }
            )
        )
    pd.concat(prediction_frames, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )
    run_id = f"pair_public_{uuid.uuid4().hex[:12]}"
    metrics = {
        "run_id": run_id,
        "model_name": "pair-catboost-public-v1",
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "benchmark": config.benchmark,
        "dataset_id": manifest["dataset_id"],
        "feature_definition_version": manifest["feature_definition_version"],
        "dataset_manifest_sha256": sha256_file(dataset_dir / "manifest.json"),
        "challenge_artifacts_read": False,
        "challenge_labels_used": False,
        "counts": counts,
        "training_config": {
            **asdict(config),
            "dataset_dir": str(config.dataset_dir),
            "output_dir": str(config.output_dir),
        },
        "best_iteration": int(model.get_best_iteration()),
        "calibration": calibrator.to_dict(),
        "threshold": threshold,
        "reports": {"catboost": reports},
    }
    _write_json(output_dir / "metrics.json", metrics)
    artifacts = {
        str(path.relative_to(output_dir)): sha256_file(path)
        for path in sorted(output_dir.rglob("*"))
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    _write_json(
        output_dir / "artifact_manifest.json",
        {"run_id": run_id, "artifacts": artifacts},
    )
    return metrics
