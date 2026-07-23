"""Leakage-safe Phase 8 training pipeline for the pair-level CatBoost model."""

from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, Pool
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from pipeline.ml.pair_model import (
    PairPreprocessor,
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    rules_only_score,
    select_alert_budget_threshold,
)
from pipeline.ml.dataset_guardrails import assert_training_allowed


PAIR_MODEL_VERSION = "pair-catboost-v1"
SUPPORTED_BENCHMARKS = ("cold_start", "temporal", "new_relationship")


@dataclass(frozen=True)
class PairTrainingConfig:
    dataset_dir: Path = Path("data/datasets/pair-v1")
    output_dir: Path = Path("models/pair-catboost-v1")
    benchmark: str = "cold_start"
    iterations: int = 500
    depth: int = 3
    learning_rate: float = 0.03
    early_stopping_rounds: int = 80
    positive_class_weight: float = 100.0
    max_alert_rate: float = 0.02
    random_seed: int = 42
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.benchmark not in SUPPORTED_BENCHMARKS:
            raise ValueError(f"unsupported benchmark: {self.benchmark}")
        if self.iterations < 1 or self.depth < 1:
            raise ValueError("iterations and depth must be positive")
        if not 0 < self.learning_rate <= 1:
            raise ValueError("learning_rate must be in (0, 1]")
        if self.positive_class_weight <= 0:
            raise ValueError("positive_class_weight must be positive")
        if not 0 < self.max_alert_rate <= 1:
            raise ValueError("max_alert_rate must be in (0, 1]")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _verify_artifact(root: Path, manifest: dict[str, Any], relative: str) -> None:
    expected = manifest["artifacts"].get(relative)
    if expected is None:
        raise ValueError(f"dataset manifest does not track {relative}")
    path = root / relative
    if not path.is_file():
        raise FileNotFoundError(path)
    actual = _sha256(path)
    if actual != expected:
        raise ValueError(f"dataset artifact hash mismatch: {relative}")


def _load_inputs(
    root: Path, benchmark: str
) -> tuple[dict[str, Any], dict[str, Any], dict[str, pd.DataFrame]]:
    manifest_path = root / "manifest.json"
    schema_path = root / "schema.json"
    if not manifest_path.is_file() or not schema_path.is_file():
        raise FileNotFoundError("pair dataset requires manifest.json and schema.json")
    manifest = json.loads(manifest_path.read_text())
    schema = json.loads(schema_path.read_text())
    if manifest["feature_definition_version"] != "pair-features-v1":
        raise ValueError("the trainer only accepts pair-features-v1")
    if manifest["challenge_labels_public"] or schema["challenge_labels_public"]:
        raise ValueError("challenge labels must remain private")

    _verify_artifact(root, manifest, "schema.json")
    frames: dict[str, pd.DataFrame] = {}
    for split in ("train", "validation", "test"):
        relative = f"dgx/{benchmark}/{split}.parquet"
        _verify_artifact(root, manifest, relative)
        frames[split] = pd.read_parquet(root / relative)

    challenge_feature_relative = "benchmarks/challenge/challenge/features.parquet"
    challenge_label_relative = (
        "benchmarks/challenge/challenge/private_labels/pair_labels.parquet"
    )
    _verify_artifact(root, manifest, challenge_feature_relative)
    _verify_artifact(root, manifest, challenge_label_relative)
    challenge = pd.read_parquet(root / challenge_feature_relative)
    challenge_labels = pd.read_parquet(root / challenge_label_relative)[
        ["hand_id", "pair_key", "is_collusive"]
    ].rename(columns={"is_collusive": "target"})
    frames["challenge"] = challenge.merge(
        challenge_labels,
        on=["hand_id", "pair_key"],
        how="inner",
        validate="one_to_one",
    )

    required = set(schema["numeric_feature_columns"]) | set(
        schema["categorical_feature_columns"]
    ) | {"target", "hand_id", "pair_key", "event_id"}
    for split, frame in frames.items():
        missing = sorted(required - set(frame.columns))
        if missing:
            raise ValueError(f"{split} is missing columns: {missing}")
        if not set(frame["target"].astype(int).unique()).issubset({0, 1}):
            raise ValueError(f"{split} has non-binary targets")
    return manifest, schema, frames


def _positive_probability(model: Any, matrix: np.ndarray) -> np.ndarray:
    return np.asarray(model.predict_proba(matrix), dtype=np.float64)[:, 1]


def _fit_player_baseline(
    matrices: dict[str, np.ndarray],
    labels: dict[str, np.ndarray],
    feature_names: tuple[str, ...],
    random_seed: int,
) -> tuple[Any, np.ndarray]:
    indices = np.asarray(
        [
            index
            for index, name in enumerate(feature_names)
            if name.startswith(("context_", "user_a_", "user_b_"))
        ],
        dtype=np.int64,
    )
    if not len(indices):
        raise ValueError("no player-only baseline columns were selected")
    # Player-history totals and rates have very different scales. Fit scaling
    # on train only so this comparison converges without leaking evaluation
    # statistics.
    model = make_pipeline(
        StandardScaler(),
        LogisticRegression(
            class_weight="balanced",
            max_iter=500,
            random_state=random_seed,
            solver="lbfgs",
        ),
    )
    model.fit(matrices["train"][:, indices], labels["train"])
    return model, indices


def _tensor_probability_onnx(path: Path, n_features: int) -> tuple[str, str]:
    """Replace ONNX-ML ZipMap with a float tensor suitable for Triton/Go."""
    import onnx
    from onnx import TensorProto, helper

    graph = onnx.load(path)
    input_name = graph.graph.input[0].name
    probability_source: str | None = None
    for node in list(graph.graph.node):
        if node.op_type == "ZipMap":
            probability_source = node.input[0]
            graph.graph.node.remove(node)
            break
    if probability_source is None:
        for output in graph.graph.output:
            if "prob" in output.name.lower() and output.type.HasField("tensor_type"):
                probability_source = output.name
                break
    if probability_source is None:
        raise RuntimeError("CatBoost ONNX export has no probability tensor")

    output_name = "pair_probabilities"
    if probability_source != output_name:
        if not any(opset.domain == "" for opset in graph.opset_import):
            graph.opset_import.append(helper.make_opsetid("", 13))
        graph.graph.node.append(
            helper.make_node(
                "Identity",
                inputs=[probability_source],
                outputs=[output_name],
                name="ExposePairProbabilityTensor",
            )
        )
    del graph.graph.output[:]
    graph.graph.output.extend(
        [helper.make_tensor_value_info(output_name, TensorProto.FLOAT, [None, 2])]
    )
    onnx.checker.check_model(graph)
    onnx.save(graph, path)
    return input_name, output_name


def _validate_onnx(
    path: Path,
    matrix: np.ndarray,
    expected: np.ndarray,
    input_name: str,
    output_name: str,
) -> dict[str, float | int]:
    import onnxruntime as ort

    session = ort.InferenceSession(str(path), providers=["CPUExecutionProvider"])
    sample = matrix[: min(15, len(matrix))].astype(np.float32, copy=False)
    actual = np.asarray(
        session.run([output_name], {input_name: sample})[0], dtype=np.float64
    )[:, 1]
    difference = float(np.max(np.abs(actual - expected[: len(sample)])))
    if difference > 1e-5:
        raise RuntimeError(f"CatBoost/ONNX probability mismatch: {difference}")

    for _ in range(5):
        session.run([output_name], {input_name: sample})
    timings_ms: list[float] = []
    for _ in range(50):
        started = time.perf_counter()
        session.run([output_name], {input_name: sample})
        timings_ms.append((time.perf_counter() - started) * 1000)
    return {
        "batch_rows": int(len(sample)),
        "runs": len(timings_ms),
        "p50_ms": float(np.percentile(timings_ms, 50)),
        "p95_ms": float(np.percentile(timings_ms, 95)),
        "max_probability_difference": difference,
    }


def _write_triton_repository(
    output_dir: Path,
    onnx_path: Path,
    *,
    input_name: str,
    output_name: str,
    n_features: int,
) -> Path:
    model_root = output_dir / "triton" / "pair_catboost"
    version_root = model_root / "1"
    version_root.mkdir(parents=True, exist_ok=True)
    shutil.copy2(onnx_path, version_root / "model.onnx")
    config = f'''name: "pair_catboost"
platform: "onnxruntime_onnx"
max_batch_size: 128
input [
  {{ name: "{input_name}" data_type: TYPE_FP32 dims: [ {n_features} ] }}
]
output [
  {{ name: "{output_name}" data_type: TYPE_FP32 dims: [ 2 ] }}
]
dynamic_batching {{
  preferred_batch_size: [ 15, 30, 60 ]
  max_queue_delay_microseconds: 1000
}}
'''
    config_path = model_root / "config.pbtxt"
    config_path.write_text(config)
    return config_path


def _threshold_and_report(
    validation_labels: np.ndarray,
    validation_probabilities: np.ndarray,
    frames: dict[str, pd.DataFrame],
    split_probabilities: dict[str, np.ndarray],
    max_alert_rate: float,
) -> tuple[float, dict[str, dict[str, Any]]]:
    threshold = select_alert_budget_threshold(
        validation_labels, validation_probabilities, max_alert_rate
    )
    reports = {
        split: binary_classification_report(
            frames[split]["target"].astype(int).to_numpy(),
            probabilities,
            threshold=threshold,
            max_alert_rate=max_alert_rate,
            hand_count=int(frames[split]["hand_id"].nunique()),
        )
        for split, probabilities in split_probabilities.items()
    }
    return threshold, reports


def _quality_gate(
    counts: dict[str, dict[str, int]],
    reports: dict[str, dict[str, dict[str, Any]]],
    latency: dict[str, float | int],
) -> dict[str, Any]:
    reasons: list[str] = []
    minimum_positives = {"train": 50, "validation": 20, "test": 20}
    for split, minimum in minimum_positives.items():
        if counts[split]["positives"] < minimum:
            reasons.append(
                f"{split} has {counts[split]['positives']} positives; need at least {minimum}"
            )

    test_pr = reports["catboost"]["test"]["pr_auc"]
    baseline_pr = [
        reports[name]["test"]["pr_auc"] for name in ("rules_only", "player_only")
    ]
    if test_pr is None or any(value is None for value in baseline_pr):
        reasons.append("test PR-AUC comparison is undefined")
    elif not all(test_pr >= value * 1.05 for value in baseline_pr):
        reasons.append(
            "CatBoost does not beat both test PR-AUC baselines by at least 5%"
        )
    test_report = reports["catboost"]["test"]
    test_prevalence = float(test_report["positive_rate"])
    if test_pr is not None and test_pr < test_prevalence * 2:
        reasons.append("test PR-AUC is less than 2x the positive base rate")
    test_budget_recall = test_report["recall_at_alert_budget"]
    if test_budget_recall is None or float(test_budget_recall) < 0.10:
        reasons.append("test recall at the alert budget is below 10%")
    if float(test_report["f1"]) <= 0:
        reasons.append("validation-selected threshold has zero test F1")
    challenge_report = reports["catboost"]["challenge"]
    challenge_pr = challenge_report["pr_auc"]
    challenge_prevalence = float(challenge_report["positive_rate"])
    if challenge_pr is None or challenge_pr < challenge_prevalence * 1.25:
        reasons.append("challenge PR-AUC is less than 1.25x its positive base rate")
    if float(latency["p95_ms"]) >= 1000:
        reasons.append("local ONNX p95 latency is not below one second")
    return {
        "promotion_eligible": not reasons,
        "reasons": reasons,
        "minimum_positive_examples": minimum_positives,
    }


def train_pair_catboost(config: PairTrainingConfig) -> dict[str, Any]:
    dataset_dir = config.dataset_dir.resolve()
    output_dir = config.output_dir.resolve()
    assert_training_allowed(dataset_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(
                f"output directory is not empty: {output_dir}; pass --overwrite"
            )
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest, schema, frames = _load_inputs(dataset_dir, config.benchmark)
    counts = {split: class_counts(frame) for split, frame in frames.items()}
    for split in ("train", "validation"):
        if counts[split]["positives"] == 0 or counts[split]["negatives"] == 0:
            raise RuntimeError(f"{split} must contain positive and negative examples")

    preprocessor = PairPreprocessor.fit(
        frames["train"],
        schema["numeric_feature_columns"],
        schema["categorical_feature_columns"],
    )
    matrices = {
        split: preprocessor.transform(frame) for split, frame in frames.items()
    }
    labels = {
        split: frame["target"].astype(int).to_numpy(dtype=np.int8)
        for split, frame in frames.items()
    }
    feature_names = preprocessor.output_columns

    train_pool = Pool(matrices["train"], labels["train"], feature_names=list(feature_names))
    validation_pool = Pool(
        matrices["validation"],
        labels["validation"],
        feature_names=list(feature_names),
    )
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
        train_pool,
        eval_set=validation_pool,
        early_stopping_rounds=config.early_stopping_rounds,
        use_best_model=True,
        verbose=False,
    )

    raw_probabilities = {
        split: _positive_probability(model, matrix)
        for split, matrix in matrices.items()
    }
    calibrator = PlattCalibrator.fit(
        labels["validation"], raw_probabilities["validation"]
    )
    calibrated_probabilities = {
        split: calibrator.predict(probabilities)
        for split, probabilities in raw_probabilities.items()
    }
    cat_threshold, cat_reports = _threshold_and_report(
        labels["validation"],
        calibrated_probabilities["validation"],
        frames,
        calibrated_probabilities,
        config.max_alert_rate,
    )

    rule_probabilities = {
        split: rules_only_score(frame) for split, frame in frames.items()
    }
    rule_threshold, rule_reports = _threshold_and_report(
        labels["validation"],
        rule_probabilities["validation"],
        frames,
        rule_probabilities,
        config.max_alert_rate,
    )

    player_model, player_indices = _fit_player_baseline(
        matrices, labels, feature_names, config.random_seed
    )
    player_probabilities = {
        split: _positive_probability(player_model, matrix[:, player_indices])
        for split, matrix in matrices.items()
    }
    player_threshold, player_reports = _threshold_and_report(
        labels["validation"],
        player_probabilities["validation"],
        frames,
        player_probabilities,
        config.max_alert_rate,
    )

    model_path = output_dir / "model.cbm"
    onnx_path = output_dir / "model.onnx"
    model.save_model(model_path)
    model.save_model(
        onnx_path,
        format="onnx",
        export_parameters={
            "onnx_domain": "ai.poker",
            "onnx_model_version": 1,
            "onnx_doc_string": "pair-catboost-v1 raw probabilities",
            "onnx_graph_name": "PairCatBoost",
        },
    )
    input_name, output_name = _tensor_probability_onnx(
        onnx_path, matrices["train"].shape[1]
    )
    latency = _validate_onnx(
        onnx_path,
        matrices["validation"],
        raw_probabilities["validation"],
        input_name,
        output_name,
    )
    triton_config_path = _write_triton_repository(
        output_dir,
        onnx_path,
        input_name=input_name,
        output_name=output_name,
        n_features=matrices["train"].shape[1],
    )

    _write_json(output_dir / "preprocessing.json", preprocessor.to_dict())
    _write_json(output_dir / "calibration.json", calibrator.to_dict())
    decision_policy = {
        "policy_version": 1,
        "probability": "platt_calibrated_positive_class",
        "threshold": cat_threshold,
        "validation_max_alert_rate": config.max_alert_rate,
        "pairs_per_six_player_hand": 15,
        "aggregation": {"player": "max_pair_probability", "hand": "max_pair_probability"},
    }
    _write_json(output_dir / "decision_policy.json", decision_policy)

    importance = pd.DataFrame(
        {
            "feature": feature_names,
            "importance": model.get_feature_importance(type="FeatureImportance"),
        }
    ).sort_values("importance", ascending=False, kind="mergesort")
    importance.to_csv(output_dir / "feature_importance.csv", index=False)
    explain_rows = min(1000, len(frames["validation"]))
    explain_pool = Pool(
        matrices["validation"][:explain_rows],
        labels["validation"][:explain_rows],
        feature_names=list(feature_names),
    )
    shap_values = np.asarray(
        model.get_feature_importance(explain_pool, type="ShapValues"),
        dtype=np.float64,
    )[:, :-1]
    shap_summary = pd.DataFrame(
        {
            "feature": feature_names,
            "mean_abs_shap": np.abs(shap_values).mean(axis=0),
        }
    ).sort_values("mean_abs_shap", ascending=False, kind="mergesort")
    shap_summary.to_csv(output_dir / "shap_summary.csv", index=False)

    prediction_rows: list[pd.DataFrame] = []
    for split in ("validation", "test", "challenge"):
        prediction_rows.append(
            pd.DataFrame(
                {
                    "split": split,
                    "event_id": frames[split]["event_id"].astype(str),
                    "hand_id": frames[split]["hand_id"].astype(str),
                    "pair_key": frames[split]["pair_key"].astype(str),
                    "raw_probability": raw_probabilities[split],
                    "calibrated_probability": calibrated_probabilities[split],
                    "alert": calibrated_probabilities[split] >= cat_threshold,
                }
            )
        )
    pd.concat(prediction_rows, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )

    reports = {
        "catboost": cat_reports,
        "rules_only": rule_reports,
        "player_only": player_reports,
    }
    quality_gate = _quality_gate(counts, reports, latency)
    run_id = f"pair_{uuid.uuid4().hex[:12]}"
    metrics = {
        "run_id": run_id,
        "model_name": PAIR_MODEL_VERSION,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "benchmark": config.benchmark,
        "dataset_id": manifest["dataset_id"],
        "feature_definition_version": manifest["feature_definition_version"],
        "dataset_manifest_sha256": _sha256(dataset_dir / "manifest.json"),
        "counts": counts,
        "training_config": {
            "iterations": config.iterations,
            "depth": config.depth,
            "learning_rate": config.learning_rate,
            "early_stopping_rounds": config.early_stopping_rounds,
            "positive_class_weight": config.positive_class_weight,
            "max_alert_rate": config.max_alert_rate,
            "random_seed": config.random_seed,
        },
        "best_iteration": int(model.get_best_iteration()),
        "calibration": calibrator.to_dict(),
        "thresholds": {
            "catboost": cat_threshold,
            "rules_only": rule_threshold,
            "player_only": player_threshold,
        },
        "reports": reports,
        "onnx_latency": latency,
        "quality_gate": quality_gate,
    }
    _write_json(output_dir / "metrics.json", metrics)

    scoring_contract = {
        "contract_version": 1,
        "model_name": PAIR_MODEL_VERSION,
        "run_id": run_id,
        "feature_definition_version": manifest["feature_definition_version"],
        "input": {
            "name": input_name,
            "dtype": "float32",
            "shape": [None, len(feature_names)],
            "preprocessing": "preprocessing.json",
            "ordered_features": list(feature_names),
        },
        "output": {
            "name": output_name,
            "dtype": "float32",
            "shape": [None, 2],
            "positive_class_index": 1,
            "probabilities_are_calibrated": False,
        },
        "calibration": "calibration.json",
        "decision_policy": "decision_policy.json",
        "batching": {
            "unit": "hand",
            "expected_pairs_per_six_player_hand": 15,
            "triton_model": "pair_catboost",
        },
    }
    _write_json(output_dir / "scoring_contract.json", scoring_contract)

    artifact_paths = [
        path
        for path in output_dir.rglob("*")
        if path.is_file() and path.name != "artifact_manifest.json"
    ]
    artifact_manifest = {
        "run_id": run_id,
        "model_name": PAIR_MODEL_VERSION,
        "artifacts": {
            str(path.relative_to(output_dir)): _sha256(path)
            for path in sorted(artifact_paths)
        },
        "triton_config": str(triton_config_path.relative_to(output_dir)),
    }
    _write_json(output_dir / "artifact_manifest.json", artifact_manifest)
    return metrics
