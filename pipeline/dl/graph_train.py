"""Train and gate Phase 11 inductive heterogeneous graph models."""

from __future__ import annotations

import json
import math
import random
import shutil
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Mapping

import numpy as np
import pandas as pd
import torch
from sklearn.metrics import average_precision_score
from torch import nn
from torch.utils.data import DataLoader, Dataset, Subset

from pipeline.ml.pair_model import (
    PlattCalibrator,
    binary_classification_report,
    class_counts,
    select_alert_budget_threshold,
)

from .graph_dataset import (
    GRAPH_BENCHMARKS,
    GRAPH_SPLITS,
    PAIR_GRAPH_FEATURES,
    RESOURCE_NODE_FEATURES,
    ROOT_USER_FEATURES,
    USER_EDGE_FEATURES,
    event_alignment_sha256,
    load_graph_split,
    sha256_file,
)
from .graph_models import TemporalHeteroGraphSAGE
from .pair_challengers import (
    NeuralPairPreprocessor,
    _load_inputs,
    challenger_gate,
    paired_hand_bootstrap_pr_auc,
)


DeviceName = Literal["auto", "cpu", "cuda"]
MODEL_NAME = "temporal_hetero_graphsage"


@dataclass(frozen=True)
class GraphTrainingConfig:
    graph_dataset_dir: Path = Path("data/datasets/pair-graph-full-v2")
    pair_dataset_dir: Path = Path("data/datasets/pair-full-v2")
    cold_start_baseline_dir: Path = Path("models/pair-catboost-full-v2")
    new_relationship_baseline_dir: Path = Path(
        "models/pair-catboost-new-relationship-v2"
    )
    output_dir: Path = Path("models/pair-graph-full-v2")
    benchmarks: tuple[str, ...] = GRAPH_BENCHMARKS
    epochs: int = 15
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
    graph_width: int = 64
    device_name: DeviceName = "auto"
    overwrite: bool = False

    def __post_init__(self) -> None:
        if not self.benchmarks or any(value not in GRAPH_BENCHMARKS for value in self.benchmarks):
            raise ValueError(f"benchmarks must be selected from {GRAPH_BENCHMARKS}")
        if len(set(self.benchmarks)) != len(self.benchmarks):
            raise ValueError("graph training benchmarks must be unique")
        if min(
            self.epochs,
            self.batch_size,
            self.patience,
            self.bootstrap_samples,
            self.graph_width,
        ) < 1:
            raise ValueError("graph training counts must be positive")
        if self.learning_rate <= 0 or self.weight_decay < 0:
            raise ValueError("invalid graph optimizer settings")
        if self.positive_class_weight <= 0 or self.num_workers < 0:
            raise ValueError("invalid graph loader or class weight")
        if not 0 < self.max_alert_rate <= 1 or self.minimum_relative_pr_gain < 0:
            raise ValueError("invalid graph promotion gate")


@dataclass(frozen=True)
class FeatureNormalizer:
    feature_names: tuple[str, ...]
    means: tuple[float, ...]
    scales: tuple[float, ...]
    valid_rows: int

    @classmethod
    def fit(
        cls,
        values: np.ndarray,
        feature_names: tuple[str, ...],
        mask: np.ndarray | None = None,
    ) -> "FeatureNormalizer":
        if values.shape[-1] != len(feature_names):
            raise ValueError("graph feature names disagree with tensor width")
        flat = values.reshape(-1, values.shape[-1]).astype(np.float64)
        if mask is not None:
            selected = flat[mask.reshape(-1).astype(bool)]
        else:
            selected = flat
        if len(selected) < 1 or not np.isfinite(selected).all():
            raise ValueError("graph normalizer needs finite valid features")
        means = selected.mean(axis=0)
        scales = selected.std(axis=0)
        scales[~np.isfinite(scales) | (scales < 1e-6)] = 1.0
        return cls(
            feature_names=feature_names,
            means=tuple(float(value) for value in means),
            scales=tuple(float(value) for value in scales),
            valid_rows=len(selected),
        )

    def transform(self, values: np.ndarray, mask: np.ndarray | None = None) -> np.ndarray:
        means = np.asarray(self.means, dtype=np.float32)
        scales = np.asarray(self.scales, dtype=np.float32)
        output = ((values.astype(np.float32) - means) / scales).astype(np.float16)
        if mask is not None:
            output *= mask[..., None]
        if not np.isfinite(output).all():
            raise ValueError("normalized graph features are non-finite")
        return output

    def to_dict(self) -> dict[str, Any]:
        return {
            "fit_split": "train",
            "feature_names": list(self.feature_names),
            "means": list(self.means),
            "scales": list(self.scales),
            "valid_rows": self.valid_rows,
        }


class PairGraphDataset(Dataset):
    def __init__(
        self,
        numeric: np.ndarray,
        categorical: np.ndarray,
        labels: np.ndarray,
        graph: Mapping[str, np.ndarray],
    ) -> None:
        if len(numeric) != len(categorical) or len(numeric) != len(labels):
            raise ValueError("graph tabular arrays are not aligned")
        if len(labels) != len(graph["root_features"]):
            raise ValueError("graph arrays are not aligned with examples")
        self.values = (
            torch.from_numpy(numeric),
            torch.from_numpy(categorical),
            torch.from_numpy(graph["root_features"]),
            torch.from_numpy(graph["user_neighbor_features"]),
            torch.from_numpy(graph["user_edge_features"]),
            torch.from_numpy(graph["user_neighbor_masks"].astype(bool, copy=False)),
            torch.from_numpy(graph["resource_features"]),
            torch.from_numpy(graph["resource_masks"].astype(bool, copy=False)),
            torch.from_numpy(graph["pair_graph_features"]),
            torch.from_numpy(labels.astype(np.float32, copy=False)),
        )

    def __len__(self) -> int:
        return len(self.values[-1])

    def __getitem__(self, index: int) -> tuple[torch.Tensor, ...]:
        return tuple(value[index] for value in self.values)


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


def _load_baseline(
    baseline_dir: Path,
    benchmark: str,
    frames: Mapping[str, pd.DataFrame],
    pair_manifest_sha: str,
) -> tuple[dict[str, Any], np.ndarray]:
    metrics_path = baseline_dir / "metrics.json"
    predictions_path = baseline_dir / "predictions.parquet"
    artifact_manifest_path = baseline_dir / "artifact_manifest.json"
    artifact_manifest = json.loads(artifact_manifest_path.read_text())
    for path in (metrics_path, predictions_path):
        relative = path.name
        if sha256_file(path) != artifact_manifest["artifacts"][relative]:
            raise ValueError(f"{benchmark} baseline artifact hash mismatch: {relative}")
    metrics = json.loads(metrics_path.read_text())
    if metrics["run_id"] != artifact_manifest["run_id"]:
        raise ValueError(f"{benchmark} baseline artifact run IDs disagree")
    if metrics["dataset_manifest_sha256"] != pair_manifest_sha:
        raise ValueError(f"{benchmark} baseline uses another pair dataset")
    if metrics["benchmark"] != benchmark:
        raise ValueError(f"baseline benchmark mismatch: expected {benchmark}")
    predictions = pd.read_parquet(predictions_path)
    predictions = predictions[predictions["split"] == "test"][
        ["event_id", "calibrated_probability"]
    ].copy()
    candidate = frames["test"][["event_id"]].copy()
    candidate["event_id"] = candidate["event_id"].astype(str)
    predictions["event_id"] = predictions["event_id"].astype(str)
    aligned = candidate.merge(predictions, on="event_id", how="left", validate="one_to_one")
    if aligned["calibrated_probability"].isna().any():
        raise ValueError(f"{benchmark} baseline predictions are incomplete")
    return metrics, aligned["calibrated_probability"].to_numpy(dtype=np.float64)


def _load_benchmark(
    config: GraphTrainingConfig,
    benchmark: str,
    graph_manifest: Mapping[str, Any],
    graph_schema: Mapping[str, Any],
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, pd.DataFrame],
    dict[str, dict[str, np.ndarray]],
    dict[str, Any],
    np.ndarray,
]:
    pair_manifest, pair_schema, frames = _load_inputs(
        config.pair_dataset_dir.resolve(), benchmark
    )
    pair_manifest_sha = sha256_file(config.pair_dataset_dir.resolve() / "manifest.json")
    if graph_manifest["source_pair_manifest_sha256"] != pair_manifest_sha:
        raise ValueError("graph and pair dataset manifests disagree")
    graphs = {}
    for split in GRAPH_SPLITS:
        relative = f"benchmarks/{benchmark}/{split}.npz"
        path = config.graph_dataset_dir.resolve() / relative
        if sha256_file(path) != graph_manifest["artifacts"][relative]:
            raise ValueError(f"graph artifact hash mismatch: {relative}")
        graph = load_graph_split(path)
        event_ids = frames[split]["event_id"].astype(str).to_numpy()
        if not np.array_equal(graph["event_ids"].astype(str), event_ids):
            raise ValueError(f"{benchmark}/{split} graph alignment failed")
        if event_alignment_sha256(event_ids) != graph_manifest["benchmarks"][benchmark][
            "splits"
        ][split]["event_alignment_sha256"]:
            raise ValueError(f"{benchmark}/{split} graph event hash mismatch")
        if np.any(graph["graph_last_edge_ns"] >= graph["example_played_ns"]):
            raise ValueError(f"{benchmark}/{split} graph contains current/future edges")
        if not np.array_equal(
            graph["labels"], frames[split]["target"].astype(np.int8).to_numpy()
        ):
            raise ValueError(f"{benchmark}/{split} graph labels are misaligned")
        graphs[split] = graph
    baseline_dir = (
        config.cold_start_baseline_dir
        if benchmark == "cold_start"
        else config.new_relationship_baseline_dir
    ).resolve()
    baseline_metrics, baseline_probabilities = _load_baseline(
        baseline_dir, benchmark, frames, pair_manifest_sha
    )
    return (
        pair_manifest,
        pair_schema,
        frames,
        graphs,
        baseline_metrics,
        baseline_probabilities,
    )


def _loader(
    dataset: Dataset,
    *,
    batch_size: int,
    shuffle: bool,
    num_workers: int,
    pin_memory: bool,
    seed: int,
) -> DataLoader:
    return DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=num_workers,
        pin_memory=pin_memory,
        persistent_workers=num_workers > 0,
        generator=torch.Generator().manual_seed(seed) if shuffle else None,
    )


def _move_batch(batch: tuple[torch.Tensor, ...], device: torch.device) -> tuple[torch.Tensor, ...]:
    return tuple(value.to(device, non_blocking=True) for value in batch)


def _predict(
    model: TemporalHeteroGraphSAGE,
    loader: DataLoader,
    loss_function: nn.Module,
    device: torch.device,
) -> tuple[float, np.ndarray]:
    model.eval()
    total, rows = 0.0, 0
    probabilities = []
    with torch.no_grad():
        for raw_batch in loader:
            batch = _move_batch(raw_batch, device)
            with torch.autocast(
                device_type=device.type,
                dtype=torch.bfloat16,
                enabled=device.type == "cuda",
            ):
                logits = model(*batch[:-1])
                loss = loss_function(logits, batch[-1])
            total += float(loss) * len(logits)
            rows += len(logits)
            probabilities.append(torch.sigmoid(logits.float()).cpu().numpy())
    return total / max(rows, 1), np.concatenate(probabilities).astype(np.float64)


def _latency(
    model: TemporalHeteroGraphSAGE,
    dataset: PairGraphDataset,
    device: torch.device,
) -> dict[str, Any]:
    batch = _move_batch(
        next(iter(DataLoader(Subset(dataset, list(range(min(15, len(dataset))))), batch_size=15))),
        device,
    )
    timings = []
    model.eval()
    with torch.no_grad():
        for _ in range(10):
            model(*batch[:-1])
        if device.type == "cuda":
            torch.cuda.synchronize(device)
        for _ in range(100):
            started = time.perf_counter()
            model(*batch[:-1])
            if device.type == "cuda":
                torch.cuda.synchronize(device)
            timings.append((time.perf_counter() - started) * 1000)
    return {
        "device": str(device),
        "batch_rows": len(batch[-1]),
        "runs": len(timings),
        "p50_ms": float(np.percentile(timings, 50)),
        "p95_ms": float(np.percentile(timings, 95)),
    }


def _normalizers(graph: Mapping[str, np.ndarray]) -> dict[str, FeatureNormalizer]:
    user_mask = graph["user_neighbor_masks"]
    resource_mask = graph["resource_masks"]
    return {
        "root": FeatureNormalizer.fit(graph["root_features"], ROOT_USER_FEATURES),
        "user_neighbor": FeatureNormalizer.fit(
            graph["user_neighbor_features"], ROOT_USER_FEATURES, user_mask
        ),
        "user_edge": FeatureNormalizer.fit(
            graph["user_edge_features"], USER_EDGE_FEATURES, user_mask
        ),
        "resource": FeatureNormalizer.fit(
            graph["resource_features"], RESOURCE_NODE_FEATURES, resource_mask
        ),
        "pair": FeatureNormalizer.fit(graph["pair_graph_features"], PAIR_GRAPH_FEATURES),
    }


def _normalize_graph(
    graph: dict[str, np.ndarray], normalizers: Mapping[str, FeatureNormalizer]
) -> None:
    graph["root_features"] = normalizers["root"].transform(graph["root_features"])
    graph["user_neighbor_features"] = normalizers["user_neighbor"].transform(
        graph["user_neighbor_features"], graph["user_neighbor_masks"]
    )
    graph["user_edge_features"] = normalizers["user_edge"].transform(
        graph["user_edge_features"], graph["user_neighbor_masks"]
    )
    graph["resource_features"] = normalizers["resource"].transform(
        graph["resource_features"], graph["resource_masks"]
    )
    graph["pair_graph_features"] = normalizers["pair"].transform(
        graph["pair_graph_features"]
    )


def train_graph_models(config: GraphTrainingConfig) -> dict[str, Any]:
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    graph_root = config.graph_dataset_dir.resolve()
    graph_manifest = json.loads((graph_root / "manifest.json").read_text())
    graph_schema_path = graph_root / "schema.json"
    graph_schema = json.loads(graph_schema_path.read_text())
    if graph_manifest["challenge_artifacts_read"] is not False:
        raise ValueError("Phase 11 graph dataset read challenge artifacts")
    if graph_schema["raw_id_embedding_count"] != 0:
        raise ValueError("Phase 11 requires feature-only inductive nodes")
    if sha256_file(graph_schema_path) != graph_manifest["artifacts"]["schema.json"]:
        raise ValueError("Phase 11 graph schema hash mismatch")
    device = _resolve_device(config.device_name)
    benchmark_results: dict[str, Any] = {}
    prediction_frames = []
    for benchmark_index, benchmark in enumerate(config.benchmarks):
        (
            pair_manifest,
            pair_schema,
            frames,
            graphs,
            baseline_metrics,
            baseline_test_probabilities,
        ) = _load_benchmark(config, benchmark, graph_manifest, graph_schema)
        counts = {split: class_counts(frame) for split, frame in frames.items()}
        preprocessor = NeuralPairPreprocessor.fit(
            frames["train"],
            pair_schema["numeric_feature_columns"],
            pair_schema["categorical_feature_columns"],
        )
        tabular = {
            split: preprocessor.transform(frames[split]) for split in GRAPH_SPLITS
        }
        labels = {
            split: frames[split]["target"].astype(np.int8).to_numpy()
            for split in GRAPH_SPLITS
        }
        normalizers = _normalizers(graphs["train"])
        for split in GRAPH_SPLITS:
            _normalize_graph(graphs[split], normalizers)
        benchmark_dir = output_dir / benchmark
        benchmark_dir.mkdir()
        _write_json(
            benchmark_dir / "preprocessing.json",
            {
                "fit_split": "train",
                "tabular": preprocessor.to_dict(),
                "graph": {name: value.to_dict() for name, value in normalizers.items()},
            },
        )
        datasets = {
            split: PairGraphDataset(
                tabular[split][0], tabular[split][1], labels[split], graphs[split]
            )
            for split in GRAPH_SPLITS
        }
        pin_memory = device.type == "cuda"
        seed = config.random_seed + benchmark_index
        loaders = {
            split: _loader(
                dataset,
                batch_size=config.batch_size,
                shuffle=split == "train",
                num_workers=config.num_workers,
                pin_memory=pin_memory,
                seed=seed,
            )
            for split, dataset in datasets.items()
        }
        _seed_everything(seed)
        model = TemporalHeteroGraphSAGE(
            len(preprocessor.numeric_columns),
            preprocessor.categorical_cardinalities,
            len(ROOT_USER_FEATURES),
            len(USER_EDGE_FEATURES),
            len(RESOURCE_NODE_FEATURES),
            len(PAIR_GRAPH_FEATURES),
            width=config.graph_width,
        ).to(device)
        if model.raw_id_embedding_count != 0:
            raise RuntimeError("graph model unexpectedly contains raw-ID embeddings")
        optimizer = torch.optim.AdamW(
            model.parameters(), lr=config.learning_rate, weight_decay=config.weight_decay
        )
        loss_function = nn.BCEWithLogitsLoss(
            pos_weight=torch.tensor(config.positive_class_weight, device=device)
        )
        best_epoch = 0
        best_validation_pr = -math.inf
        best_validation_loss = math.inf
        best_state = None
        history = []
        started = time.perf_counter()
        for epoch in range(1, config.epochs + 1):
            epoch_started = time.perf_counter()
            model.train()
            total, rows = 0.0, 0
            for raw_batch in loaders["train"]:
                batch = _move_batch(raw_batch, device)
                optimizer.zero_grad(set_to_none=True)
                with torch.autocast(
                    device_type=device.type,
                    dtype=torch.bfloat16,
                    enabled=device.type == "cuda",
                ):
                    logits = model(*batch[:-1])
                    loss = loss_function(logits, batch[-1])
                loss.backward()
                torch.nn.utils.clip_grad_norm_(model.parameters(), 5.0)
                optimizer.step()
                total += float(loss.detach()) * len(logits)
                rows += len(logits)
            validation_loss, validation_raw = _predict(
                model, loaders["validation"], loss_function, device
            )
            validation_pr = float(
                average_precision_score(labels["validation"], validation_raw)
            )
            history.append(
                {
                    "epoch": epoch,
                    "train_loss": total / rows,
                    "validation_loss": validation_loss,
                    "validation_pr_auc": validation_pr,
                    "seconds": time.perf_counter() - epoch_started,
                }
            )
            print(
                f"[pair-graph][{benchmark}] epoch={epoch} train_loss={total / rows:.6f} "
                f"validation_loss={validation_loss:.6f} "
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
                    f"[pair-graph][{benchmark}] early_stop best_epoch={best_epoch}",
                    flush=True,
                )
                break
        if best_state is None:
            raise RuntimeError(f"{benchmark} graph training produced no checkpoint")
        model.load_state_dict(best_state)
        raw = {
            split: _predict(model, loaders[split], loss_function, device)[1]
            for split in ("validation", "test")
        }
        calibrator = PlattCalibrator.fit(labels["validation"], raw["validation"])
        calibrated = {split: calibrator.predict(values) for split, values in raw.items()}
        threshold = select_alert_budget_threshold(
            labels["validation"], calibrated["validation"], config.max_alert_rate
        )
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
        bootstrap = paired_hand_bootstrap_pr_auc(
            frames["test"],
            calibrated["test"],
            baseline_test_probabilities,
            samples=config.bootstrap_samples,
            seed=seed,
        )
        baseline_report = baseline_metrics["reports"]["catboost"]["test"]
        gate = challenger_gate(
            reports["test"],
            baseline_report,
            bootstrap,
            minimum_relative_pr_gain=config.minimum_relative_pr_gain,
            max_alert_rate=config.max_alert_rate,
        )
        latency = _latency(model, datasets["validation"], device)
        torch.save(
            {
                "model_name": MODEL_NAME,
                "benchmark": benchmark,
                "state_dict": best_state,
                "numeric_dim": len(preprocessor.numeric_columns),
                "categorical_cardinalities": preprocessor.categorical_cardinalities,
                "graph_width": config.graph_width,
                "raw_id_embedding_count": 0,
                "feature_definition_version": pair_manifest["feature_definition_version"],
            },
            benchmark_dir / "model.pt",
        )
        result = {
            "model_name": MODEL_NAME,
            "benchmark": benchmark,
            "best_epoch": best_epoch,
            "epochs_ran": len(history),
            "best_validation_pr_auc": best_validation_pr,
            "best_validation_loss": best_validation_loss,
            "training_seconds": time.perf_counter() - started,
            "parameter_count": sum(parameter.numel() for parameter in model.parameters()),
            "raw_id_embedding_count": 0,
            "calibration": calibrator.to_dict(),
            "threshold": threshold,
            "reports": reports,
            "latency": latency,
            "paired_bootstrap": bootstrap,
            "quality_gate": gate,
            "history": history,
            "counts": counts,
            "baseline": {
                "run_id": baseline_metrics["run_id"],
                "test": baseline_report,
            },
        }
        _write_json(benchmark_dir / "metrics.json", result)
        benchmark_results[benchmark] = result
        for split in ("validation", "test"):
            prediction_frames.append(
                pd.DataFrame(
                    {
                        "model_name": MODEL_NAME,
                        "benchmark": benchmark,
                        "split": split,
                        "event_id": frames[split]["event_id"].astype(str),
                        "hand_id": frames[split]["hand_id"].astype(str),
                        "pair_key": frames[split]["pair_key"].astype(str),
                        "target": labels[split],
                        "raw_probability": raw[split],
                        "calibrated_probability": calibrated[split],
                        "alert": calibrated[split] >= threshold,
                    }
                )
            )
        print(
            f"[pair-graph][{benchmark}] test_pr_auc={reports['test']['pr_auc']:.6f} "
            f"test_f1={reports['test']['f1']:.6f} "
            f"promotion_candidate={gate['promotion_candidate']}",
            flush=True,
        )
    pd.concat(prediction_frames, ignore_index=True).to_parquet(
        output_dir / "predictions.parquet", index=False
    )
    run_id = f"pair_graph_{uuid.uuid4().hex[:12]}"
    stable_lift = all(
        result["quality_gate"]["promotion_candidate"]
        for result in benchmark_results.values()
    ) and set(benchmark_results) == set(GRAPH_BENCHMARKS)
    summary = {
        "run_id": run_id,
        "phase": 11,
        "trained_at": datetime.now(tz=timezone.utc).isoformat(),
        "dataset_id": graph_manifest["dataset_id"],
        "feature_definition_version": graph_manifest["feature_definition_version"],
        "graph_dataset_manifest_sha256": sha256_file(graph_root / "manifest.json"),
        "pair_dataset_manifest_sha256": graph_manifest["source_pair_manifest_sha256"],
        "challenge_artifacts_read": False,
        "challenge_labels_used": False,
        "raw_id_embedding_count": 0,
        "inductive_node_initialization": "feature_only",
        "device": str(device),
        "gpu_name": torch.cuda.get_device_name(device) if device.type == "cuda" else None,
        "stable_incremental_lift": stable_lift,
        "promotion_eligible": False,
        "training_config": {
            **asdict(config),
            "graph_dataset_dir": str(config.graph_dataset_dir),
            "pair_dataset_dir": str(config.pair_dataset_dir),
            "cold_start_baseline_dir": str(config.cold_start_baseline_dir),
            "new_relationship_baseline_dir": str(config.new_relationship_baseline_dir),
            "output_dir": str(config.output_dir),
        },
        "benchmarks": benchmark_results,
    }
    _write_json(output_dir / "summary.json", summary)
    artifacts = {
        str(path.relative_to(output_dir)): sha256_file(path)
        for path in sorted(output_dir.rglob("*"))
        if path.is_file() and path.name != "artifact_manifest.json"
    }
    _write_json(
        output_dir / "artifact_manifest.json",
        {"run_id": run_id, "artifacts": artifacts},
    )
    return summary
