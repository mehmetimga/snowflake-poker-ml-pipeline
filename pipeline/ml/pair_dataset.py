"""Frozen, leakage-safe pair datasets built from immutable world artifacts."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from pipeline.context import enrich_player_hand, select_context_as_of
from pipeline.events import (
    CurrentHandPairFeatures,
    EventEnvelope,
    PairContextFeatures,
    PairFeatureEvent,
    PairHandLabel,
    PairHistoryFeatures,
    PlayerHandContextEvent,
    UserHistoryFeatures,
    validate_event,
)
from pipeline.features import PairFeatureCore
from pipeline.ml.dataset_guardrails import assert_training_allowed


PAIR_DATASET_SCHEMA_VERSION = 1
SPLIT_NAMES = ("train", "validation", "test", "challenge")


def _iter_jsonl(path: Path):
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def _prefixed(prefix: str, fields: Iterable[str]) -> list[str]:
    return [f"{prefix}_{field}" for field in fields]


MODEL_NUMERIC_FEATURE_COLUMNS = (
    _prefixed("current", CurrentHandPairFeatures.model_fields)
    + _prefixed("context", PairContextFeatures.model_fields)
    + _prefixed("user_a", UserHistoryFeatures.model_fields)
    + _prefixed("user_b", UserHistoryFeatures.model_fields)
    + _prefixed("pair", PairHistoryFeatures.model_fields)
)
MODEL_CATEGORICAL_FEATURE_COLUMNS = ["context_status_a", "context_status_b"]
MODEL_FEATURE_COLUMNS = MODEL_NUMERIC_FEATURE_COLUMNS + MODEL_CATEGORICAL_FEATURE_COLUMNS

AUDIT_COLUMNS = [
    "event_id",
    "dataset_id",
    "source_dataset_split",
    "benchmark_split",
    "hand_id",
    "table_id",
    "played_at",
    "pair_key",
    "player_a",
    "player_b",
    "num_players",
    "snapshot_revision",
    "feature_definition_version",
    "source_hand_event_id",
    "source_player_context_event_id_a",
    "source_player_context_event_id_b",
    "source_revision_a",
    "source_revision_b",
    "context_version_a",
    "context_version_b",
]

LABEL_COLUMNS = [
    "example_id",
    "dataset_id",
    "source_dataset_split",
    "benchmark_split",
    "hand_id",
    "pair_key",
    "player_a",
    "player_b",
    "is_collusive",
    "collusion_pair_id",
    "label_available_at",
    "provenance",
]


@dataclass(frozen=True)
class PairDatasetBuildConfig:
    source_dir: Path
    output_dir: Path
    temporal_source_split: str = "train"
    new_relationship_source_split: str = "train"
    overwrite: bool = False

    def __post_init__(self) -> None:
        if self.temporal_source_split not in SPLIT_NAMES:
            raise ValueError("invalid temporal source split")
        if self.new_relationship_source_split not in SPLIT_NAMES:
            raise ValueError("invalid new-relationship source split")
        if self.output_dir.resolve() == self.source_dir.resolve():
            raise ValueError("pair output directory must differ from the source directory")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _population_hash(values: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(set(values))).encode("utf-8")).hexdigest()


def _flatten_group(prefix: str, value: object) -> dict[str, Any]:
    raw = value.model_dump(mode="json")
    return {f"{prefix}_{key}": child for key, child in raw.items()}


def flatten_pair_feature(event: PairFeatureEvent, benchmark_split: str) -> dict[str, Any]:
    payload = event.payload
    row: dict[str, Any] = {
        "event_id": str(event.event_id),
        "dataset_id": event.dataset_id,
        "source_dataset_split": event.dataset_split,
        "benchmark_split": benchmark_split,
        "hand_id": payload.hand_id,
        "table_id": payload.table_id,
        "played_at": payload.played_at.isoformat(),
        "pair_key": payload.pair_key,
        "player_a": payload.player_a,
        "player_b": payload.player_b,
        "num_players": payload.num_players,
        "snapshot_revision": payload.snapshot_revision,
        "feature_definition_version": payload.feature_definition_version,
        "source_hand_event_id": str(payload.source_hand_event_id),
        "source_player_context_event_id_a": str(
            payload.source_player_context_event_id_a
        ),
        "source_player_context_event_id_b": str(
            payload.source_player_context_event_id_b
        ),
        "source_revision_a": payload.source_revision_a,
        "source_revision_b": payload.source_revision_b,
        "context_status_a": payload.context_status_a,
        "context_status_b": payload.context_status_b,
        "context_version_a": payload.context_version_a,
        "context_version_b": payload.context_version_b,
    }
    row.update(_flatten_group("current", payload.current_hand))
    row.update(_flatten_group("context", payload.context))
    row.update(_flatten_group("user_a", payload.user_history_a))
    row.update(_flatten_group("user_b", payload.user_history_b))
    row.update(_flatten_group("pair", payload.pair_history))
    return row


def _label_row(label: PairHandLabel, benchmark_split: str) -> dict[str, Any]:
    return {
        "example_id": str(label.example_id),
        "dataset_id": label.dataset_id,
        "source_dataset_split": label.dataset_split,
        "benchmark_split": benchmark_split,
        "hand_id": label.hand_id,
        "pair_key": label.pair_key,
        "player_a": label.player_a,
        "player_b": label.player_b,
        "is_collusive": bool(label.is_collusive),
        "collusion_pair_id": label.collusion_pair_id,
        "label_available_at": label.label_available_at.isoformat(),
        "provenance": label.provenance,
    }


def derive_world_split(source_dir: Path, split: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Derive pair snapshots and read their private labels for one source split."""
    split_dir = source_dir / split
    contexts: dict[str, list[EventEnvelope]] = {}
    for raw in _iter_jsonl(split_dir / "events" / "user_context.jsonl"):
        event = validate_event(raw)
        contexts.setdefault(str(event.payload["user_id"]), []).append(event)
    hands = sorted(
        (validate_event(raw) for raw in _iter_jsonl(split_dir / "events" / "hands.jsonl")),
        key=lambda event: (event.occurred_at, str(event.event_id)),
    )
    core = PairFeatureCore()
    pair_events: list[PairFeatureEvent] = []
    for hand in hands:
        enriched: list[PlayerHandContextEvent] = []
        for player in sorted(hand.payload["players"], key=lambda value: value["player_id"]):
            player_id = str(player["player_id"])
            selected = select_context_as_of(
                contexts.get(player_id, []),
                user_id=player_id,
                played_at=hand.occurred_at,
            )
            enriched.append(
                enrich_player_hand(
                    hand,
                    player_id=player_id,
                    context_event=selected,
                    emitted_at=hand.emitted_at,
                )
            )
        pair_events.extend(core.process_many(enriched))

    features = pd.DataFrame(
        [flatten_pair_feature(event, split) for event in pair_events],
        columns=AUDIT_COLUMNS + MODEL_FEATURE_COLUMNS,
    ).sort_values(["played_at", "hand_id", "pair_key"], kind="mergesort")

    labels_root = split_dir / ("private_labels" if split == "challenge" else "labels")
    labels = pd.DataFrame(
        [
            _label_row(PairHandLabel.model_validate(raw), split)
            for raw in _iter_jsonl(labels_root / "pair_labels.jsonl")
        ],
        columns=LABEL_COLUMNS,
    ).sort_values(["hand_id", "pair_key"], kind="mergesort")
    feature_keys = set(zip(features["hand_id"], features["pair_key"]))
    label_keys = set(zip(labels["hand_id"], labels["pair_key"]))
    if feature_keys != label_keys:
        raise ValueError(
            f"feature/label identity mismatch for {split}: "
            f"features={len(feature_keys)} labels={len(label_keys)}"
        )
    availability = features[["hand_id", "pair_key", "played_at"]].merge(
        labels[["hand_id", "pair_key", "label_available_at"]],
        on=["hand_id", "pair_key"],
        how="inner",
        validate="one_to_one",
    )
    if (
        pd.to_datetime(availability["label_available_at"], utc=True)
        < pd.to_datetime(availability["played_at"], utc=True)
    ).any():
        raise ValueError(f"label availability precedes feature time for {split}")
    return features.reset_index(drop=True), labels.reset_index(drop=True)


def _chronological_assignment(features: pd.DataFrame) -> dict[str, str]:
    hands = (
        features[["hand_id", "played_at"]]
        .drop_duplicates()
        .sort_values(["played_at", "hand_id"], kind="mergesort")
    )
    count = len(hands)
    if count < 3:
        raise ValueError("temporal benchmark requires at least three hands")
    train_count = max(1, int(count * 0.70))
    validation_count = max(1, int(count * 0.15))
    if train_count + validation_count >= count:
        train_count = count - 2
        validation_count = 1
    assignments: dict[str, str] = {}
    for index, hand_id in enumerate(hands["hand_id"].astype(str)):
        if index < train_count:
            split = "train"
        elif index < train_count + validation_count:
            split = "validation"
        else:
            split = "test"
        assignments[hand_id] = split
    return assignments


class _UnionFind:
    def __init__(self, values: Iterable[str]) -> None:
        self.parent = {value: value for value in values}

    def find(self, value: str) -> str:
        root = value
        while self.parent[root] != root:
            root = self.parent[root]
        while self.parent[value] != value:
            next_value = self.parent[value]
            self.parent[value] = root
            value = next_value
        return root

    def union(self, left: str, right: str) -> None:
        root_left, root_right = self.find(left), self.find(right)
        if root_left != root_right:
            self.parent[max(root_left, root_right)] = min(root_left, root_right)


def _new_relationship_assignment(
    features: pd.DataFrame,
    labels: pd.DataFrame,
) -> tuple[dict[str, str], dict[str, list[str]]]:
    positive_pairs = sorted(
        set(labels.loc[labels["is_collusive"], "pair_key"].astype(str))
    )
    if not positive_pairs:
        raise ValueError("new-relationship benchmark requires positive pair labels")
    union = _UnionFind(positive_pairs)
    positive_set = set(positive_pairs)
    for _, group in features.groupby("hand_id"):
        present = sorted(set(group["pair_key"].astype(str)) & positive_set)
        for pair_key in present[1:]:
            union.union(present[0], pair_key)
    components: dict[str, list[str]] = {}
    for pair_key in positive_pairs:
        components.setdefault(union.find(pair_key), []).append(pair_key)
    ordered_components = sorted(components.values(), key=lambda values: tuple(values))
    component_splits: list[str]
    if len(ordered_components) == 1:
        component_splits = ["test"]
    elif len(ordered_components) == 2:
        component_splits = ["validation", "test"]
    else:
        train_count = max(1, int(len(ordered_components) * 0.70))
        validation_count = max(1, int(len(ordered_components) * 0.15))
        if train_count + validation_count >= len(ordered_components):
            train_count = len(ordered_components) - 2
            validation_count = 1
        component_splits = (
            ["train"] * train_count
            + ["validation"] * validation_count
            + ["test"]
            * (len(ordered_components) - train_count - validation_count)
        )
    split_by_pair = {
        pair_key: split
        for component, split in zip(ordered_components, component_splits)
        for pair_key in component
    }
    assignments: dict[str, str] = {}
    ordered_hands = (
        features[["hand_id", "played_at"]]
        .drop_duplicates()
        .sort_values(["played_at", "hand_id"], kind="mergesort")
    )
    for hand_id in ordered_hands["hand_id"].astype(str):
        pair_keys = set(
            features.loc[features["hand_id"] == hand_id, "pair_key"].astype(str)
        )
        protected_splits = {split_by_pair[key] for key in pair_keys if key in split_by_pair}
        if len(protected_splits) > 1:
            raise RuntimeError("positive relationship components were not fully connected")
        if protected_splits:
            assignments[hand_id] = next(iter(protected_splits))

    remaining = [
        hand_id
        for hand_id in ordered_hands["hand_id"].astype(str)
        if hand_id not in assignments
    ]
    target_cycle = ["train"] * 14 + ["validation"] * 3 + ["test"] * 3
    for index, hand_id in enumerate(remaining):
        assignments[hand_id] = target_cycle[index % len(target_cycle)]

    protected = {
        split: sorted(pair for pair, target in split_by_pair.items() if target == split)
        for split in ("train", "validation", "test")
    }
    return assignments, protected


def _partition(
    features: pd.DataFrame,
    labels: pd.DataFrame,
    assignments: dict[str, str],
) -> dict[str, tuple[pd.DataFrame, pd.DataFrame]]:
    output: dict[str, tuple[pd.DataFrame, pd.DataFrame]] = {}
    for split in ("train", "validation", "test"):
        hand_ids = {hand for hand, target in assignments.items() if target == split}
        feature_part = features[features["hand_id"].isin(hand_ids)].copy()
        label_part = labels[labels["hand_id"].isin(hand_ids)].copy()
        feature_part["benchmark_split"] = split
        label_part["benchmark_split"] = split
        output[split] = (
            feature_part.sort_values(["played_at", "hand_id", "pair_key"], kind="mergesort"),
            label_part.sort_values(["hand_id", "pair_key"], kind="mergesort"),
        )
    return output


def _write_frame(path: Path, frame: pd.DataFrame) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(path, index=False, engine="pyarrow", compression="zstd")
    return {"file": str(path), "rows": len(frame), "sha256": _sha256(path)}


def _write_benchmark(
    root: Path,
    name: str,
    partitions: dict[str, tuple[pd.DataFrame, pd.DataFrame]],
    artifacts: dict[str, str],
) -> dict[str, Any]:
    result: dict[str, Any] = {"splits": {}}
    for split, (features, labels) in partitions.items():
        split_dir = root / "benchmarks" / name / split
        feature_path = split_dir / "features.parquet"
        labels_dir = split_dir / ("private_labels" if split == "challenge" else "labels")
        label_path = labels_dir / "pair_labels.parquet"
        _write_frame(feature_path, features[AUDIT_COLUMNS + MODEL_FEATURE_COLUMNS])
        _write_frame(label_path, labels[LABEL_COLUMNS])
        artifacts[str(feature_path.relative_to(root))] = _sha256(feature_path)
        artifacts[str(label_path.relative_to(root))] = _sha256(label_path)
        result["splits"][split] = {
            "hands": int(features["hand_id"].nunique()),
            "feature_rows": len(features),
            "label_rows": len(labels),
            "positive_rows": int(labels["is_collusive"].sum()),
            "population_players": len(set(features["player_a"]) | set(features["player_b"])),
            "population_sha256": _population_hash(
                list(features["player_a"].astype(str))
                + list(features["player_b"].astype(str))
            ),
        }
    return result


def _write_dgx_exports(
    root: Path,
    benchmarks: dict[str, dict[str, tuple[pd.DataFrame, pd.DataFrame]]],
    artifacts: dict[str, str],
) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for benchmark, partitions in benchmarks.items():
        result[benchmark] = {}
        for split in ("train", "validation", "test"):
            if split not in partitions:
                continue
            features, labels = partitions[split]
            targets = labels[["hand_id", "pair_key", "is_collusive"]].rename(
                columns={"is_collusive": "target"}
            )
            joined = features.merge(
                targets,
                on=["hand_id", "pair_key"],
                how="inner",
                validate="one_to_one",
            )
            joined["target"] = joined["target"].astype("int8")
            columns = [
                "event_id",
                "hand_id",
                "pair_key",
                "played_at",
                "benchmark_split",
            ] + MODEL_FEATURE_COLUMNS + ["target"]
            path = root / "dgx" / benchmark / f"{split}.parquet"
            _write_frame(path, joined[columns])
            artifacts[str(path.relative_to(root))] = _sha256(path)
            result[benchmark][split] = {
                "rows": len(joined),
                "positive_rows": int(joined["target"].sum()),
                "sha256": _sha256(path),
            }
    return result


def build_pair_datasets(config: PairDatasetBuildConfig) -> dict[str, Any]:
    source_dir = config.source_dir.resolve()
    output_dir = config.output_dir.resolve()
    source_manifest = assert_training_allowed(source_dir)
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    derived = {split: derive_world_split(source_dir, split) for split in SPLIT_NAMES}
    cold_start = {split: derived[split] for split in SPLIT_NAMES}

    temporal_features, temporal_labels = derived[config.temporal_source_split]
    temporal = _partition(
        temporal_features,
        temporal_labels,
        _chronological_assignment(temporal_features),
    )

    relationship_features, relationship_labels = derived[
        config.new_relationship_source_split
    ]
    relationship_assignment, protected_pairs = _new_relationship_assignment(
        relationship_features, relationship_labels
    )
    new_relationship = _partition(
        relationship_features,
        relationship_labels,
        relationship_assignment,
    )
    train_protected = set(protected_pairs["train"])
    for split in ("validation", "test"):
        if train_protected & set(protected_pairs[split]):
            raise RuntimeError("protected positive pair leaked into training")

    challenge = {"challenge": derived["challenge"]}
    artifacts: dict[str, str] = {}
    benchmark_frames = {
        "cold_start": cold_start,
        "temporal": temporal,
        "new_relationship": new_relationship,
        "challenge": challenge,
    }
    benchmark_manifest = {
        name: _write_benchmark(output_dir, name, partitions, artifacts)
        for name, partitions in benchmark_frames.items()
    }
    benchmark_manifest["temporal"]["source_split"] = config.temporal_source_split
    benchmark_manifest["temporal"]["policy"] = "chronological_70_15_15"
    benchmark_manifest["new_relationship"]["source_split"] = (
        config.new_relationship_source_split
    )
    benchmark_manifest["new_relationship"]["protected_positive_pairs"] = protected_pairs
    benchmark_manifest["new_relationship"]["policy"] = (
        "hand_atomic_positive_pair_component_holdout"
    )

    dgx_manifest = _write_dgx_exports(
        output_dir,
        {
            "cold_start": {
                split: values
                for split, values in cold_start.items()
                if split != "challenge"
            },
            "temporal": temporal,
            "new_relationship": new_relationship,
        },
        artifacts,
    )
    schema = {
        "schema_version": PAIR_DATASET_SCHEMA_VERSION,
        "feature_definition_version": "pair-features-v1",
        "audit_columns": AUDIT_COLUMNS,
        "numeric_feature_columns": MODEL_NUMERIC_FEATURE_COLUMNS,
        "categorical_feature_columns": MODEL_CATEGORICAL_FEATURE_COLUMNS,
        "label_columns": LABEL_COLUMNS,
        "target_column": "target",
        "challenge_labels_public": False,
    }
    schema_path = output_dir / "schema.json"
    schema_path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    artifacts[str(schema_path.relative_to(output_dir))] = _sha256(schema_path)

    manifest = {
        "schema_version": PAIR_DATASET_SCHEMA_VERSION,
        "dataset_id": source_manifest["dataset_id"],
        "source_manifest_sha256": _sha256(source_dir / "manifest.json"),
        "feature_definition_version": "pair-features-v1",
        "point_in_time_policy": "prior-history-only",
        "challenge_labels_public": False,
        "benchmarks": benchmark_manifest,
        "dgx": dgx_manifest,
        "artifacts": dict(sorted(artifacts.items())),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest
