"""Leakage-safe benchmark assignments over an immutable multi-table world."""

from __future__ import annotations

import hashlib
import itertools
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping

from pipeline.events import validate_event

from .dataset import SPLIT_NAMES


BENCHMARK_SCHEMA_VERSION = 1
BENCHMARK_NAMES = ("cold_start", "temporal", "new_relationship", "challenge")
SCORED_SPLITS = ("train", "validation", "test")
_ASSIGNMENT_FIELDS = {
    "schema_version",
    "dataset_id",
    "benchmark",
    "benchmark_split",
    "source_dataset_split",
    "hand_id",
    "played_at",
    "player_ids",
}
_FORBIDDEN_ASSIGNMENT_FIELDS = {
    "is_collusive",
    "collusion_pair_id",
    "scenario_family",
    "case_id",
    "group_id",
    "target",
    "label",
}


def _json_line(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _set_hash(values: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(set(values))).encode("utf-8")).hexdigest()


def _pair_key(left: str, right: str) -> str:
    player_a, player_b = sorted((left, right))
    return f"{player_a}:{player_b}"


@dataclass(frozen=True)
class MultiTableBenchmarkConfig:
    source_dir: Path
    output_dir: Path
    temporal_source_split: str = "train"
    new_relationship_source_split: str = "train"

    def __post_init__(self) -> None:
        if self.temporal_source_split not in SPLIT_NAMES:
            raise ValueError("invalid temporal source split")
        if self.new_relationship_source_split not in SPLIT_NAMES:
            raise ValueError("invalid new-relationship source split")
        if self.source_dir.resolve() == self.output_dir.resolve():
            raise ValueError("benchmark output must differ from its source")


@dataclass(frozen=True)
class HandIndexRow:
    dataset_id: str
    source_split: str
    hand_id: str
    played_at: datetime
    player_ids: tuple[str, ...]

    def to_assignment(
        self,
        *,
        benchmark: str,
        benchmark_split: str,
    ) -> dict[str, Any]:
        return {
            "schema_version": BENCHMARK_SCHEMA_VERSION,
            "dataset_id": self.dataset_id,
            "benchmark": benchmark,
            "benchmark_split": benchmark_split,
            "source_dataset_split": self.source_split,
            "hand_id": self.hand_id,
            "played_at": self.played_at.isoformat().replace("+00:00", "Z"),
            "player_ids": list(self.player_ids),
        }

    @property
    def pair_keys(self) -> set[str]:
        return {
            _pair_key(left, right)
            for left, right in itertools.combinations(self.player_ids, 2)
        }


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

    def union_many(self, values: Iterable[str]) -> None:
        ordered = tuple(dict.fromkeys(values))
        for value in ordered[1:]:
            self.union(ordered[0], value)

    def union(self, left: str, right: str) -> None:
        root_left = self.find(left)
        root_right = self.find(right)
        if root_left != root_right:
            self.parent[max(root_left, root_right)] = min(
                root_left,
                root_right,
            )


def _load_hands(
    source_dir: Path,
    dataset_id: str,
) -> dict[str, list[HandIndexRow]]:
    output: dict[str, list[HandIndexRow]] = {}
    seen: set[str] = set()
    for split in SPLIT_NAMES:
        rows: list[HandIndexRow] = []
        path = source_dir / split / "events" / "hands.jsonl"
        for raw in _iter_jsonl(path):
            event = validate_event(raw)
            hand_id = str(event.payload["hand_id"])
            if hand_id in seen:
                raise ValueError(f"duplicate source hand_id: {hand_id}")
            seen.add(hand_id)
            players = tuple(
                sorted(str(player["player_id"]) for player in event.payload["players"])
            )
            rows.append(
                HandIndexRow(
                    dataset_id=dataset_id,
                    source_split=split,
                    hand_id=hand_id,
                    played_at=event.occurred_at,
                    player_ids=players,
                )
            )
        output[split] = sorted(
            rows,
            key=lambda value: (value.played_at, value.hand_id),
        )
    return output


def _labels_root(source_dir: Path, split: str) -> Path:
    if split == "challenge":
        raise ValueError("challenge private labels must not be opened by D5")
    return source_dir / split / "labels"


def _load_cases(source_dir: Path, split: str) -> list[dict[str, Any]]:
    path = _labels_root(source_dir, split) / "scenario_cases.jsonl"
    return list(_iter_jsonl(path))


def _verify_source_integrity(
    source_dir: Path,
    source_manifest: Mapping[str, Any],
) -> dict[str, int]:
    verified = 0
    skipped_challenge_private = 0
    for relative, expected in source_manifest["artifacts"].items():
        if relative.startswith("challenge/private_labels/"):
            skipped_challenge_private += 1
            continue
        path = source_dir / relative
        if not path.exists():
            raise FileNotFoundError(path)
        if _sha256(path) != expected:
            raise ValueError(f"source artifact hash mismatch: {relative}")
        verified += 1
    return {
        "verified_artifacts": verified,
        "skipped_challenge_private_artifacts": skipped_challenge_private,
    }


def _case_hand_sets(
    cases: Iterable[Mapping[str, Any]],
) -> list[tuple[str, tuple[str, ...]]]:
    return [
        (
            str(case["case_id"]),
            tuple(str(hand_id) for hand_id in case["hand_ids"]),
        )
        for case in cases
    ]


def _temporal_assignments(
    hands: list[HandIndexRow],
    cases: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, Any]]:
    if len(hands) < 3:
        raise ValueError("temporal benchmark requires at least three hands")
    first_boundary = max(1, int(len(hands) * 0.70))
    second_boundary = min(
        len(hands) - 1,
        max(first_boundary + 1, int(len(hands) * 0.85)),
    )
    assignments: dict[str, str] = {}
    for index, hand in enumerate(hands):
        if index < first_boundary:
            split = "train"
        elif index < second_boundary:
            split = "validation"
        else:
            split = "test"
        assignments[hand.hand_id] = split
    return assignments, {
        "policy": "chronological_70_15_15",
        "train_end_index": first_boundary,
        "validation_end_index": second_boundary,
        "source_case_count": len(cases),
        "continuing_cases_may_cross_time_boundaries": True,
        "warmup_source_splits": {
            "train": [],
            "validation": ["train"],
            "test": ["train", "validation"],
        },
        "warmup_rows_are_scored": False,
    }


def _split_component_counts(count: int) -> dict[str, int]:
    if count < 3:
        raise ValueError(
            "new-relationship benchmark requires at least three "
            "independent protected components"
        )
    train = max(1, int(count * 0.70))
    validation = max(1, int(count * 0.15))
    if train + validation >= count:
        train = count - 2
        validation = 1
    return {
        "train": train,
        "validation": validation,
        "test": count - train - validation,
    }


def _new_relationship_assignments(
    hands: list[HandIndexRow],
    cases: list[dict[str, Any]],
) -> tuple[dict[str, str], dict[str, Any]]:
    by_id = {hand.hand_id: hand for hand in hands}
    union = _UnionFind(by_id)
    for _, hand_ids in _case_hand_sets(cases):
        union.union_many(hand_ids)

    positive_pairs: set[str] = set()
    for case in cases:
        if not bool(case["is_collusive"]):
            continue
        positive_pairs.update(
            _pair_key(left, right)
            for left, right in itertools.combinations(
                (str(value) for value in case["members"]),
                2,
            )
        )
    if not positive_pairs:
        raise ValueError("new-relationship benchmark needs positive groups")

    occurrences: dict[str, list[str]] = {pair_key: [] for pair_key in positive_pairs}
    for hand in hands:
        for pair_key in hand.pair_keys & positive_pairs:
            occurrences[pair_key].append(hand.hand_id)
    for pair_key, hand_ids in occurrences.items():
        if not hand_ids:
            raise RuntimeError(f"protected pair has no source hand: {pair_key}")
        union.union_many(hand_ids)

    component_hands: dict[str, list[str]] = {}
    for hand_id in by_id:
        component_hands.setdefault(union.find(hand_id), []).append(hand_id)
    component_pairs: dict[str, set[str]] = {}
    for pair_key, hand_ids in occurrences.items():
        component_pairs.setdefault(union.find(hand_ids[0]), set()).add(pair_key)

    protected_components = sorted(
        component_pairs,
        key=lambda root: (
            tuple(sorted(component_pairs[root])),
            min(component_hands[root]),
        ),
    )
    target_component_counts = _split_component_counts(len(protected_components))
    component_split: dict[str, str] = {}
    cursor = 0
    for split in SCORED_SPLITS:
        target = target_component_counts[split]
        for root in protected_components[cursor : cursor + target]:
            component_split[root] = split
        cursor += target

    desired_hands = {
        "train": int(len(hands) * 0.70),
        "validation": int(len(hands) * 0.15),
    }
    desired_hands["test"] = (
        len(hands) - desired_hands["train"] - desired_hands["validation"]
    )
    assigned_hands = CounterBySplit()
    for root, split in component_split.items():
        assigned_hands.add(split, len(component_hands[root]))

    remaining = sorted(
        (root for root in component_hands if root not in component_split),
        key=lambda root: (
            min(by_id[hand_id].played_at for hand_id in component_hands[root]),
            min(component_hands[root]),
        ),
    )
    for root in remaining:
        deficits = {
            split: desired_hands[split] - assigned_hands[split]
            for split in SCORED_SPLITS
        }
        split = max(
            SCORED_SPLITS,
            key=lambda value: (deficits[value], -SCORED_SPLITS.index(value)),
        )
        component_split[root] = split
        assigned_hands.add(split, len(component_hands[root]))

    assignments = {hand_id: component_split[union.find(hand_id)] for hand_id in by_id}
    protected_by_split: dict[str, list[str]] = {
        split: sorted(
            pair_key
            for root, target in component_split.items()
            if target == split
            for pair_key in component_pairs.get(root, set())
        )
        for split in SCORED_SPLITS
    }
    return assignments, {
        "policy": "hand_case_and_positive_relationship_component_atomic",
        "protected_components": len(protected_components),
        "protected_pair_counts": {
            split: len(protected_by_split[split]) for split in SCORED_SPLITS
        },
        "protected_pair_sha256": {
            split: _set_hash(protected_by_split[split]) for split in SCORED_SPLITS
        },
        "_protected_pairs": protected_by_split,
    }


class CounterBySplit(dict[str, int]):
    def __init__(self) -> None:
        super().__init__((split, 0) for split in SCORED_SPLITS)

    def add(self, split: str, value: int) -> None:
        self[split] += value


def _partition_rows(
    hands: Iterable[HandIndexRow],
    assignments: Mapping[str, str],
) -> dict[str, list[HandIndexRow]]:
    output = {split: [] for split in SCORED_SPLITS}
    for hand in hands:
        output[assignments[hand.hand_id]].append(hand)
    return output


def _assignment_map(
    partitions: Mapping[str, Iterable[HandIndexRow]],
) -> dict[str, str]:
    result: dict[str, str] = {}
    for split, hands in partitions.items():
        for hand in hands:
            if hand.hand_id in result:
                raise ValueError(f"hand assigned more than once: {hand.hand_id}")
            result[hand.hand_id] = split
    return result


def _players(hands: Iterable[HandIndexRow]) -> set[str]:
    return {player_id for hand in hands for player_id in hand.player_ids}


def _pairs(hands: Iterable[HandIndexRow]) -> set[str]:
    return {pair_key for hand in hands for pair_key in hand.pair_keys}


def _case_crossings(
    assignments: Mapping[str, str],
    cases: Iterable[Mapping[str, Any]],
) -> int:
    return sum(
        len(
            {
                assignments[hand_id]
                for hand_id in case["hand_ids"]
                if hand_id in assignments
            }
        )
        > 1
        for case in cases
    )


def _audit_assignments(
    *,
    source_hands: Mapping[str, list[HandIndexRow]],
    cold_cases: Mapping[str, list[dict[str, Any]]],
    temporal_source_split: str,
    relationship_source_split: str,
    temporal_cases: list[dict[str, Any]],
    relationship_cases: list[dict[str, Any]],
    benchmarks: Mapping[str, Mapping[str, list[HandIndexRow]]],
    policies: Mapping[str, Mapping[str, Any]],
    relationship_private: Mapping[str, Any],
    source_integrity: Mapping[str, int],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    cold = benchmarks["cold_start"]
    cold_populations = {split: _players(hands) for split, hands in cold.items()}
    cold_intersections = {
        f"{left}:{right}": len(cold_populations[left] & cold_populations[right])
        for index, left in enumerate(SPLIT_NAMES)
        for right in SPLIT_NAMES[index + 1 :]
    }
    cold_pairs = {split: _pairs(hands) for split, hands in cold.items()}
    cold_pair_intersections = {
        f"{left}:{right}": len(cold_pairs[left] & cold_pairs[right])
        for index, left in enumerate(SPLIT_NAMES)
        for right in SPLIT_NAMES[index + 1 :]
    }
    checks.append(
        {
            "check_id": "cold_start_player_and_pair_disjoint",
            "status": (
                "passed"
                if all(value == 0 for value in cold_intersections.values())
                and all(value == 0 for value in cold_pair_intersections.values())
                else "failed"
            ),
            "player_intersections": cold_intersections,
            "pair_intersections": cold_pair_intersections,
        }
    )
    cold_groups = {
        split: {str(case["group_id"]) for case in cases}
        for split, cases in cold_cases.items()
    }
    group_intersections = {
        f"{left}:{right}": len(cold_groups[left] & cold_groups[right])
        for index, left in enumerate(SCORED_SPLITS)
        for right in SCORED_SPLITS[index + 1 :]
    }
    checks.append(
        {
            "check_id": "cold_start_group_disjoint",
            "status": (
                "passed"
                if all(value == 0 for value in group_intersections.values())
                else "failed"
            ),
            "public_split_intersections": group_intersections,
            "challenge_inferred_disjoint_from_player_gate": True,
        }
    )

    temporal = benchmarks["temporal"]
    temporal_map = _assignment_map(temporal)
    temporal_order = max(hand.played_at for hand in temporal["train"]) < min(
        hand.played_at for hand in temporal["validation"]
    ) and max(hand.played_at for hand in temporal["validation"]) < min(
        hand.played_at for hand in temporal["test"]
    )
    temporal_case_crossings = _case_crossings(
        temporal_map,
        temporal_cases,
    )
    checks.append(
        {
            "check_id": "temporal_strict_order",
            "status": "passed" if temporal_order else "failed",
            "strict_time_order": temporal_order,
            "continuing_case_cross_split_count": temporal_case_crossings,
            "continuing_cases_allowed": True,
        }
    )

    relationship = benchmarks["new_relationship"]
    relationship_map = _assignment_map(relationship)
    protected = relationship_private["_protected_pairs"]
    pair_intersections = {
        f"{left}:{right}": len(set(protected[left]) & set(protected[right]))
        for index, left in enumerate(SCORED_SPLITS)
        for right in SCORED_SPLITS[index + 1 :]
    }
    pair_occurrence_splits: dict[str, set[str]] = {
        pair_key: set() for values in protected.values() for pair_key in values
    }
    for split, hands in relationship.items():
        for hand in hands:
            for pair_key in hand.pair_keys & set(pair_occurrence_splits):
                pair_occurrence_splits[pair_key].add(split)
    crossing_pairs = sum(len(splits) > 1 for splits in pair_occurrence_splits.values())
    relationship_case_crossings = _case_crossings(
        relationship_map,
        relationship_cases,
    )
    train_players = _players(relationship["train"])
    later_players = _players(relationship["validation"]) | _players(
        relationship["test"]
    )
    known_player_overlap = len(train_players & later_players)
    checks.append(
        {
            "check_id": "new_relationship_protection",
            "status": (
                "passed"
                if all(value == 0 for value in pair_intersections.values())
                and crossing_pairs == 0
                and relationship_case_crossings == 0
                and known_player_overlap > 0
                else "failed"
            ),
            "protected_pair_intersections": pair_intersections,
            "protected_pairs_crossing_splits": crossing_pairs,
            "case_cross_split_count": relationship_case_crossings,
            "known_player_overlap": known_player_overlap,
        }
    )

    expected_hands = {
        "cold_start": {
            hand.hand_id for hands in source_hands.values() for hand in hands
        },
        "temporal": {hand.hand_id for hand in source_hands[temporal_source_split]},
        "new_relationship": {
            hand.hand_id for hand in source_hands[relationship_source_split]
        },
        "challenge": {hand.hand_id for hand in source_hands["challenge"]},
    }
    assignment_contract_passed = True
    assignment_integrity: dict[str, Any] = {}
    for benchmark, partitions in benchmarks.items():
        assigned_ids: list[str] = []
        for split, hands in partitions.items():
            for hand in hands:
                row = hand.to_assignment(
                    benchmark="audit",
                    benchmark_split=split,
                )
                assignment_contract_passed &= set(row) == _ASSIGNMENT_FIELDS
                assignment_contract_passed &= not (
                    set(row) & _FORBIDDEN_ASSIGNMENT_FIELDS
                )
                assigned_ids.append(hand.hand_id)
        assigned_set = set(assigned_ids)
        duplicates = len(assigned_ids) - len(assigned_set)
        missing = expected_hands[benchmark] - assigned_set
        unexpected = assigned_set - expected_hands[benchmark]
        assignment_contract_passed &= not duplicates
        assignment_contract_passed &= not missing
        assignment_contract_passed &= not unexpected
        assignment_integrity[benchmark] = {
            "assigned_hands": len(assigned_ids),
            "duplicate_hands": duplicates,
            "missing_hands": len(missing),
            "unexpected_hands": len(unexpected),
        }
    checks.append(
        {
            "check_id": "assignment_contract_and_hand_atomicity",
            "status": "passed" if assignment_contract_passed else "failed",
            "forbidden_fields": sorted(_FORBIDDEN_ASSIGNMENT_FIELDS),
            "benchmarks": assignment_integrity,
        }
    )
    evaluation_policy_passed = all(
        policies[benchmark].get("preprocessing_fit_split") == "train"
        and policies[benchmark].get("threshold_selection_split") == "validation"
        and policies[benchmark].get("public_test_access") == "once_after_freeze"
        for benchmark in ("cold_start", "temporal", "new_relationship")
    )
    checks.append(
        {
            "check_id": "train_fit_and_validation_selection_policy",
            "status": "passed" if evaluation_policy_passed else "failed",
            "preprocessing_fit_split": "train",
            "threshold_selection_split": "validation",
            "public_test_access": "once_after_freeze",
        }
    )
    checks.append(
        {
            "check_id": "challenge_isolation",
            "status": "passed",
            "challenge_private_labels_read": False,
            "challenge_labels_copied": False,
        }
    )
    status = (
        "passed" if all(check["status"] == "passed" for check in checks) else "failed"
    )
    return {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "status": status,
        "source_integrity": dict(source_integrity),
        "checks": checks,
    }


def _write_product(
    root: Path,
    *,
    benchmark: str,
    partitions: Mapping[str, list[HandIndexRow]],
    dataset_id: str,
    source_manifest_sha256: str,
    policy: Mapping[str, Any],
) -> tuple[dict[str, Any], list[Path]]:
    product_root = root / benchmark
    product_root.mkdir(parents=True, exist_ok=True)
    artifacts: dict[str, str] = {}
    split_manifest: dict[str, Any] = {}
    written: list[Path] = []
    for split, hands in partitions.items():
        path = product_root / split / "hand_assignments.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        ordered = sorted(hands, key=lambda hand: (hand.played_at, hand.hand_id))
        with path.open("w") as stream:
            for hand in ordered:
                stream.write(
                    _json_line(
                        hand.to_assignment(
                            benchmark=benchmark,
                            benchmark_split=split,
                        )
                    )
                )
        relative = str(path.relative_to(product_root))
        artifacts[relative] = _sha256(path)
        written.append(path)
        players = _players(ordered)
        split_manifest[split] = {
            "hands": len(ordered),
            "players": len(players),
            "player_sha256": _set_hash(players),
            "hand_sha256": _set_hash(hand.hand_id for hand in ordered),
            "first_played_at": (ordered[0].played_at.isoformat() if ordered else None),
            "last_played_at": (ordered[-1].played_at.isoformat() if ordered else None),
            "assignment_file": relative,
            "assignment_sha256": artifacts[relative],
            "labels_in_product": False,
        }
    manifest = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "benchmark": benchmark,
        "source_manifest_sha256": source_manifest_sha256,
        "labels_copied": False,
        "challenge_private_labels_read": False,
        "policy": dict(policy),
        "splits": split_manifest,
        "artifacts": artifacts,
    }
    manifest_path = product_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    written.append(manifest_path)
    return manifest, written


def _load_assignment_products(
    root: Path,
    source_hands: Mapping[str, list[HandIndexRow]],
) -> dict[str, dict[str, list[HandIndexRow]]]:
    by_id = {hand.hand_id: hand for hands in source_hands.values() for hand in hands}
    output: dict[str, dict[str, list[HandIndexRow]]] = {}
    for benchmark in BENCHMARK_NAMES:
        manifest = json.loads((root / benchmark / "manifest.json").read_text())
        output[benchmark] = {}
        for split, details in manifest["splits"].items():
            rows: list[HandIndexRow] = []
            for raw in _iter_jsonl(root / benchmark / details["assignment_file"]):
                if set(raw) != _ASSIGNMENT_FIELDS:
                    raise ValueError(
                        f"invalid assignment fields for {benchmark}/{split}"
                    )
                if set(raw) & _FORBIDDEN_ASSIGNMENT_FIELDS:
                    raise ValueError("private field in benchmark assignment")
                hand = by_id[str(raw["hand_id"])]
                if (
                    raw["dataset_id"] != hand.dataset_id
                    or raw["source_dataset_split"] != hand.source_split
                    or tuple(raw["player_ids"]) != hand.player_ids
                    or raw["played_at"]
                    != hand.played_at.isoformat().replace("+00:00", "Z")
                    or raw["benchmark"] != benchmark
                    or raw["benchmark_split"] != split
                ):
                    raise ValueError("benchmark assignment does not match source")
                rows.append(hand)
            output[benchmark][split] = rows
    return output


def build_multitable_benchmarks(
    config: MultiTableBenchmarkConfig,
) -> dict[str, Any]:
    source_dir = config.source_dir.resolve()
    output_dir = config.output_dir.resolve()
    source_manifest_path = source_dir / "manifest.json"
    if not source_manifest_path.exists():
        raise FileNotFoundError(source_manifest_path)
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"benchmark output is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    source_manifest = json.loads(source_manifest_path.read_text())
    dataset_id = str(source_manifest["dataset_id"])
    source_manifest_sha256 = _sha256(source_manifest_path)
    source_integrity = _verify_source_integrity(source_dir, source_manifest)
    source_hands = _load_hands(source_dir, dataset_id)

    cold_cases = {split: _load_cases(source_dir, split) for split in SCORED_SPLITS}
    temporal_cases = _load_cases(source_dir, config.temporal_source_split)
    relationship_cases = _load_cases(
        source_dir,
        config.new_relationship_source_split,
    )
    temporal_assignment, temporal_policy = _temporal_assignments(
        source_hands[config.temporal_source_split],
        temporal_cases,
    )
    relationship_assignment, relationship_private = _new_relationship_assignments(
        source_hands[config.new_relationship_source_split],
        relationship_cases,
    )
    relationship_policy = {
        key: value
        for key, value in relationship_private.items()
        if not key.startswith("_")
    }
    benchmarks: dict[str, dict[str, list[HandIndexRow]]] = {
        "cold_start": {split: list(source_hands[split]) for split in SPLIT_NAMES},
        "temporal": _partition_rows(
            source_hands[config.temporal_source_split],
            temporal_assignment,
        ),
        "new_relationship": _partition_rows(
            source_hands[config.new_relationship_source_split],
            relationship_assignment,
        ),
        "challenge": {"challenge": list(source_hands["challenge"])},
    }
    policies = {
        "cold_start": {
            "policy": "source_population_disjoint_splits",
            "preprocessing_fit_split": "train",
            "threshold_selection_split": "validation",
            "public_test_access": "once_after_freeze",
        },
        "temporal": {
            **temporal_policy,
            "source_split": config.temporal_source_split,
            "preprocessing_fit_split": "train",
            "threshold_selection_split": "validation",
            "public_test_access": "once_after_freeze",
        },
        "new_relationship": {
            **relationship_policy,
            "source_split": config.new_relationship_source_split,
            "preprocessing_fit_split": "train",
            "threshold_selection_split": "validation",
            "public_test_access": "once_after_freeze",
        },
        "challenge": {
            "policy": "sealed_source_challenge_assignment_only",
            "label_visibility": "sealed_source_private_labels_not_read",
        },
    }

    written: list[Path] = []
    for benchmark in BENCHMARK_NAMES:
        _, paths = _write_product(
            output_dir,
            benchmark=benchmark,
            partitions=benchmarks[benchmark],
            dataset_id=dataset_id,
            source_manifest_sha256=source_manifest_sha256,
            policy=policies[benchmark],
        )
        written.extend(paths)

    audit = _audit_assignments(
        source_hands=source_hands,
        cold_cases=cold_cases,
        temporal_source_split=config.temporal_source_split,
        relationship_source_split=config.new_relationship_source_split,
        temporal_cases=temporal_cases,
        relationship_cases=relationship_cases,
        benchmarks=benchmarks,
        policies=policies,
        relationship_private=relationship_private,
        source_integrity=source_integrity,
    )
    if audit["status"] != "passed":
        raise RuntimeError("multi-table benchmark leakage audit failed")
    audit_path = output_dir / "leakage_audit.json"
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    written.append(audit_path)

    schema = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "assignment_schema": "poker.multitable-benchmark-assignment.v1",
        "assignment_fields": sorted(_ASSIGNMENT_FIELDS),
        "forbidden_assignment_fields": sorted(_FORBIDDEN_ASSIGNMENT_FIELDS),
        "labels_copied": False,
    }
    schema_path = output_dir / "schema.json"
    schema_path.write_text(json.dumps(schema, indent=2, sort_keys=True) + "\n")
    written.append(schema_path)
    artifacts = {str(path.relative_to(output_dir)): _sha256(path) for path in written}
    manifest = {
        "schema_version": BENCHMARK_SCHEMA_VERSION,
        "dataset_id": dataset_id,
        "source_manifest_sha256": source_manifest_sha256,
        "challenge_private_labels_read": False,
        "labels_copied": False,
        "benchmarks": {
            name: {
                "manifest": f"{name}/manifest.json",
                "manifest_sha256": _sha256(output_dir / name / "manifest.json"),
            }
            for name in BENCHMARK_NAMES
        },
        "leakage_audit": "leakage_audit.json",
        "leakage_audit_sha256": _sha256(audit_path),
        "artifacts": dict(sorted(artifacts.items())),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def verify_multitable_benchmarks(
    root: Path,
    source_dir: Path,
) -> dict[str, Any]:
    root = root.resolve()
    source_dir = source_dir.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    source_manifest_path = source_dir / "manifest.json"
    if _sha256(source_manifest_path) != manifest["source_manifest_sha256"]:
        raise ValueError("benchmark source manifest hash changed")
    for relative, expected in manifest["artifacts"].items():
        path = root / relative
        if not path.exists() or _sha256(path) != expected:
            raise ValueError(f"benchmark artifact hash mismatch: {relative}")

    source_manifest = json.loads(source_manifest_path.read_text())
    source_integrity = _verify_source_integrity(source_dir, source_manifest)
    source_hands = _load_hands(source_dir, str(source_manifest["dataset_id"]))
    benchmarks = _load_assignment_products(root, source_hands)
    policies = {
        benchmark: json.loads((root / benchmark / "manifest.json").read_text())[
            "policy"
        ]
        for benchmark in BENCHMARK_NAMES
    }
    cold_cases = {split: _load_cases(source_dir, split) for split in SCORED_SPLITS}
    temporal_source = json.loads((root / "temporal" / "manifest.json").read_text())[
        "policy"
    ]["source_split"]
    relationship_manifest = json.loads(
        (root / "new_relationship" / "manifest.json").read_text()
    )
    relationship_source = relationship_manifest["policy"]["source_split"]
    temporal_cases = _load_cases(source_dir, temporal_source)
    relationship_cases = _load_cases(source_dir, relationship_source)
    _, relationship_private = _new_relationship_assignments(
        source_hands[relationship_source],
        relationship_cases,
    )
    recomputed = _audit_assignments(
        source_hands=source_hands,
        cold_cases=cold_cases,
        temporal_source_split=temporal_source,
        relationship_source_split=relationship_source,
        temporal_cases=temporal_cases,
        relationship_cases=relationship_cases,
        benchmarks=benchmarks,
        policies=policies,
        relationship_private=relationship_private,
        source_integrity=source_integrity,
    )
    stored = json.loads((root / manifest["leakage_audit"]).read_text())
    if recomputed != stored or stored["status"] != "passed":
        raise ValueError("stored leakage audit does not match recomputation")
    if any(root.glob("**/labels")) or any(root.glob("**/private_labels")):
        raise ValueError("benchmark assignment product must not copy labels")
    return {
        "status": "passed",
        "artifacts": len(manifest["artifacts"]),
        "benchmarks": len(benchmarks),
        "source_hands": sum(len(values) for values in source_hands.values()),
        "challenge_private_labels_read": False,
    }
