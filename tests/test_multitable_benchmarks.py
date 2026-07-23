from __future__ import annotations

import itertools
import json
import shutil
from datetime import datetime
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from pipeline.generator import (
    MultiTableBenchmarkConfig,
    MultiTableProfile,
    ScenarioPlan,
    build_multitable_benchmarks,
    build_multitable_dataset,
    verify_multitable_benchmarks,
)
from pipeline.generator.dataset import iter_jsonl


def _profile() -> MultiTableProfile:
    return MultiTableProfile.from_dict(
        {
            "schema_version": 1,
            "profile_id": "multitable-benchmark-test-v1",
            "dataset_id": "multitable-benchmark-test-v1",
            "split_hands": {
                "train": 240,
                "validation": 120,
                "test": 120,
                "challenge": 120,
            },
            "registered_players": 120,
            "daily_active_players": 40,
            "peak_concurrent_players": 20,
            "table_size_counts": {"4": 2, "5": 2, "6": 2},
            "hands_per_table_hour": 60,
            "simulated_day_hours": 1,
            "max_simultaneous_tables": 2,
            "simultaneous_table_distribution": {"1": 0.5, "2": 0.5},
            "seat_rebalance_minutes": 30,
            "minimum_session_minutes": 20,
            "maximum_session_minutes": 60,
            "n_colluding_pairs": 10,
            "seed": 5_991,
        }
    )


def _scenario_plan() -> ScenarioPlan:
    return ScenarioPlan.from_dict(
        {
            "schema_version": 1,
            "scenario_plan_id": "multitable-benchmark-scenarios-v1",
            "positive_hand_rate": 0.04,
            "hard_negative_hand_rate": 0.04,
            "hands_per_case": 2,
            "minimum_cases_per_positive_family": 1,
            "minimum_cases_per_hard_negative_family": 1,
            "positive_family_weights": {
                "soft_play": 0.25,
                "chip_dump": 0.25,
                "squeeze_collude": 0.25,
                "fold_benefit": 0.25,
            },
            "hard_negative_family_weights": {
                "legitimate_multitabler": 0.25,
                "innocent_household": 0.25,
                "shared_network": 0.25,
                "frequent_coplayer": 0.25,
            },
            "ring_cases_per_split": 1,
            "ring_members": 3,
            "seed_offset": 700_113,
        }
    )


@pytest.fixture()
def benchmark_world(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[Path, Path]:
    source = tmp_path / "source"
    output = tmp_path / "benchmarks"
    build_multitable_dataset(
        source,
        _profile(),
        scenario_plan=_scenario_plan(),
    )

    original_open = Path.open

    def guarded_open(path: Path, *args, **kwargs):
        if "challenge/private_labels/" in path.as_posix():
            raise AssertionError(f"challenge truth was opened: {path}")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    build_multitable_benchmarks(
        MultiTableBenchmarkConfig(source_dir=source, output_dir=output)
    )
    assert verify_multitable_benchmarks(output, source)["status"] == "passed"
    return source, output


def _assignment_rows(root: Path, benchmark: str, split: str) -> list[dict]:
    return list(iter_jsonl(root / benchmark / split / "hand_assignments.jsonl"))


def test_benchmark_products_are_label_free_deterministic_and_schema_valid(
    benchmark_world: tuple[Path, Path],
    tmp_path: Path,
):
    source, output = benchmark_world
    second = tmp_path / "benchmarks-second"
    second_manifest = build_multitable_benchmarks(
        MultiTableBenchmarkConfig(source_dir=source, output_dir=second)
    )
    first_manifest = json.loads((output / "manifest.json").read_text())

    assert second_manifest == first_manifest
    assert first_manifest["challenge_private_labels_read"] is False
    assert first_manifest["labels_copied"] is False
    assert not any(
        path.name in {"labels", "private_labels"} for path in output.rglob("*")
    )

    assignment_validator = Draft202012Validator(
        json.loads(
            Path(
                "schemas/generator/"
                "poker.multitable-benchmark-assignment.v1.schema.json"
            ).read_text()
        ),
        format_checker=FormatChecker(),
    )
    audit_validator = Draft202012Validator(
        json.loads(
            Path(
                "schemas/generator/" "poker.multitable-leakage-audit.v1.schema.json"
            ).read_text()
        )
    )
    forbidden = {
        "case_id",
        "collusion_pair_id",
        "group_id",
        "is_collusive",
        "label",
        "scenario_family",
        "target",
    }
    for path in output.glob("**/hand_assignments.jsonl"):
        for row in iter_jsonl(path):
            assignment_validator.validate(row)
            assert not forbidden & set(row)
    audit = json.loads((output / "leakage_audit.json").read_text())
    audit_validator.validate(audit)
    assert audit["status"] == "passed"
    assert all(check["status"] == "passed" for check in audit["checks"])


def test_cold_start_and_temporal_contracts(
    benchmark_world: tuple[Path, Path],
):
    _, output = benchmark_world
    cold_players = {
        split: {
            player_id
            for row in _assignment_rows(output, "cold_start", split)
            for player_id in row["player_ids"]
        }
        for split in ("train", "validation", "test", "challenge")
    }
    for index, left in enumerate(cold_players):
        for right in tuple(cold_players)[index + 1 :]:
            assert cold_players[left].isdisjoint(cold_players[right])

    temporal = {
        split: _assignment_rows(output, "temporal", split)
        for split in ("train", "validation", "test")
    }
    assert {split: len(rows) for split, rows in temporal.items()} == {
        "train": 168,
        "validation": 36,
        "test": 36,
    }
    assert max(row["played_at"] for row in temporal["train"]) < min(
        row["played_at"] for row in temporal["validation"]
    )
    assert max(row["played_at"] for row in temporal["validation"]) < min(
        row["played_at"] for row in temporal["test"]
    )
    assert all(
        row["source_dataset_split"] == "train"
        for rows in temporal.values()
        for row in rows
    )
    policy = json.loads((output / "temporal" / "manifest.json").read_text())["policy"]
    assert policy["warmup_source_splits"] == {
        "train": [],
        "validation": ["train"],
        "test": ["train", "validation"],
    }
    assert policy["warmup_rows_are_scored"] is False
    assert policy["preprocessing_fit_split"] == "train"
    assert policy["threshold_selection_split"] == "validation"


def test_new_relationship_keeps_every_protected_pair_and_case_atomic(
    benchmark_world: tuple[Path, Path],
):
    source, output = benchmark_world
    assignment = {
        row["hand_id"]: split
        for split in ("train", "validation", "test")
        for row in _assignment_rows(output, "new_relationship", split)
    }
    cases = list(iter_jsonl(source / "train" / "labels" / "scenario_cases.jsonl"))
    positive_pairs = {
        tuple(sorted(pair))
        for case in cases
        if case["is_collusive"]
        for pair in itertools.combinations(case["members"], 2)
    }
    assert positive_pairs
    for case in cases:
        assert len({assignment[hand_id] for hand_id in case["hand_ids"]}) == 1

    occurrence_splits = {pair: set() for pair in positive_pairs}
    for event in iter_jsonl(source / "train" / "events" / "hands.jsonl"):
        players = {player["player_id"] for player in event["payload"]["players"]}
        for pair in positive_pairs:
            if set(pair) <= players:
                occurrence_splits[pair].add(assignment[event["payload"]["hand_id"]])
    assert all(len(splits) == 1 for splits in occurrence_splits.values())

    players_by_split = {
        split: {
            player_id
            for row in _assignment_rows(output, "new_relationship", split)
            for player_id in row["player_ids"]
        }
        for split in ("train", "validation", "test")
    }
    assert players_by_split["train"] & (
        players_by_split["validation"] | players_by_split["test"]
    )


def test_checker_rejects_artifact_tampering(
    benchmark_world: tuple[Path, Path],
    tmp_path: Path,
):
    source, output = benchmark_world
    tampered = tmp_path / "tampered"
    shutil.copytree(output, tampered)
    assignment = tampered / "temporal" / "train" / "hand_assignments.jsonl"
    assignment.write_text(assignment.read_text() + "\n")

    with pytest.raises(ValueError, match="benchmark artifact hash mismatch"):
        verify_multitable_benchmarks(tampered, source)


def test_assignment_times_are_parseable(
    benchmark_world: tuple[Path, Path],
):
    _, output = benchmark_world
    for row in _assignment_rows(output, "challenge", "challenge"):
        assert datetime.fromisoformat(row["played_at"].replace("Z", "+00:00"))
