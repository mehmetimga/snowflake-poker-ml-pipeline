from __future__ import annotations

import json
import shutil
from collections import Counter
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from pipeline.events import (
    PairFeatureEvent,
    PairHandLabel,
    PlayerHandContextEvent,
    assert_inference_safe,
    validate_event,
)
from pipeline.generator import (
    AlertAcceptanceBuildConfig,
    AlertAcceptanceProfile,
    build_alert_acceptance_pack,
    verify_alert_acceptance_pack,
)
from pipeline.generator.dataset import iter_jsonl
from pipeline.ml.pair_dataset import PairDatasetBuildConfig, build_pair_datasets
from pipeline.ml.pair_train import PairTrainingConfig, train_pair_catboost


@pytest.fixture(scope="module")
def acceptance_pack(
    tmp_path_factory: pytest.TempPathFactory,
) -> tuple[Path, Path, Path]:
    root = tmp_path_factory.mktemp("alert-acceptance")
    benchmark = root / "benchmark"
    assignment = benchmark / "cold_start" / "train" / "hand_assignments.jsonl"
    assignment.parent.mkdir(parents=True)
    assignment.write_text(
        json.dumps(
            {
                "hand_id": "MULTITABLE-COLD-V1-TRAIN-H-00000000",
            }
        )
        + "\n"
    )
    (benchmark / "manifest.json").write_text(
        json.dumps({"schema_version": 1, "dataset_id": "multitable-cold-v1"}) + "\n"
    )
    output = root / "pack"
    profile = AlertAcceptanceProfile.from_json(
        Path("config/generator/multitable-alert-acceptance-v1.json")
    )
    build_alert_acceptance_pack(
        AlertAcceptanceBuildConfig(
            output_dir=output,
            model_dir=Path("models/pair-catboost-full-v2"),
            benchmark_dir=benchmark,
            profile=profile,
        )
    )
    return output, benchmark, root


def _validator(relative: str) -> Draft202012Validator:
    return Draft202012Validator(
        json.loads(Path(relative).read_text()),
        format_checker=FormatChecker(),
    )


def test_alert_acceptance_pack_is_deterministic_and_verifiable(
    acceptance_pack: tuple[Path, Path, Path],
):
    output, benchmark, root = acceptance_pack
    result = verify_alert_acceptance_pack(
        output,
        model_dir=Path("models/pair-catboost-full-v2"),
        benchmark_dir=benchmark,
    )
    assert result["status"] == "passed"
    assert result["hands"] == 16
    assert result["player_context"] == 96
    assert result["pair_features"] == 240
    assert result["expected_model_alerts"] == 14
    assert result["selected_demo_alerts"] == 10
    assert result["benchmark_hand_overlap"] == 0
    assert result["training_allowed"] is False

    second = root / "pack-second"
    second_manifest = build_alert_acceptance_pack(
        AlertAcceptanceBuildConfig(
            output_dir=second,
            model_dir=Path("models/pair-catboost-full-v2"),
            benchmark_dir=benchmark,
            profile=AlertAcceptanceProfile.from_json(
                Path("config/generator/" "multitable-alert-acceptance-v1.json")
            ),
        )
    )
    assert second_manifest == json.loads((output / "manifest.json").read_text())
    for relative, digest in second_manifest["artifacts"].items():
        assert (
            digest
            == json.loads((output / "manifest.json").read_text())["artifacts"][relative]
        )


def test_alert_acceptance_contracts_and_public_boundary(
    acceptance_pack: tuple[Path, Path, Path],
):
    output, _, _ = acceptance_pack
    _validator(
        "schemas/generator/" "poker.multitable-alert-acceptance-profile.v1.schema.json"
    ).validate(json.loads((output / "config.json").read_text()))
    case_validator = _validator(
        "schemas/generator/poker.alert-acceptance-case.v1.schema.json"
    )
    evidence_validator = _validator(
        "schemas/generator/"
        "poker.alert-acceptance-evidence-expectation.v1.schema.json"
    )
    score_validator = _validator(
        "schemas/generator/" "poker.alert-acceptance-score-expectation.v1.schema.json"
    )
    for row in iter_jsonl(output / "private_labels" / "cases.jsonl"):
        case_validator.validate(row)
    for row in iter_jsonl(output / "private_oracle" / "evidence_expectations.jsonl"):
        evidence_validator.validate(row)
    for row in iter_jsonl(output / "private_oracle" / "score_expectations.jsonl"):
        score_validator.validate(row)
    for row in iter_jsonl(output / "private_labels" / "pair_labels.jsonl"):
        PairHandLabel.model_validate(row)

    hands = [
        validate_event(row) for row in iter_jsonl(output / "events" / "hands.jsonl")
    ]
    player_context = [
        PlayerHandContextEvent.model_validate(row)
        for row in iter_jsonl(output / "expected" / "player_context.jsonl")
    ]
    features = [
        PairFeatureEvent.model_validate(row)
        for row in iter_jsonl(output / "expected" / "pair_features.jsonl")
    ]
    for value in (*hands, *player_context, *features):
        assert_inference_safe(value.model_dump(mode="json"))
    assert {int(hand.payload["num_players"]) for hand in hands} == {6}
    assert set(Counter(event.payload.hand_id for event in player_context).values()) == {
        6
    }
    assert set(Counter(event.payload.hand_id for event in features).values()) == {15}
    public_text = (output / "events" / "hands.jsonl").read_text() + (
        output / "snapshots" / "users.jsonl"
    ).read_text()
    assert "scenario_family" not in public_text
    assert "is_collusive" not in public_text
    assert "selected_demo_alert" not in public_text


def test_rule_oracle_covers_positive_and_precise_negative_cases(
    acceptance_pack: tuple[Path, Path, Path],
):
    output, _, _ = acceptance_pack
    expectations = list(
        iter_jsonl(output / "private_oracle" / "evidence_expectations.jsonl")
    )
    keyed = {(row["reason_code"], row["rule_id"]): row for row in expectations}
    repeated = keyed[
        (
            "five-hand-three-directional-sixty-percent-window",
            "pair.repeated-fold-to-partner-wins",
        )
    ]
    assert repeated["expectation"] == "must_fire"
    assert repeated["minimum_firings"] == repeated["maximum_firings"] == 2
    assert len(repeated["qualifying_hand_ids"]) == 2
    assert keyed[("devices-are-distinct", "pair.same-device")]["maximum_firings"] == 0
    assert (
        keyed[("activity-alone-is-not-device-evidence", "pair.same-device")][
            "maximum_firings"
        ]
        == 0
    )
    assert (
        keyed[("activity-alone-is-not-network-evidence", "pair.same-network")][
            "maximum_firings"
        ]
        == 0
    )

    cases = list(iter_jsonl(output / "private_labels" / "cases.jsonl"))
    household = next(
        row for row in cases if row["scenario_family"] == "innocent_household"
    )
    assert household["is_collusive"] is False
    household_device = next(
        row
        for row in expectations
        if row["case_id"] == household["case_id"]
        and row["rule_id"] == "pair.same-device"
    )
    assert household_device["expectation"] == "must_fire"


def test_training_commands_reject_alert_acceptance_before_writing(
    acceptance_pack: tuple[Path, Path, Path],
    tmp_path: Path,
):
    output, _, _ = acceptance_pack
    pair_output = tmp_path / "pair-output"
    with pytest.raises(ValueError, match="prohibited from model training"):
        build_pair_datasets(
            PairDatasetBuildConfig(
                source_dir=output,
                output_dir=pair_output,
            )
        )
    assert not pair_output.exists()

    model_output = tmp_path / "model-output"
    with pytest.raises(ValueError, match="prohibited from model training"):
        train_pair_catboost(
            PairTrainingConfig(
                dataset_dir=output,
                output_dir=model_output,
            )
        )
    assert not model_output.exists()


def test_checker_rejects_artifact_tampering(
    acceptance_pack: tuple[Path, Path, Path],
    tmp_path: Path,
):
    output, benchmark, _ = acceptance_pack
    tampered = tmp_path / "tampered"
    shutil.copytree(output, tampered)
    path = tampered / "private_oracle" / "score_expectations.jsonl"
    path.write_text(path.read_text() + "\n")

    with pytest.raises(
        ValueError,
        match="alert-acceptance artifact hash mismatch",
    ):
        verify_alert_acceptance_pack(
            tampered,
            model_dir=Path("models/pair-catboost-full-v2"),
            benchmark_dir=benchmark,
        )
