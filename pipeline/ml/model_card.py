"""Hash-bound model-card evidence for the active poker risk champion."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from pipeline.ml.scenario_holdout import SCENARIO_HOLDOUT_CONTRACT_VERSION
from pipeline.ml.seed_stability import SEED_STABILITY_CONTRACT_VERSION
from pipeline.ml.stability import STABILITY_CONTRACT_VERSION, sha256
from pipeline.ops.registry import validate_registry, verify_artifact_bundle


MODEL_CARD_CONTRACT_VERSION = 1


@dataclass(frozen=True)
class ModelCardGovernance:
    owner: str = "poker-ml-platform"
    review_date: str = "2026-07-20"

    def __post_init__(self) -> None:
        if not self.owner.strip():
            raise ValueError("model-card owner must be non-empty")
        try:
            datetime.strptime(self.review_date, "%Y-%m-%d")
        except ValueError as exc:
            raise ValueError("model-card review_date must use YYYY-MM-DD") from exc

    def to_dict(self) -> dict[str, str]:
        return {"owner": self.owner, "review_date": self.review_date}

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ModelCardGovernance":
        return cls(owner=str(raw["owner"]), review_date=str(raw["review_date"]))


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected a JSON object: {path}")
    return value


def _assert_equal(actual: Any, expected: Any, message: str) -> None:
    if actual != expected:
        raise ValueError(message)


def _source(path: Path, logical_path: str) -> dict[str, str]:
    return {"path": logical_path, "sha256": sha256(path)}


def compute_model_card(
    dataset_dir: Path,
    model_dir: Path,
    registry_dir: Path,
    stability_report_path: Path,
    seed_stability_report_path: Path,
    scenario_holdout_report_path: Path,
    *,
    governance: ModelCardGovernance,
) -> dict[str, Any]:
    """Compute the model card after validating every referenced identity."""

    dataset_dir = dataset_dir.resolve()
    model_dir = model_dir.resolve()
    registry_dir = registry_dir.resolve()
    stability_report_path = stability_report_path.resolve()
    seed_stability_report_path = seed_stability_report_path.resolve()
    scenario_holdout_report_path = scenario_holdout_report_path.resolve()
    dataset_manifest_path = dataset_dir / "manifest.json"
    model_manifest_path = model_dir / "artifact_manifest.json"
    metrics_path = model_dir / "metrics.json"
    preprocessing_path = model_dir / "preprocessing.json"
    scoring_contract_path = model_dir / "scoring_contract.json"
    calibration_path = model_dir / "calibration.json"
    decision_policy_path = model_dir / "decision_policy.json"
    registry_path = registry_dir / "registry.json"
    deployment_path = registry_dir / "deployment.json"
    drift_path = registry_dir / "drift_report.json"
    operational_path = registry_dir / "operational_report.json"

    dataset = _load_json(dataset_manifest_path)
    model_manifest = verify_artifact_bundle(model_dir)
    metrics = _load_json(metrics_path)
    preprocessing = _load_json(preprocessing_path)
    scoring_contract = _load_json(scoring_contract_path)
    calibration = _load_json(calibration_path)
    decision_policy = _load_json(decision_policy_path)
    registry = _load_json(registry_path)
    deployment = _load_json(deployment_path)
    drift = _load_json(drift_path)
    operational = _load_json(operational_path)
    stability = _load_json(stability_report_path)
    seed_stability = _load_json(seed_stability_report_path)
    scenario_holdout = _load_json(scenario_holdout_report_path)
    active = validate_registry(registry_dir)["active"]

    if stability.get("contract_version") != STABILITY_CONTRACT_VERSION:
        raise ValueError("model card requires the current stability contract")
    if seed_stability.get("contract_version") != SEED_STABILITY_CONTRACT_VERSION:
        raise ValueError("model card requires the current seed-stability contract")
    if scenario_holdout.get("contract_version") != SCENARIO_HOLDOUT_CONTRACT_VERSION:
        raise ValueError("model card requires the current scenario-holdout contract")
    _assert_equal(
        sha256(dataset_manifest_path),
        metrics.get("dataset_manifest_sha256"),
        "model metrics do not bind the current dataset manifest",
    )
    model_name = str(metrics["model_name"])
    run_id = str(metrics["run_id"])
    for actual, message in (
        (model_manifest.get("model_name"), "model manifest name mismatch"),
        (scoring_contract.get("model_name"), "scoring contract name mismatch"),
        (active.get("model_name"), "active registry model mismatch"),
        (deployment.get("model_name"), "deployment model mismatch"),
        (operational.get("model_name"), "operational model mismatch"),
        (drift.get("model_name"), "drift model mismatch"),
        (stability.get("model", {}).get("model_name"), "stability model mismatch"),
        (
            seed_stability.get("champion", {}).get("model_name"),
            "seed-stability model mismatch",
        ),
        (
            scenario_holdout.get("champion", {}).get("model_name"),
            "scenario-holdout model mismatch",
        ),
    ):
        _assert_equal(actual, model_name, message)
    for actual, message in (
        (model_manifest.get("run_id"), "model manifest run mismatch"),
        (scoring_contract.get("run_id"), "scoring contract run mismatch"),
        (active.get("run_id"), "active registry run mismatch"),
        (deployment.get("model_run_id"), "deployment run mismatch"),
        (operational.get("model_run_id"), "operational run mismatch"),
        (drift.get("model_run_id"), "drift run mismatch"),
        (stability.get("model", {}).get("run_id"), "stability run mismatch"),
        (
            seed_stability.get("champion", {}).get("run_id"),
            "seed-stability run mismatch",
        ),
        (
            scenario_holdout.get("champion", {}).get("run_id"),
            "scenario-holdout run mismatch",
        ),
    ):
        _assert_equal(actual, run_id, message)
    _assert_equal(
        metrics.get("dataset_id"), dataset.get("dataset_id"), "dataset ID mismatch"
    )
    _assert_equal(
        metrics.get("feature_definition_version"),
        dataset.get("feature_definition_version"),
        "feature definition mismatch",
    )
    _assert_equal(
        operational.get("passed"), True, "operational evidence has not passed"
    )
    _assert_equal(
        operational.get("artifact_manifest_sha256"),
        sha256(model_manifest_path),
        "operational evidence belongs to another artifact",
    )
    threshold = float(decision_policy["threshold"])
    for actual, message in (
        (metrics["thresholds"]["catboost"], "metrics threshold mismatch"),
        (deployment.get("decision_threshold"), "deployment threshold mismatch"),
        (stability.get("model", {}).get("threshold"), "stability threshold mismatch"),
    ):
        if float(actual) != threshold:
            raise ValueError(message)

    benchmark = str(metrics["benchmark"])
    benchmark_splits = dataset["benchmarks"][benchmark]["splits"]
    public_splits = {
        split: {
            "rows": int(benchmark_splits[split]["feature_rows"]),
            "hands": int(benchmark_splits[split]["hands"]),
            "positive_rows": int(benchmark_splits[split]["positive_rows"]),
            "population_sha256": benchmark_splits[split]["population_sha256"],
        }
        for split in ("train", "validation", "test")
    }
    ordered_features = scoring_contract["input"]["ordered_features"]
    segment_analysis = stability["segment_analysis"]
    seed_controls = seed_stability.get("leakage_controls", {})
    if seed_controls.get("loaded_splits") != ["train", "validation"] or any(
        seed_controls.get(key) is not False
        for key in (
            "test_dataset_loaded",
            "challenge_dataset_loaded",
            "challenge_labels_loaded",
            "seed_selected_using_evaluation",
        )
    ):
        raise ValueError("model card rejects unsafe seed-stability evidence")
    scenario_controls = scenario_holdout.get("leakage_controls", {})
    if scenario_controls.get("loaded_splits") != [
        "train",
        "validation",
        "test",
    ] or any(
        scenario_controls.get(key) is not False
        for key in (
            "challenge_dataset_loaded",
            "challenge_labels_loaded",
            "scenario_lineage_used_as_model_feature",
            "test_used_for_training_calibration_or_selection",
            "production_model_changed",
        )
    ):
        raise ValueError("model card rejects unsafe scenario-holdout evidence")
    suppressed = [
        f"{item['segment_family']}={item['segment_value']}"
        for item in segment_analysis["segments"]
        if item["reliability"]["status"] == "suppressed"
    ]

    return {
        "contract_version": MODEL_CARD_CONTRACT_VERSION,
        "identity": {
            "model_name": model_name,
            "run_id": run_id,
            "model_family": "CatBoost gradient-boosted decision trees",
            "stage": active["stage"],
            "status": active["status"],
            "benchmark": benchmark,
        },
        "governance": governance.to_dict(),
        "prediction": {
            "unit": "unordered player pair within one completed poker hand",
            "pairs_per_six_player_hand": int(
                decision_policy["pairs_per_six_player_hand"]
            ),
            "output": "Platt-calibrated positive-class pair-risk probability",
            "hand_aggregation": decision_policy["aggregation"]["hand"],
            "intended_use": [
                "prioritize completed hands and player pairs for analyst review",
                "provide governed risk evidence in the poker fraud/collusion workflow",
                "support shadow evaluation and model monitoring",
            ],
            "prohibited_uses": [
                "automatic suspension, punishment, or guilt determination",
                "scoring a hand before all required pair features are complete",
                "use outside poker collusion-risk review without new validation",
                "claiming real-production accuracy from the synthetic benchmark",
            ],
        },
        "dataset": {
            "dataset_id": dataset["dataset_id"],
            "manifest_sha256": sha256(dataset_manifest_path),
            "source_manifest_sha256": dataset["source_manifest_sha256"],
            "feature_definition_version": dataset["feature_definition_version"],
            "point_in_time_policy": dataset["point_in_time_policy"],
            "public_splits": public_splits,
            "private_challenge": {
                "labels_public": False,
                "data_loaded_for_stability_or_model_card": False,
                "metrics_included_in_card": False,
            },
            "label_policy": (
                "Synthetic PokerKit scenario labels; historical features use "
                "prior-hand state only and unresolved human reviews are not labels."
            ),
        },
        "features": {
            "definition_version": metrics["feature_definition_version"],
            "preprocessing_contract_version": preprocessing["contract_version"],
            "input_dtype": scoring_contract["input"]["dtype"],
            "ordered_feature_count": len(ordered_features),
            "ordered_features": ordered_features,
        },
        "model": {
            "training_configuration": metrics["training_config"],
            "best_iteration": metrics["best_iteration"],
            "calibration": calibration,
            "preprocessing": "median-filled numeric plus fixed-vocabulary one-hot categorical",
        },
        "decision_policy": {
            "policy_version": decision_policy["policy_version"],
            "threshold": threshold,
            "validation_max_alert_rate": decision_policy[
                "validation_max_alert_rate"
            ],
            "probability": decision_policy["probability"],
        },
        "evaluation": {
            "split": stability["configuration"]["split"],
            "overall_counts": stability["counts"],
            "overall_metrics": stability["point_metrics"],
            "overall_bootstrap": stability["bootstrap"],
            "segment_analysis": segment_analysis,
            "validation_seed_robustness": {
                "configuration": seed_stability["configuration"],
                "metric_summaries": seed_stability["metric_summaries"],
                "robustness": seed_stability["robustness"],
                "leakage_controls": seed_controls,
            },
            "generator_scenario_holdouts": {
                "summary": scenario_holdout["summary"],
                "generator_seed_holdouts": scenario_holdout[
                    "generator_seed_holdouts"
                ],
                "scenario_family_holdouts": scenario_holdout[
                    "scenario_family_holdouts"
                ],
                "scenario_family_mapping": scenario_holdout[
                    "scenario_family_mapping"
                ],
                "leakage_controls": scenario_controls,
            },
        },
        "limitations": {
            "synthetic_data": (
                "The current evidence is synthetic and cannot estimate real prevalence, "
                "investigator yield, fairness, or enforcement impact."
            ),
            "known_failure_modes": [
                "novel coordination patterns absent from the generator may receive low scores",
                "rare positive labels create wide confidence intervals",
                "missing or stale context can shift scores outside validated behavior",
                "feature or concept drift can invalidate calibration and the alert threshold",
                "pair scores do not establish intent or guilt",
            ],
            "suppressed_segments": suppressed,
        },
        "monitoring": {
            "drift_contract_version": drift["contract_version"],
            "status": drift["status"],
            "score_psi": drift["score"],
            "summary": drift["summary"],
            "reference": "independent validation window",
        },
        "inference": {
            "scoring_contract_version": scoring_contract["contract_version"],
            "input": scoring_contract["input"],
            "output": scoring_contract["output"],
            "triton_model": scoring_contract["batching"]["triton_model"],
            "onnx_latency": metrics["onnx_latency"],
            "operational_verification": {
                "passed": operational["passed"],
                "acceptance_coverage": operational["acceptance_coverage"],
                "platform": operational["platform"],
            },
        },
        "promotion_and_rollback": {
            "registry_scope": registry["scope"],
            "promotion_gates": active["promotion_gates"],
            "automatic_production_promotion": registry["promotion_policy"][
                "automatic_production_promotion"
            ],
            "deployment_id": deployment["deployment_id"],
            "artifact_manifest_sha256": deployment[
                "artifact_manifest_sha256"
            ],
            "rollback": deployment["rollback"],
        },
        "source_artifacts": {
            "dataset_manifest": _source(dataset_manifest_path, "dataset/manifest.json"),
            "model_artifact_manifest": _source(
                model_manifest_path, "model/artifact_manifest.json"
            ),
            "model_metrics": _source(metrics_path, "model/metrics.json"),
            "preprocessing": _source(preprocessing_path, "model/preprocessing.json"),
            "scoring_contract": _source(
                scoring_contract_path, "model/scoring_contract.json"
            ),
            "calibration": _source(calibration_path, "model/calibration.json"),
            "decision_policy": _source(
                decision_policy_path, "model/decision_policy.json"
            ),
            "stability_report": _source(
                stability_report_path, "registry/stability_report.json"
            ),
            "validation_seed_stability": _source(
                seed_stability_report_path,
                "registry/validation_seed_stability.json",
            ),
            "scenario_holdout_report": _source(
                scenario_holdout_report_path,
                "registry/scenario_holdout_report.json",
            ),
            "registry": _source(registry_path, "registry/registry.json"),
            "deployment": _source(deployment_path, "registry/deployment.json"),
            "drift_report": _source(drift_path, "registry/drift_report.json"),
            "operational_report": _source(
                operational_path, "registry/operational_report.json"
            ),
        },
    }


def render_model_card_markdown(card: Mapping[str, Any]) -> str:
    """Render the governed JSON evidence into a compact human-readable card."""

    identity = card["identity"]
    evaluation = card["evaluation"]
    metric = evaluation["overall_metrics"]
    interval = evaluation["overall_bootstrap"]["metrics"]["pr_auc"]
    seed_evidence = evaluation["validation_seed_robustness"]
    seed_pr_auc = seed_evidence["metric_summaries"]["pr_auc"]
    scenario_evidence = evaluation["generator_scenario_holdouts"]
    scenario_summary = scenario_evidence["summary"]
    reliable = sum(
        item["reliability"]["status"] == "reliable"
        for item in evaluation["segment_analysis"]["segments"]
    )
    segments = len(evaluation["segment_analysis"]["segments"])
    lines = [
        f"# Model card: {identity['model_name']}",
        "",
        f"- Run: `{identity['run_id']}`",
        f"- Stage: `{identity['stage']}` / `{identity['status']}`",
        f"- Owner: `{card['governance']['owner']}`",
        f"- Review date: `{card['governance']['review_date']}`",
        f"- Dataset: `{card['dataset']['dataset_id']}`",
        f"- Feature definition: `{card['features']['definition_version']}`",
        "",
        "## Intended use",
        "",
        *[f"- {value}" for value in card["prediction"]["intended_use"]],
        "",
        "## Prohibited uses",
        "",
        *[f"- {value}" for value in card["prediction"]["prohibited_uses"]],
        "",
        "## Public test evidence",
        "",
        f"- Rows: `{evaluation['overall_counts']['rows']}`",
        f"- Hands: `{evaluation['overall_counts']['hands']}`",
        f"- PR-AUC: `{metric['pr_auc']:.6f}`",
        f"- Hand-bootstrap 95% interval: `[{interval['lower']:.6f}, {interval['upper']:.6f}]`",
        f"- Reliable segments: `{reliable}/{segments}`; other segment metrics are suppressed",
        f"- Validation seed status: `{seed_evidence['robustness']['status']}`",
        f"- Validation PR-AUC across seeds: `[{seed_pr_auc['minimum']:.6f}, {seed_pr_auc['maximum']:.6f}]`",
        f"- Unseen scenario-family PR-AUC: `[{scenario_summary['minimum_scenario_holdout_pr_auc']:.6f}, {scenario_summary['maximum_scenario_holdout_pr_auc']:.6f}]`",
        "",
        "## Limitations",
        "",
        f"- {card['limitations']['synthetic_data']}",
        *[f"- {value}" for value in card["limitations"]["known_failure_modes"]],
        "",
        "## Deployment",
        "",
        f"- Deployment: `{card['promotion_and_rollback']['deployment_id']}`",
        f"- Decision threshold: `{card['decision_policy']['threshold']}`",
        f"- Drift status: `{card['monitoring']['status']}`",
        f"- Operational verification: `{card['inference']['operational_verification']['passed']}`",
        "",
        "This Markdown is generated from the hash-bound machine-readable model card.",
        "",
    ]
    return "\n".join(lines)


def build_model_card(
    dataset_dir: Path,
    model_dir: Path,
    registry_dir: Path,
    stability_report_path: Path,
    seed_stability_report_path: Path,
    scenario_holdout_report_path: Path,
    output_path: Path,
    markdown_path: Path,
    *,
    governance: ModelCardGovernance,
) -> dict[str, Any]:
    card = compute_model_card(
        dataset_dir,
        model_dir,
        registry_dir,
        stability_report_path,
        seed_stability_report_path,
        scenario_holdout_report_path,
        governance=governance,
    )
    card["generated_at"] = datetime.now(tz=timezone.utc).isoformat()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(card, indent=2, sort_keys=True) + "\n")
    markdown_path.write_text(render_model_card_markdown(card))
    return card


def validate_model_card(
    dataset_dir: Path,
    model_dir: Path,
    registry_dir: Path,
    stability_report_path: Path,
    seed_stability_report_path: Path,
    scenario_holdout_report_path: Path,
    card_path: Path,
    markdown_path: Path,
) -> dict[str, Any]:
    card = _load_json(card_path)
    if card.get("contract_version") != MODEL_CARD_CONTRACT_VERSION:
        raise ValueError("unsupported model-card contract")
    expected = compute_model_card(
        dataset_dir,
        model_dir,
        registry_dir,
        stability_report_path,
        seed_stability_report_path,
        scenario_holdout_report_path,
        governance=ModelCardGovernance.from_dict(card["governance"]),
    )
    actual = {key: value for key, value in card.items() if key != "generated_at"}
    if actual != expected:
        raise ValueError("model card does not match current governed evidence")
    if markdown_path.read_text() != render_model_card_markdown(card):
        raise ValueError("model-card Markdown does not match machine-readable evidence")
    segments = expected["evaluation"]["segment_analysis"]["segments"]
    return {
        "model_name": expected["identity"]["model_name"],
        "run_id": expected["identity"]["run_id"],
        "owner": expected["governance"]["owner"],
        "review_date": expected["governance"]["review_date"],
        "reliable_segments": sum(
            item["reliability"]["status"] == "reliable" for item in segments
        ),
        "suppressed_segments": sum(
            item["reliability"]["status"] == "suppressed" for item in segments
        ),
    }
