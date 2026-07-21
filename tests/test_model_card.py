from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from pipeline.ml.model_card import (
    ModelCardGovernance,
    build_model_card,
    validate_model_card,
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _write_card_fixture(root: Path) -> tuple[Path, Path, Path, Path, Path, Path]:
    dataset = root / "dataset"
    model = root / "model"
    registry = root / "registry"
    dataset.mkdir()
    model.mkdir()
    registry.mkdir()
    dataset_manifest_path = dataset / "manifest.json"
    splits = {
        name: {
            "feature_rows": rows,
            "hands": hands,
            "positive_rows": positives,
            "population_sha256": f"population-{name}",
        }
        for name, rows, hands, positives in (
            ("train", 300, 20, 8),
            ("validation", 75, 5, 2),
            ("test", 75, 5, 2),
        )
    }
    _write_json(
        dataset_manifest_path,
        {
            "schema_version": 1,
            "dataset_id": "fixture-dataset-v1",
            "source_manifest_sha256": "fixture-source",
            "feature_definition_version": "pair-features-v1",
            "point_in_time_policy": "prior-history-only",
            "challenge_labels_public": False,
            "benchmarks": {"cold_start": {"splits": splits}},
        },
    )
    run_id = "fixture-run"
    model_name = "fixture-catboost"
    threshold = 0.8
    metrics = {
        "model_name": model_name,
        "run_id": run_id,
        "benchmark": "cold_start",
        "dataset_id": "fixture-dataset-v1",
        "dataset_manifest_sha256": _sha256(dataset_manifest_path),
        "feature_definition_version": "pair-features-v1",
        "training_config": {
            "depth": 3,
            "iterations": 20,
            "learning_rate": 0.03,
            "max_alert_rate": 0.2,
            "positive_class_weight": 10.0,
            "random_seed": 42,
        },
        "best_iteration": 10,
        "thresholds": {"catboost": threshold},
        "onnx_latency": {
            "batch_rows": 15,
            "p50_ms": 0.1,
            "p95_ms": 0.2,
            "runs": 5,
            "max_probability_difference": 0.0,
        },
    }
    required_model_files = {
        "metrics.json": metrics,
        "preprocessing.json": {
            "contract_version": 1,
            "numeric_columns": ["feature_a"],
            "categorical_columns": [],
            "output_columns": ["feature_a"],
            "output_dtype": "float32",
        },
        "scoring_contract.json": {
            "contract_version": 1,
            "model_name": model_name,
            "run_id": run_id,
            "input": {
                "dtype": "float32",
                "name": "features",
                "shape": [None, 1],
                "ordered_features": ["feature_a"],
                "preprocessing": "preprocessing.json",
            },
            "output": {
                "dtype": "float32",
                "name": "pair_probabilities",
                "shape": [None, 2],
                "positive_class_index": 1,
            },
            "batching": {"triton_model": "fixture_model", "unit": "hand"},
        },
        "calibration.json": {
            "method": "platt_validation",
            "slope": 1.0,
            "intercept": 0.0,
        },
        "decision_policy.json": {
            "policy_version": 1,
            "threshold": threshold,
            "validation_max_alert_rate": 0.2,
            "probability": "platt_calibrated_positive_class",
            "pairs_per_six_player_hand": 15,
            "aggregation": {
                "hand": "max_pair_probability",
                "player": "max_pair_probability",
            },
        },
    }
    for relative, value in required_model_files.items():
        _write_json(model / relative, value)
    _write_json(
        model / "artifact_manifest.json",
        {
            "model_name": model_name,
            "run_id": run_id,
            "artifacts": {
                relative: _sha256(model / relative)
                for relative in required_model_files
            },
        },
    )
    artifact_manifest_hash = _sha256(model / "artifact_manifest.json")
    interval = {
        "lower": 0.5,
        "median": 0.6,
        "upper": 0.7,
        "effective_samples": 10,
        "point_estimate": 0.6,
    }
    stability_path = registry / "stability_report.json"
    _write_json(
        stability_path,
        {
            "contract_version": 2,
            "configuration": {"split": "test"},
            "model": {
                "model_name": model_name,
                "run_id": run_id,
                "threshold": threshold,
            },
            "counts": {"rows": 75, "hands": 5, "positives": 2, "negatives": 73},
            "point_metrics": {"pr_auc": 0.6},
            "bootstrap": {"metrics": {"pr_auc": interval}},
            "segment_analysis": {
                "definition_version": 1,
                "reliability_floor": {
                    "minimum_hands": 1,
                    "minimum_positives": 1,
                    "minimum_negatives": 1,
                },
                "suppression_policy": "counts_visible_metrics_hidden_below_any_floor",
                "segments": [
                    {
                        "segment_family": "same_network",
                        "segment_value": "true",
                        "definition": "context_same_network == true",
                        "counts": {
                            "rows": 20,
                            "hands": 5,
                            "positives": 2,
                            "negatives": 18,
                        },
                        "reliability": {"status": "reliable", "reasons": []},
                        "point_metrics": {"pr_auc": 0.6},
                        "bootstrap": {"metrics": {"pr_auc": interval}},
                    },
                    {
                        "segment_family": "context_availability",
                        "segment_value": "missing",
                        "definition": "at least one pair member has missing context",
                        "counts": {
                            "rows": 0,
                            "hands": 0,
                            "positives": 0,
                            "negatives": 0,
                        },
                        "reliability": {
                            "status": "suppressed",
                            "reasons": ["hands_below_minimum:0<1"],
                        },
                        "point_metrics": None,
                        "bootstrap": None,
                    },
                ],
            },
        },
    )
    seed_stability_path = registry / "validation_seed_stability.json"
    _write_json(
        seed_stability_path,
        {
            "contract_version": 1,
            "configuration": {
                "benchmark": "cold_start",
                "seeds": [1, 2, 3, 4, 5],
                "maximum_relative_pr_auc_spread": 0.25,
                "minimum_pr_auc_prevalence_multiple": 2.0,
            },
            "champion": {"model_name": model_name, "run_id": run_id},
            "metric_summaries": {
                "pr_auc": {
                    "minimum": 0.5,
                    "maximum": 0.7,
                    "mean": 0.6,
                    "standard_deviation": 0.05,
                    "range": 0.2,
                }
            },
            "robustness": {
                "status": "warning",
                "reasons": ["fixture warning"],
                "seed_selection_performed": False,
                "production_model_changed": False,
            },
            "leakage_controls": {
                "loaded_splits": ["train", "validation"],
                "test_dataset_loaded": False,
                "challenge_dataset_loaded": False,
                "challenge_labels_loaded": False,
                "model_predictions_loaded": False,
                "seed_selected_using_evaluation": False,
            },
        },
    )
    scenario_holdout_path = registry / "scenario_holdout_report.json"
    _write_json(
        scenario_holdout_path,
        {
            "contract_version": 1,
            "champion": {"model_name": model_name, "run_id": run_id},
            "summary": {
                "minimum_scenario_holdout_pr_auc": 0.1,
                "maximum_scenario_holdout_pr_auc": 0.5,
                "mean_scenario_holdout_pr_auc": 0.3,
                "families_evaluated": 4,
                "status": "observed",
                "production_model_changed": False,
                "scenario_model_selected": False,
            },
            "generator_seed_holdouts": [],
            "scenario_family_holdouts": [],
            "scenario_family_mapping": {
                "source": "fixture",
                "assignment": "fixture",
                "families": [
                    "soft_play",
                    "chip_dump",
                    "squeeze_collude",
                    "fold_benefit",
                ],
            },
            "leakage_controls": {
                "loaded_splits": ["train", "validation", "test"],
                "challenge_dataset_loaded": False,
                "challenge_labels_loaded": False,
                "scenario_lineage_used_as_model_feature": False,
                "held_out_family_hands_removed_from_training": True,
                "held_out_family_hands_removed_from_calibration": True,
                "test_used_for_training_calibration_or_selection": False,
                "production_model_changed": False,
            },
        },
    )
    active = {
        "model_name": model_name,
        "run_id": run_id,
        "stage": "production",
        "status": "active",
        "artifact_uri": str(model.resolve()),
        "artifact_manifest_sha256": artifact_manifest_hash,
        "promotion_gates": {
            "artifact_integrity": "passed",
            "public_quality": "passed",
            "private_challenge": "passed",
            "manual_approval": "fixture",
            "operational_verification": "passed",
        },
    }
    _write_json(
        registry / "registry.json",
        {
            "schema_version": 1,
            "scope": {
                "tenant_id": "*",
                "product_id": "poker",
                "benchmark": "cold_start",
            },
            "promotion_policy": {
                "automatic_production_promotion": False,
                "rollback_required": True,
            },
            "entries": [active],
        },
    )
    _write_json(
        registry / "deployment.json",
        {
            "schema_version": 1,
            "deployment_id": "fixture-production",
            "model_name": model_name,
            "model_run_id": run_id,
            "artifact_manifest_sha256": artifact_manifest_hash,
            "decision_threshold": threshold,
            "rollback": {
                "model_name": model_name,
                "model_run_id": run_id,
                "artifact_uri": str(model.resolve()),
            },
        },
    )
    (registry / "audit_log.jsonl").write_text(
        json.dumps(
            {
                "event_type": "deployment.snapshot_recorded",
                "tenant_id": "*",
                "product_id": "poker",
            }
        )
        + "\n"
    )
    _write_json(
        registry / "drift_report.json",
        {
            "contract_version": 1,
            "model_name": model_name,
            "model_run_id": run_id,
            "status": "ok",
            "score": {"psi": 0.01, "status": "ok"},
            "summary": {"ok_checks": 2, "warning_checks": 0, "critical_checks": 0},
        },
    )
    _write_json(
        registry / "operational_report.json",
        {
            "schema_version": 1,
            "model_name": model_name,
            "model_run_id": run_id,
            "artifact_manifest_sha256": artifact_manifest_hash,
            "passed": True,
            "platform": "fixture",
            "acceptance_coverage": ["artifact_integrity", "replay"],
        },
    )
    return (
        dataset,
        model,
        registry,
        stability_path,
        seed_stability_path,
        scenario_holdout_path,
    )


def test_model_card_recomputes_renders_and_detects_source_mutation(
    tmp_path: Path,
) -> None:
    (
        dataset,
        model,
        registry,
        stability,
        seed_stability,
        scenario_holdout,
    ) = _write_card_fixture(tmp_path)
    card_path = registry / "model_card.json"
    markdown_path = registry / "model_card.md"
    card = build_model_card(
        dataset,
        model,
        registry,
        stability,
        seed_stability,
        scenario_holdout,
        card_path,
        markdown_path,
        governance=ModelCardGovernance("risk-science", "2026-07-20"),
    )
    result = validate_model_card(
        dataset,
        model,
        registry,
        stability,
        seed_stability,
        scenario_holdout,
        card_path,
        markdown_path,
    )
    assert result["model_name"] == "fixture-catboost"
    assert result["reliable_segments"] == 1
    assert result["suppressed_segments"] == 1
    assert card["prediction"]["prohibited_uses"]
    assert "# Model card: fixture-catboost" in markdown_path.read_text()

    drift_path = registry / "drift_report.json"
    drift = json.loads(drift_path.read_text())
    drift["status"] = "warning"
    _write_json(drift_path, drift)
    with pytest.raises(ValueError, match="governed evidence"):
        validate_model_card(
            dataset,
            model,
            registry,
            stability,
            seed_stability,
            scenario_holdout,
            card_path,
            markdown_path,
        )
