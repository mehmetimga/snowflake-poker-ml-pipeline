"""Adapt current model artifacts to the generic candidate registry contract."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.ops.candidate import build_candidate_evidence, write_candidate_evidence
from pipeline.ops.registry import build_registry


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--champion-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    parser.add_argument("--ensemble-dir", type=Path, default=Path("models/pair-ensemble-full-v2"))
    parser.add_argument("--dataset-dir", type=Path, default=Path("data/datasets/pair-full-v2"))
    parser.add_argument("--output-dir", type=Path, default=Path("models/registry"))
    parser.add_argument("--operational-report", type=Path, default=Path("models/registry/operational_report.json"))
    args = parser.parse_args()
    champion_metrics = json.loads((args.champion_dir / "metrics.json").read_text())
    ensemble_metrics = json.loads((args.ensemble_dir / "metrics.json").read_text())
    candidate_dir = args.output_dir / "candidate-inputs"
    champion_path = candidate_dir / "catboost-champion.json"
    ensemble_path = candidate_dir / "oof-stack-candidate.json"
    champion = build_candidate_evidence(
        args.champion_dir,
        args.dataset_dir / "manifest.json",
        model_family="tabular_catboost",
        tenant_id="*",
        product_id="poker",
        benchmark="cold_start",
        metrics_path="metrics.json",
        predictions_path="predictions.parquet",
        metric_name="pr_auc",
        metric_json_path=("reports", "catboost", "test", "pr_auc"),
        quality_gate_json_path=("quality_gate", "promotion_eligible"),
        requested_stage="production",
        private_challenge="passed",
        manual_approval="grandfathered_initial_champion",
        reasons=champion_metrics["quality_gate"]["reasons"],
        scoring_contract_path="scoring_contract.json",
        decision_policy_path="decision_policy.json",
        operational_report_path=args.operational_report,
    )
    ensemble = build_candidate_evidence(
        args.ensemble_dir,
        args.dataset_dir / "manifest.json",
        model_family="hybrid_oof_stack",
        tenant_id="*",
        product_id="poker",
        benchmark="cold_start",
        metrics_path="metrics.json",
        predictions_path="predictions.parquet",
        metric_name="pr_auc",
        metric_json_path=("reports", "test", "pr_auc"),
        quality_gate_json_path=("quality_gate", "promotion_candidate"),
        requested_stage="candidate",
        private_challenge="not_run",
        manual_approval="not_requested",
        reasons=ensemble_metrics["quality_gate"]["reasons"],
        decision_policy_path="decision_policy.json",
    )
    write_candidate_evidence(champion_path, champion)
    write_candidate_evidence(ensemble_path, ensemble)
    registry = build_registry((champion_path, ensemble_path), args.output_dir)
    production = next(entry for entry in registry["entries"] if entry["stage"] == "production")
    candidate = next(
        entry for entry in registry["entries"]
        if entry["run_id"] == ensemble["candidate"]["run_id"]
    )
    print(
        f"[model-registry] production={production['model_name']}:{production['run_id']} "
        f"ensemble_stage={candidate['stage']} entries={len(registry['entries'])}"
    )


if __name__ == "__main__":
    main()
