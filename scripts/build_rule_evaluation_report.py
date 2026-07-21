#!/usr/bin/env python3
"""Build the deterministic Rules v2 public-test governance report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.rules.evaluation import build_rule_evaluation_report


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument("--dataset", type=Path, default=Path("data/datasets/pair-full-v2"))
    value.add_argument("--model-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    value.add_argument("--source-world", type=Path, default=Path("data/datasets/context-full-v2"))
    value.add_argument("--scenario-report", type=Path, default=Path("models/registry/scenario_holdout_report.json"))
    value.add_argument("--lineage", type=Path, default=Path("models/registry/generator_scenario_lineage.parquet"))
    value.add_argument("--stateless-rules", type=Path, default=Path("schemas/rules/pair-rules-v1.json"))
    value.add_argument("--stateful-rules", type=Path, default=Path("schemas/rules/stateful-pair-rules-v1.json"))
    value.add_argument("--evaluation-config", type=Path, default=Path("schemas/rules/rule-evaluation-v1.json"))
    value.add_argument("--rollout", type=Path, default=Path("schemas/rules/rule-rollout-v1.json"))
    value.add_argument("--output", type=Path, default=Path("models/registry/rule_evaluation_report.json"))
    return value


def main() -> None:
    args = parser().parse_args()
    report = build_rule_evaluation_report(
        args.output,
        dataset_dir=args.dataset,
        model_dir=args.model_dir,
        source_world_dir=args.source_world,
        scenario_report_path=args.scenario_report,
        lineage_path=args.lineage,
        stateless_rules_path=args.stateless_rules,
        stateful_rules_path=args.stateful_rules,
        evaluation_config_path=args.evaluation_config,
        rollout_path=args.rollout,
    )
    print(json.dumps({
        "output": str(args.output),
        "rules": len(report["rule_results"]),
        "rows": report["dataset"]["rows"],
        "hands": report["dataset"]["hands"],
        "rollback_probability_match": report["rollback"]["replay_proof"]["bit_for_bit_probability_match"],
    }, sort_keys=True))


if __name__ == "__main__":
    main()
