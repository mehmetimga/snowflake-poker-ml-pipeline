#!/usr/bin/env python3
"""Validate and deterministically recompute the Rules v2 governance report."""

from __future__ import annotations

import json

from scripts.build_rule_evaluation_report import parser
from pipeline.rules.evaluation import validate_rule_evaluation_report


def main() -> None:
    args = parser().parse_args()
    result = validate_rule_evaluation_report(
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
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
