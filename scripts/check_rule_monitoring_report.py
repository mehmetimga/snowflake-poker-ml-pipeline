#!/usr/bin/env python3
"""Recompute and verify Rules v2 monitoring and Prometheus artifacts."""

from __future__ import annotations

import json
import argparse
from pathlib import Path

from pipeline.rules.monitoring import validate_rule_monitoring_report


def parser() -> argparse.ArgumentParser:
    value = argparse.ArgumentParser(description=__doc__)
    value.add_argument(
        "--baseline",
        type=Path,
        default=Path("models/registry/rule_evaluation_report.json"),
    )
    value.add_argument(
        "--window",
        type=Path,
        default=Path("models/registry/rule_monitoring_window.json"),
    )
    value.add_argument(
        "--output",
        type=Path,
        default=Path("models/registry/rule_monitoring_report.json"),
    )
    value.add_argument(
        "--prometheus-output",
        type=Path,
        default=Path("models/registry/rule_monitoring.prom"),
    )
    return value


def main() -> None:
    args = parser().parse_args()
    result = validate_rule_monitoring_report(
        args.baseline,
        args.window,
        args.output,
        args.prometheus_output,
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
