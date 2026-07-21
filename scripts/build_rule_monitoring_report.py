#!/usr/bin/env python3
"""Evaluate one Rules v2 window and export dashboard/alert artifacts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.rules.monitoring import build_rule_monitoring_report


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
    report = build_rule_monitoring_report(
        args.baseline,
        args.window,
        args.output,
        args.prometheus_output,
    )
    print(
        json.dumps(
            {
                "window_id": report["window"]["window_id"],
                "status": report["summary"]["status"],
                "rules": report["summary"]["rules"],
                "alerts": report["summary"]["alerts"],
                "output": str(args.output),
                "prometheus_output": str(args.prometheus_output),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
