#!/usr/bin/env python3
"""Build a delayed-label Rules v2 monitoring window from the B5 replay."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline.rules.monitoring import build_monitoring_window


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--baseline",
        type=Path,
        default=Path("models/registry/rule_evaluation_report.json"),
    )
    parser.add_argument(
        "--dataset", type=Path, default=Path("data/datasets/pair-full-v2")
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("models/registry/rule_monitoring_window.json"),
    )
    parser.add_argument("--tenant-id", default="demo")
    parser.add_argument("--product-id", default="poker")
    args = parser.parse_args()
    window = build_monitoring_window(
        args.baseline,
        args.dataset,
        args.output,
        tenant_id=args.tenant_id,
        product_id=args.product_id,
    )
    print(
        json.dumps(
            {
                "window_id": window["window_id"],
                "tenant_id": window["tenant_id"],
                "hands": window["counts"]["labeled_hands"],
                "rules": len(window["rules"]),
                "output": str(args.output),
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
