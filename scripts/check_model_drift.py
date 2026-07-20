"""Build a reference and evaluate current model-input/score drift."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from pipeline.ops.drift import build_drift_reference, evaluate_drift


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=Path, default=Path("data/datasets/pair-full-v2"))
    parser.add_argument("--model-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    parser.add_argument("--output-dir", type=Path, default=Path("models/registry"))
    parser.add_argument(
        "--reference-split", choices=("train", "validation"), default="validation",
        help="use a window comparable to the monitored production window",
    )
    args = parser.parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reference_path = args.output_dir / "drift_reference.json"
    reference = build_drift_reference(
        args.dataset, args.model_dir, reference_path,
        reference_split=args.reference_split,
    )
    test = pd.read_parquet(args.dataset / "dgx" / "cold_start" / "test.parquet")
    predictions = pd.read_parquet(args.model_dir / "predictions.parquet")
    test_predictions = predictions[predictions["split"] == "test"]
    aligned = test[["event_id"]].copy()
    aligned["event_id"] = aligned["event_id"].astype(str)
    scores = test_predictions[["event_id", "calibrated_probability"]].copy()
    scores["event_id"] = scores["event_id"].astype(str)
    aligned = aligned.merge(scores, on="event_id", how="left", validate="one_to_one")
    if aligned["calibrated_probability"].isna().any():
        raise RuntimeError("test score population is incomplete")
    metrics = json.loads((args.model_dir / "metrics.json").read_text())
    report = evaluate_drift(
        reference, test, aligned["calibrated_probability"], split="test",
        model_name=metrics["model_name"], model_run_id=metrics["run_id"],
    )
    (args.output_dir / "drift_report.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(
        f"[model-drift] status={report['status']} rows={report['rows']} "
        f"critical={report['summary']['critical_checks']} "
        f"warning={report['summary']['warning_checks']} "
        f"score_psi={report['score']['psi']:.6f}"
    )


if __name__ == "__main__":
    main()
