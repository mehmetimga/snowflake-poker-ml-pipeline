"""Compare persisted challenge alerts with the frozen ground-truth sidecar."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score

from pipeline.generator.dataset import iter_jsonl
from pipeline.warehouse import get_warehouse


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--labels",
        type=Path,
        default=Path("data/datasets/cpu-v1/challenge.labels.jsonl"),
    )
    parser.add_argument("--threshold", type=float, default=0.5)
    parser.add_argument("--output", type=Path, default=Path("models/challenge_metrics.json"))
    args = parser.parse_args()

    labels = pd.DataFrame(iter_jsonl(args.labels))
    if labels.empty:
        raise SystemExit(f"No labels found in {args.labels}")
    labels["hand_id"] = labels["hand_id"].astype(str)
    labels["player_id"] = labels["player_id"].astype(str)

    warehouse = get_warehouse()
    try:
        alerts = warehouse.fetch_df(
            "SELECT hand_id, suspicious_player_id, risk_score FROM ALERTS"
        )
    finally:
        warehouse.close()
    if alerts.empty:
        raise SystemExit("No alerts found. Replay the challenge stream before evaluation.")

    alerts = (
        alerts.rename(columns={"suspicious_player_id": "player_id"})
        .groupby(["hand_id", "player_id"], as_index=False)["risk_score"]
        .max()
    )
    scored = labels.merge(alerts, on=["hand_id", "player_id"], how="left")
    scored["risk_score"] = scored["risk_score"].fillna(0.0).astype(float)
    y_true = scored["is_suspicious"].astype(bool).to_numpy()
    y_pred = (scored["risk_score"].to_numpy() >= args.threshold)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[False, True]).ravel()
    report = {
        "threshold": args.threshold,
        "labeled_player_hands": int(len(scored)),
        "positive_labels": int(y_true.sum()),
        "alerts_matched": int(scored["risk_score"].gt(0).sum()),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"[challenge] {json.dumps(report, sort_keys=True)}")
    print(f"[challenge] wrote {args.output}")


if __name__ == "__main__":
    main()
