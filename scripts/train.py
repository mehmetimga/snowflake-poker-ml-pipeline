"""End-to-end training orchestrator.

Each stage runs in a fresh subprocess to avoid native-library conflicts
between XGBoost/CatBoost/LightGBM, PyTorch, PyTorch-Geometric, and ONNX Runtime
when loaded into the same process.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys


STAGES = [
    ("classical ML + ONNX", "pipeline.ml.train", "train_all"),
    ("LSTM + Transformer", "pipeline.dl.train", "train_sequence_models"),
    ("GNN (VGAE + HGT)", "pipeline.gnn.train", "train_gnn"),
    ("Wide-and-Deep meta-learner", "pipeline.meta.train", "train_meta_learner"),
    ("inference -> ALERTS", "pipeline.inference.scorer", "score_warehouse"),
]
CPU_STAGE_NUMBERS = {1, 5}


def _run_stage(idx: int, title: str, module: str, fn: str) -> None:
    print(f"\n=== stage {idx}: {title} ===", flush=True)
    code = (
        f"from {module} import {fn}\n"
        "from pipeline.warehouse import get_warehouse\n"
        f"{fn}(get_warehouse())\n"
    )
    env = {**os.environ, "PYTHONUNBUFFERED": "1"}
    res = subprocess.run([sys.executable, "-c", code], env=env)
    if res.returncode != 0:
        raise SystemExit(f"Stage {idx} ({title}) exited with code {res.returncode}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile",
        choices=("cpu", "full"),
        default="cpu",
        help="cpu runs classical ML + inference; full also runs DL/GNN/meta",
    )
    parser.add_argument(
        "--only",
        nargs="+",
        type=int,
        help="Run a subset of stages (1-indexed).",
    )
    args = parser.parse_args()
    default_stages = CPU_STAGE_NUMBERS if args.profile == "cpu" else set(range(1, len(STAGES) + 1))
    selected = set(args.only or default_stages)
    for i, (title, module, fn) in enumerate(STAGES, 1):
        if i in selected:
            _run_stage(i, title, module, fn)
    print("\nDone.")


if __name__ == "__main__":
    main()
