from __future__ import annotations

import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import streamlit as st

st.set_page_config(page_title="Retrain", layout="wide")
st.title("Retrain")

repo_root = Path(__file__).resolve().parents[2]
script = repo_root / "scripts" / "train.py"

if st.button("Run training pipeline"):
    log_placeholder = st.empty()
    output_lines: list[str] = []
    proc = subprocess.Popen(
        [sys.executable, str(script)],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=str(repo_root),
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        output_lines.append(line.rstrip())
        log_placeholder.code("\n".join(output_lines[-200:]), language="text")
    proc.wait()
    if proc.returncode == 0:
        st.success("Training pipeline complete.")
    else:
        st.error(f"Training failed (exit code {proc.returncode}).")
