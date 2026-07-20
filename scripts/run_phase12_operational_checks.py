"""Run and record replay, recovery, load, race, security, and audit checks."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_check(check_id: str, command: list[str], cwd: Path) -> dict[str, Any]:
    started = time.perf_counter()
    result = subprocess.run(command, cwd=cwd, text=True, capture_output=True)
    output = (result.stdout + result.stderr).strip()
    return {
        "check_id": check_id,
        "command": command,
        "cwd": str(cwd),
        "passed": result.returncode == 0,
        "return_code": result.returncode,
        "duration_seconds": time.perf_counter() - started,
        "output_tail": output[-4000:],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, default=Path("models/registry/operational_report.json"))
    parser.add_argument("--model-dir", type=Path, default=Path("models/pair-catboost-full-v2"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    go_root = root / "services" / "go"
    model_dir = args.model_dir.resolve()
    artifact_manifest_path = model_dir / "artifact_manifest.json"
    artifact_manifest = json.loads(artifact_manifest_path.read_text())
    checks = [
        run_check(
            "promoted_artifact_contract",
            ["go", "run", "./cmd/risk-contract-check", "--model-dir", str(model_dir)],
            go_root,
        ),
        run_check("go_contract_replay_recovery_load_security", ["go", "test", "./..."], go_root),
        run_check("go_race_detector", ["go", "test", "-race", "./internal/risk", "./internal/stream"], go_root),
        run_check(
            "go_score_benchmark",
            ["go", "test", "-run", "^$", "-bench", "^BenchmarkScoreHand$", "-benchtime=1s", "./internal/risk"],
            go_root,
        ),
        run_check(
            "python_governance_contracts",
            [
                sys.executable, "-m", "pytest", "-q",
                "tests/test_pair_ensemble.py", "tests/test_model_ops.py",
                "tests/test_snowflake_warehouse.py",
            ],
            root,
        ),
    ]
    report = {
        "schema_version": 1,
        "generated_at": datetime.now(tz=timezone.utc).isoformat(),
        "platform": platform.platform(),
        "model_name": artifact_manifest["model_name"],
        "model_run_id": artifact_manifest["run_id"],
        "artifact_manifest_sha256": sha256(artifact_manifest_path),
        "passed": all(check["passed"] for check in checks),
        "acceptance_coverage": [
            "end_to_end_contract", "replay", "restart_recovery", "publish_before_commit",
            "concurrent_load", "race_detection", "tenant_isolation", "tenant_allowlist",
            "artifact_integrity", "artifact_run_binding", "audit_scope",
        ],
        "checks": checks,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    for check in checks:
        print(
            f"[phase12-operational] {check['check_id']} "
            f"passed={check['passed']} seconds={check['duration_seconds']:.3f}"
        )
    print(f"[phase12-operational] passed={report['passed']} report={args.output}")
    if not report["passed"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
