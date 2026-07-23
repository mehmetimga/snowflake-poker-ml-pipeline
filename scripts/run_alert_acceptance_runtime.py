#!/usr/bin/env python3
"""Run bounded Java/Flink-core and Go/Triton D6 acceptance parity."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.generator import verify_alert_acceptance_pack


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run(
    command: list[str],
    *,
    cwd: Path | None = None,
    environment: dict[str, str] | None = None,
) -> dict[str, Any]:
    completed = subprocess.run(
        command,
        cwd=cwd,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        detail = completed.stderr.strip() or completed.stdout.strip()
        raise RuntimeError(
            f"runtime command failed ({completed.returncode}): "
            f"{' '.join(command)}\n{detail}"
        )
    stdout = completed.stdout.strip()
    start = stdout.find("{")
    if start < 0:
        raise RuntimeError(f"runtime command did not return JSON: {' '.join(command)}")
    try:
        return json.loads(stdout[start:])
    except json.JSONDecodeError as error:
        raise RuntimeError(
            f"runtime command returned invalid JSON: {' '.join(command)}"
        ) from error


def _source_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip() if completed.returncode == 0 else "unavailable"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dataset",
        type=Path,
        default=Path("data/datasets/multitable-alert-acceptance-v1"),
    )
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=Path("models/pair-catboost-full-v2"),
    )
    parser.add_argument(
        "--benchmark-dir",
        type=Path,
        default=Path("data/datasets/multitable-benchmarks-v1"),
    )
    parser.add_argument(
        "--review-policy",
        type=Path,
        default=Path("schemas/policies/review-policy-v1.json"),
    )
    parser.add_argument(
        "--rule-rollout",
        type=Path,
        default=Path("schemas/rules/rule-rollout-v1.json"),
    )
    parser.add_argument(
        "--java-jar",
        type=Path,
        default=Path(
            "streaming/flink-java/pair-features/" "target/pair-features-0.1.0.jar"
        ),
    )
    parser.add_argument(
        "--triton-url",
        default="http://127.0.0.1:18000",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
    )
    parser.add_argument("--skip-go", action="store_true")
    parser.add_argument("--go", default="go")
    parser.add_argument("--timeout-seconds", type=float, default=30.0)
    args = parser.parse_args()

    started = time.monotonic()
    run_started_at = datetime.now(timezone.utc)
    run_id = run_started_at.strftime("alert-acceptance-%Y%m%dT%H%M%SZ")
    output_dir = (
        args.output_dir if args.output_dir is not None else Path("data/runs") / run_id
    ).resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        parser.error(f"--output-dir must be empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    dataset = args.dataset.resolve()
    model_dir = args.model_dir.resolve()
    benchmark_dir = args.benchmark_dir.resolve()
    java_jar = args.java_jar.resolve()
    report_path = output_dir / "runtime-report.json"

    report: dict[str, Any] = {
        "schema_version": 1,
        "run_id": run_id,
        "status": "running",
        "started_at": run_started_at.isoformat().replace("+00:00", "Z"),
        "source_commit": _source_commit(),
        "dataset": str(dataset),
        "model_dir": str(model_dir),
        "bindings": {
            "acceptance_manifest_sha256": _sha256(dataset / "manifest.json"),
            "model_manifest_sha256": _sha256(model_dir / "artifact_manifest.json"),
            "review_policy_sha256": _sha256(args.review_policy.resolve()),
            "rule_rollout_sha256": _sha256(args.rule_rollout.resolve()),
        },
        "phases": {
            "pack_verification": {"status": "not_run"},
            "java_flink_core": {"status": "not_run"},
            "go_triton": {"status": "not_run"},
            "kafka_spcs": {"status": "not_run"},
            "snowflake_sinks": {"status": "not_run"},
            "admin": {"status": "not_run"},
        },
    }
    try:
        pack = verify_alert_acceptance_pack(
            dataset,
            model_dir=model_dir,
            benchmark_dir=benchmark_dir,
            review_policy_path=args.review_policy,
        )
        report["phases"]["pack_verification"] = pack

        if not java_jar.is_file():
            raise FileNotFoundError(
                f"Java pair-feature jar is missing: {java_jar}; "
                "run make multitable-alert-replay-java-build"
            )
        java_output = output_dir / "java-pair-features.jsonl"
        java_report = _run(
            [
                "java",
                "-cp",
                str(java_jar),
                "com.aicampions.poker.features.AlertAcceptanceReplay",
                "--input",
                str(dataset / "expected" / "player_context.jsonl"),
                "--expected",
                str(dataset / "expected" / "pair_features.jsonl"),
                "--output",
                str(java_output),
            ]
        )
        java_report["artifact_sha256"] = _sha256(java_output)
        report["phases"]["java_flink_core"] = java_report

        if args.skip_go:
            report["phases"]["go_triton"] = {
                "status": "not_run",
                "reason": "explicitly skipped",
            }
            report["status"] = "partial"
        else:
            go_output = output_dir / "go-results.jsonl"
            go_report = _run(
                [
                    args.go,
                    "run",
                    "./cmd/alert-acceptance",
                    "--dataset",
                    str(dataset),
                    "--model-dir",
                    str(model_dir),
                    "--review-policy",
                    str(args.review_policy.resolve()),
                    "--rule-rollout",
                    str(args.rule_rollout.resolve()),
                    "--triton-url",
                    args.triton_url,
                    "--output",
                    str(go_output),
                    "--timeout",
                    f"{args.timeout_seconds}s",
                    "--build-version",
                    report["source_commit"][:12],
                ],
                cwd=Path("services/go").resolve(),
                environment={
                    **os.environ,
                    "GOCACHE": "/tmp/snowflake-poker-ml-go-build-cache",
                },
            )
            go_report["artifact_sha256"] = _sha256(go_output)
            report["phases"]["go_triton"] = go_report
            report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        finished = datetime.now(timezone.utc)
        report["finished_at"] = finished.isoformat().replace("+00:00", "Z")
        report["duration_ms"] = int((time.monotonic() - started) * 1_000)
        report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")

    print(
        "[alert-acceptance-runtime] "
        f"status={report['status']} "
        f"java={report['phases']['java_flink_core']['status']} "
        f"go_triton={report['phases']['go_triton']['status']} "
        f"report={report_path}"
    )


if __name__ == "__main__":
    main()
