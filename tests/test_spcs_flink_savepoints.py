from __future__ import annotations

import json
import subprocess

import pytest

from scripts import spcs_flink_savepoints as savepoints


def test_job_spec_is_private_controller_with_bounded_resources() -> None:
    spec = json.loads(
        savepoints._job_spec(
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:0123456789ab",
            "poker-flink.ffpe.svc.spcs.internal",
            300,
        )
    )["spec"]

    assert "endpoints" not in spec
    controller = spec["containers"][0]
    assert controller["command"] == ["/bin/bash"]
    assert controller["args"][0] == "-lc"
    assert controller["resources"]["limits"] == {"cpu": 0.5, "memory": "512Mi"}
    assert '"cancel-job":false' in controller["args"][1]
    assert "poker-event-time-context-enrichment-v1" in controller["args"][1]
    assert "poker-pair-features-v1" in controller["args"][1]


def test_controller_script_has_valid_bash_syntax() -> None:
    result = subprocess.run(
        ["bash", "-n"],
        input=savepoints._controller_script(
            "poker-flink.ffpe.svc.spcs.internal", 300
        ),
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    ("image", "dns"),
    [
        ("poker-flink:dev", "poker-flink.ffpe.svc.spcs.internal"),
        (
            "/POKER_ML_DEMO/SPCS/POKER_ML_REPO/poker-flink:dev",
            "https://poker-flink.ffpe.svc.spcs.internal",
        ),
    ],
)
def test_job_spec_rejects_unsafe_identity(image: str, dns: str) -> None:
    with pytest.raises(ValueError):
        savepoints._job_spec(image, dns, 300)


def test_parse_results_requires_exactly_both_named_savepoints() -> None:
    logs = """
noise
POKER_FLINK_SAVEPOINT_RESULT|context|aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa|file:/opt/flink/state/savepoints/savepoint-context
POKER_FLINK_SAVEPOINT_RESULT|pair|bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb|file:///opt/flink/state/savepoints/savepoint-pair
"""

    assert savepoints._parse_results(logs) == {
        "context": {
            "job_id": "a" * 32,
            "location": "file:/opt/flink/state/savepoints/savepoint-context",
        },
        "pair": {
            "job_id": "b" * 32,
            "location": "file:///opt/flink/state/savepoints/savepoint-pair",
        },
    }

    with pytest.raises(ValueError, match="missing savepoint"):
        savepoints._parse_results(logs.splitlines()[2])


@pytest.mark.parametrize(
    ("value", "expected"),
    [(False, False), ("false", False), ("TRUE", True), (1, True)],
)
def test_snowflake_bool(value: object, expected: bool) -> None:
    assert savepoints._snowflake_bool(value) is expected
