from __future__ import annotations

import copy
import hashlib
import json
from pathlib import Path

import pytest

from admin.data_access import rule_monitoring_artifacts
from pipeline.rules.monitoring import (
    compute_rule_monitoring_report,
    render_prometheus_metrics,
)


def _seal(value: dict[str, object]) -> dict[str, object]:
    payload = {
        key: item
        for key, item in value.items()
        if key not in {"generated_at", "integrity"}
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    value["integrity"] = {"algorithm": "sha256", "payload_sha256": digest}
    value["generated_at"] = "2026-07-21T12:00:00+00:00"
    return value


def _baseline() -> dict[str, object]:
    return _seal(
        {
            "contract_version": 1,
            "configuration": {
                "evaluation_id": "fixture-evaluation-v1",
                "monitoring": {"maximum_circular_or_unknown_label_rows": 0},
            },
            "dataset": {
                "dataset_id": "fixture-v1",
                "benchmark": "cold_start",
                "split": "test",
                "private_challenge_loaded": False,
            },
            "model": {
                "model_name": "fixture-catboost",
                "run_id": "fixture-run",
                "production_model_changed": False,
            },
            "rollback": {"rollout_id": "fixture-rollout-v1"},
            "rule_results": [
                {
                    "rule_id": "pair.fixture",
                    "rule_version": 1,
                    "rule_owner": "risk-analytics",
                    "runtime": "go-risk-scorer",
                }
            ],
            "monitoring": {
                "window_requirements": {
                    "minimum_labeled_hands": 250,
                    "minimum_positive_labels": 20,
                    "minimum_rule_firings": 20,
                },
                "per_rule_thresholds": [
                    {
                        "rule_id": "pair.fixture",
                        "rule_version": 1,
                        "baseline_firing_rate": 0.1,
                        "allowed_firing_rate": {"minimum": 0.05, "maximum": 0.15},
                        "minimum_labeled_precision": 0.4,
                        "maximum_alert_volume_per_1000_hands": 300.0,
                    }
                ],
            },
        }
    )


def _window(baseline: dict[str, object]) -> dict[str, object]:
    baseline_digest = baseline["integrity"]["payload_sha256"]  # type: ignore[index]
    return _seal(
        {
            "contract_version": 1,
            "window_id": "fixture-window-v1",
            "tenant_id": "tenant-a",
            "product_id": "poker",
            "dataset": {
                "dataset_id": "fixture-v1",
                "benchmark": "cold_start",
                "split": "test",
                "synthetic": True,
            },
            "model": {"model_name": "fixture-catboost", "run_id": "fixture-run"},
            "rollout": {"rollout_id": "fixture-rollout-v1", "mode": "shadow"},
            "baseline": {
                "evaluation_id": "fixture-evaluation-v1",
                "payload_sha256": baseline_digest,
            },
            "interval": {
                "event_start": "2026-07-01T00:00:00+00:00",
                "event_end": "2026-07-07T00:00:00+00:00",
                "label_cutoff_at": "2026-07-14T00:00:00+00:00",
            },
            "counts": {
                "pair_rows": 1000,
                "labeled_hands": 500,
                "independently_labeled_rows": 1000,
                "positive_labels": 100,
                "negative_labels": 900,
                "circular_label_rows": 0,
                "unknown_label_rows": 0,
            },
            "label_provenance_counts": {"analyst_confirmed": 1000},
            "rules": [
                {
                    "rule_id": "pair.fixture",
                    "rule_version": 1,
                    "runtime": "go-risk-scorer",
                    "enabled": True,
                    "firings": 100,
                    "true_positives": 50,
                }
            ],
            "controls": {
                "private_challenge_loaded": False,
                "automatic_enforcement_enabled": False,
            },
        }
    )


def _reseal(value: dict[str, object]) -> dict[str, object]:
    value.pop("integrity", None)
    value.pop("generated_at", None)
    return _seal(value)


def test_stable_eligible_window_is_ok_and_emits_no_alert() -> None:
    baseline = _baseline()
    report = compute_rule_monitoring_report(baseline, _window(baseline))
    assert report["summary"]["status"] == "ok"
    assert report["summary"]["alerts"] == 0
    assert report["rule_results"][0]["eligible"] is True
    assert report["controls"]["thin_windows_pass_quality_gate"] is False


def test_thin_window_is_insufficient_data_not_a_pass_or_alert() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["counts"]["labeled_hands"] = 10  # type: ignore[index]
    window["counts"]["positive_labels"] = 1  # type: ignore[index]
    window["counts"]["negative_labels"] = 999  # type: ignore[index]
    window["rules"][0]["firings"] = 10  # type: ignore[index]
    window["rules"][0]["true_positives"] = 1  # type: ignore[index]
    report = compute_rule_monitoring_report(baseline, _reseal(window))
    assert report["summary"]["status"] == "insufficient_data"
    assert report["summary"]["alerts"] == 0
    assert report["rule_results"][0]["eligible"] is False
    assert len(report["rule_results"][0]["eligibility_reasons"]) == 3


def test_disabled_rule_is_reported_as_disabled_without_quality_alert() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["rules"][0]["enabled"] = False  # type: ignore[index]
    report = compute_rule_monitoring_report(baseline, _reseal(window))
    assert report["summary"]["status"] == "disabled"
    assert report["summary"]["disabled_rules"] == 1
    assert report["summary"]["alerts"] == 0
    assert report["rule_results"][0]["eligible"] is False


def test_synthetic_drift_alert_is_deterministic_and_fully_linked() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["rules"][0]["firings"] = 400  # type: ignore[index]
    window["rules"][0]["true_positives"] = 10  # type: ignore[index]
    window = _reseal(window)
    first = compute_rule_monitoring_report(baseline, window)
    second = compute_rule_monitoring_report(baseline, window)
    assert first == second
    assert first["summary"]["status"] == "critical"
    alert = first["alerts"][0]
    assert alert["tenant_id"] == "tenant-a"
    assert alert["rule_id"] == "pair.fixture"
    assert alert["rule_version"] == 1
    assert alert["rollout_id"] == "fixture-rollout-v1"
    assert alert["model_run_id"] == "fixture-run"
    assert alert["evaluation_id"] == "fixture-evaluation-v1"
    assert alert["window_id"] == "fixture-window-v1"
    assert alert["automatic_rule_disable"] is False
    assert alert["enforcement_authority"] is False
    assert set(alert["reason_codes"]) == {
        "alert_volume_above_baseline_ratio",
        "firing_rate_above_baseline_band",
        "precision_below_baseline_ratio",
    }


def test_circular_label_contamination_is_critical_even_when_counts_are_thin() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["counts"]["independently_labeled_rows"] = 999  # type: ignore[index]
    window["counts"]["negative_labels"] = 899  # type: ignore[index]
    window["counts"]["circular_label_rows"] = 1  # type: ignore[index]
    report = compute_rule_monitoring_report(baseline, _reseal(window))
    assert report["summary"]["status"] == "critical"
    assert report["alerts"][0]["recommended_action"] == (
        "quarantine_labels_and_rebuild_window"
    )


def test_prometheus_export_has_status_lineage_and_alert_reason() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["rules"][0]["firings"] = 400  # type: ignore[index]
    window["rules"][0]["true_positives"] = 10  # type: ignore[index]
    report = compute_rule_monitoring_report(baseline, _reseal(window))
    rendered = render_prometheus_metrics(report)
    assert "poker_rule_monitor_status{" in rendered
    assert 'tenant_id="tenant-a"' in rendered
    assert 'model_run_id="fixture-run"' in rendered
    assert 'reason_code="precision_below_baseline_ratio"' in rendered
    assert "poker_rule_monitor_labeled_hands{" in rendered
    assert "poker_rule_monitor_positive_labels{" in rendered


def test_integrity_and_static_contract_schemas_are_enforced() -> None:
    baseline = _baseline()
    window = _window(baseline)
    window["tenant_id"] = "mutated"
    with pytest.raises(ValueError, match="integrity"):
        compute_rule_monitoring_report(baseline, window)
    window_schema = json.loads(
        Path("schemas/events/poker.rule-monitoring-window.v1.schema.json").read_text()
    )
    alert_schema = json.loads(
        Path("schemas/events/poker.rule-monitoring-alert.v1.schema.json").read_text()
    )
    assert window_schema["properties"]["contract_version"] == {"const": 1}
    assert alert_schema["properties"]["automatic_rule_disable"] == {"const": False}
    assert alert_schema["properties"]["enforcement_authority"] == {"const": False}


def test_admin_loader_reads_available_monitoring_artifacts(tmp_path: Path) -> None:
    (tmp_path / "rule_monitoring_report.json").write_text(
        json.dumps({"contract_version": 1, "summary": {"status": "ok"}})
    )
    (tmp_path / "rule_monitoring_window.json").write_text("not-json")
    artifacts = rule_monitoring_artifacts(tmp_path)
    assert artifacts["report"]["summary"]["status"] == "ok"
    assert "window" not in artifacts
