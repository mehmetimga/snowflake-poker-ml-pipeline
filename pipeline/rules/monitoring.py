"""Delayed-label Rules v2 monitoring, alerts, and Prometheus export.

The B5 evaluation report is the immutable baseline.  A monitoring window is
eligible only after enough independent labels and rule firings exist.  Thin
windows are explicitly reported as ``insufficient_data`` and never pass a
quality gate by accident.
"""

from __future__ import annotations

import hashlib
import json
import math
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd

from pipeline.ml.stability import sha256


RULE_MONITORING_WINDOW_CONTRACT_VERSION = 1
RULE_MONITORING_REPORT_CONTRACT_VERSION = 1
RULE_MONITORING_ALERT_CONTRACT_VERSION = 1
MONITORING_ALERT_NAMESPACE = uuid.UUID("95c9b135-90b4-5e47-a27f-45d3cce9c277")
STATUS_CODES = {
    "ok": 0,
    "warning": 1,
    "critical": 2,
    "insufficient_data": 3,
    "disabled": 4,
}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise FileNotFoundError(path)
    value = json.loads(path.read_text())
    if not isinstance(value, dict):
        raise ValueError(f"expected JSON object: {path}")
    return value


def _canonical_payload(value: Mapping[str, Any]) -> bytes:
    payload = {
        key: item
        for key, item in value.items()
        if key not in {"generated_at", "integrity"}
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _payload_digest(value: Mapping[str, Any]) -> str:
    return hashlib.sha256(_canonical_payload(value)).hexdigest()


def _validate_integrity(value: Mapping[str, Any], *, owner: str) -> str:
    digest = _payload_digest(value)
    if value.get("integrity") != {
        "algorithm": "sha256",
        "payload_sha256": digest,
    }:
        raise ValueError(f"{owner} payload integrity check failed")
    return digest


def _seal(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result["integrity"] = {
        "algorithm": "sha256",
        "payload_sha256": _payload_digest(result),
    }
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    return result


def _utc_timestamp(value: Any, *, field: str) -> pd.Timestamp:
    result = pd.Timestamp(value)
    if result.tzinfo is None:
        raise ValueError(f"{field} must include a timezone")
    return result.tz_convert("UTC")


def _validate_baseline(baseline: Mapping[str, Any]) -> str:
    if baseline.get("contract_version") != 1:
        raise ValueError("unsupported B5 rule-evaluation baseline")
    digest = _validate_integrity(baseline, owner="rule-evaluation baseline")
    if baseline.get("dataset", {}).get("private_challenge_loaded") is not False:
        raise ValueError("monitoring baseline must not load private challenge data")
    if baseline.get("model", {}).get("production_model_changed") is not False:
        raise ValueError("monitoring baseline cannot change the production model")
    if not baseline.get("monitoring", {}).get("per_rule_thresholds"):
        raise ValueError("monitoring baseline has no per-rule thresholds")
    return digest


def _validate_window(window: Mapping[str, Any]) -> str:
    if window.get("contract_version") != RULE_MONITORING_WINDOW_CONTRACT_VERSION:
        raise ValueError("unsupported rule-monitoring window contract")
    digest = _validate_integrity(window, owner="rule-monitoring window")
    for field in ("window_id", "tenant_id", "product_id"):
        if not str(window.get(field, "")).strip():
            raise ValueError(f"monitoring window requires {field}")
    interval = window.get("interval", {})
    start = _utc_timestamp(interval.get("event_start"), field="event_start")
    end = _utc_timestamp(interval.get("event_end"), field="event_end")
    cutoff = _utc_timestamp(interval.get("label_cutoff_at"), field="label_cutoff_at")
    if end < start or cutoff < end:
        raise ValueError("monitoring window times must satisfy start <= end <= label cutoff")
    counts = window.get("counts", {})
    required_counts = (
        "pair_rows",
        "labeled_hands",
        "independently_labeled_rows",
        "positive_labels",
        "negative_labels",
        "circular_label_rows",
        "unknown_label_rows",
    )
    for field in required_counts:
        if not isinstance(counts.get(field), int) or counts[field] < 0:
            raise ValueError(f"monitoring count {field} must be a non-negative integer")
    if counts["pair_rows"] < counts["independently_labeled_rows"]:
        raise ValueError("independently labeled rows cannot exceed pair rows")
    if counts["positive_labels"] + counts["negative_labels"] != counts[
        "independently_labeled_rows"
    ]:
        raise ValueError("positive and negative labels do not cover independent rows")
    if counts["labeled_hands"] < 1 or counts["pair_rows"] < 1:
        raise ValueError("monitoring window must contain hands and pair rows")
    seen: set[tuple[str, int]] = set()
    for rule in window.get("rules", []):
        identity = (str(rule.get("rule_id", "")), int(rule.get("rule_version", 0)))
        if not identity[0] or identity[1] < 1 or identity in seen:
            raise ValueError("monitoring window rules must have unique versioned identities")
        seen.add(identity)
        firings = rule.get("firings")
        true_positives = rule.get("true_positives")
        if not isinstance(firings, int) or not 0 <= firings <= counts["pair_rows"]:
            raise ValueError(f"invalid firing count for {identity[0]}")
        if not isinstance(true_positives, int) or not 0 <= true_positives <= min(
            firings, counts["positive_labels"]
        ):
            raise ValueError(f"invalid true-positive count for {identity[0]}")
        if not isinstance(rule.get("enabled"), bool):
            raise ValueError(f"rule {identity[0]} requires enabled status")
    if not seen:
        raise ValueError("monitoring window contains no rules")
    return digest


def compute_monitoring_window_from_baseline(
    baseline: Mapping[str, Any],
    evaluation_frame: pd.DataFrame,
    label_frame: pd.DataFrame,
    *,
    tenant_id: str,
    product_id: str,
) -> dict[str, Any]:
    """Create a stable replay window from the B5 public-test observations."""

    baseline_digest = _validate_baseline(baseline)
    required_evaluation = {"played_at", "hand_id", "target"}
    required_labels = {"label_available_at"}
    missing = sorted(required_evaluation - set(evaluation_frame))
    if missing:
        raise ValueError(f"evaluation frame is missing columns: {missing}")
    missing = sorted(required_labels - set(label_frame))
    if missing:
        raise ValueError(f"label frame is missing columns: {missing}")
    event_times = pd.to_datetime(evaluation_frame["played_at"], utc=True, errors="raise")
    label_times = pd.to_datetime(label_frame["label_available_at"], utc=True, errors="raise")
    if len(evaluation_frame) != baseline["dataset"]["rows"]:
        raise ValueError("monitoring replay row count disagrees with B5 baseline")
    if evaluation_frame["hand_id"].astype(str).nunique() != baseline["dataset"]["hands"]:
        raise ValueError("monitoring replay hand count disagrees with B5 baseline")
    enabled = set(
        baseline["rollback"]["replay_proof"]["enabled_rule_ids_before"]
    )
    rules = []
    for result in baseline["rule_results"]:
        point = result["point_metrics"]
        rules.append(
            {
                "rule_id": result["rule_id"],
                "rule_version": int(result["rule_version"]),
                "runtime": result["runtime"],
                "enabled": result["rule_id"] in enabled,
                "firings": int(point["firings"]),
                "true_positives": int(point["true_positives"]),
            }
        )
    label_audit = baseline["label_independence"]
    point = baseline["rule_results"][0]["point_metrics"]
    evaluation_id = baseline["configuration"]["evaluation_id"]
    window = {
        "contract_version": RULE_MONITORING_WINDOW_CONTRACT_VERSION,
        "window_id": f"{evaluation_id}-replay-{tenant_id}",
        "tenant_id": tenant_id,
        "product_id": product_id,
        "dataset": {
            "dataset_id": baseline["dataset"]["dataset_id"],
            "benchmark": baseline["dataset"]["benchmark"],
            "split": baseline["dataset"]["split"],
            "synthetic": True,
        },
        "model": {
            "model_name": baseline["model"]["model_name"],
            "run_id": baseline["model"]["run_id"],
        },
        "rollout": {
            "rollout_id": baseline["rollback"]["rollout_id"],
            "mode": "shadow",
        },
        "baseline": {
            "evaluation_id": evaluation_id,
            "payload_sha256": baseline_digest,
        },
        "interval": {
            "event_start": event_times.min().isoformat(),
            "event_end": event_times.max().isoformat(),
            "label_cutoff_at": label_times.max().isoformat(),
        },
        "counts": {
            "pair_rows": int(len(evaluation_frame)),
            "labeled_hands": int(evaluation_frame["hand_id"].astype(str).nunique()),
            "independently_labeled_rows": int(
                label_audit["included_independent_rows"]
            ),
            "positive_labels": int(point["positives"]),
            "negative_labels": int(point["negatives"]),
            "circular_label_rows": int(label_audit["excluded_circular_rows"]),
            "unknown_label_rows": int(label_audit["unknown_rows"]),
        },
        "label_provenance_counts": dict(
            label_audit["observed_provenance_counts"]
        ),
        "rules": rules,
        "controls": {
            "private_challenge_loaded": False,
            "scenario_lineage_used_as_rule_input": False,
            "automatic_enforcement_enabled": False,
        },
    }
    return window


def build_monitoring_window(
    baseline_path: Path,
    dataset_dir: Path,
    output_path: Path,
    *,
    tenant_id: str = "demo",
    product_id: str = "poker",
) -> dict[str, Any]:
    baseline = _load_json(baseline_path)
    _validate_baseline(baseline)
    evaluation_source = baseline["source_artifacts"]["public_evaluation"]
    labels_source = baseline["source_artifacts"]["public_labels"]
    evaluation_path = dataset_dir / evaluation_source["path"]
    labels_path = dataset_dir / labels_source["path"]
    if sha256(evaluation_path) != evaluation_source["sha256"]:
        raise ValueError("public evaluation hash changed after B5")
    if sha256(labels_path) != labels_source["sha256"]:
        raise ValueError("public label hash changed after B5")
    window = compute_monitoring_window_from_baseline(
        baseline,
        pd.read_parquet(evaluation_path),
        pd.read_parquet(labels_path),
        tenant_id=tenant_id,
        product_id=product_id,
    )
    window["source_artifacts"] = {
        "rule_evaluation_report": {
            "path": baseline_path.name,
            "sha256": sha256(baseline_path),
        },
        "public_evaluation": dict(evaluation_source),
        "public_labels": dict(labels_source),
    }
    sealed = _seal(window)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sealed, indent=2, sort_keys=True) + "\n")
    return sealed


def _identity_maps(
    baseline: Mapping[str, Any], window: Mapping[str, Any]
) -> tuple[dict[tuple[str, int], Mapping[str, Any]], dict[tuple[str, int], Mapping[str, Any]]]:
    baseline_rules = {
        (str(value["rule_id"]), int(value["rule_version"])): value
        for value in baseline["rule_results"]
    }
    window_rules = {
        (str(value["rule_id"]), int(value["rule_version"])): value
        for value in window["rules"]
    }
    if set(baseline_rules) != set(window_rules):
        raise ValueError("monitoring window does not exactly cover baseline rules")
    return baseline_rules, window_rules


def _monitoring_alert(
    *,
    baseline: Mapping[str, Any],
    window: Mapping[str, Any],
    rule: Mapping[str, Any],
    severity: str,
    reason_codes: Sequence[str],
    observed: Mapping[str, Any],
    thresholds: Mapping[str, Any],
) -> dict[str, Any]:
    identity = "|".join(
        (
            window["tenant_id"],
            window["product_id"],
            window["window_id"],
            rule["rule_id"],
            str(rule["rule_version"]),
            baseline["configuration"]["evaluation_id"],
            baseline["integrity"]["payload_sha256"],
            window["rollout"]["rollout_id"],
            window["model"]["model_name"],
            window["model"]["run_id"],
            ",".join(sorted(reason_codes)),
        )
    )
    alert_id = str(uuid.uuid5(MONITORING_ALERT_NAMESPACE, identity))
    label_problem = any("label" in reason for reason in reason_codes)
    return {
        "contract_version": RULE_MONITORING_ALERT_CONTRACT_VERSION,
        "alert_id": alert_id,
        "alert_type": "rule_monitoring",
        "severity": severity,
        "status": "open",
        "tenant_id": window["tenant_id"],
        "product_id": window["product_id"],
        "dataset_id": window["dataset"]["dataset_id"],
        "benchmark": window["dataset"]["benchmark"],
        "window_id": window["window_id"],
        "window_start": window["interval"]["event_start"],
        "window_end": window["interval"]["event_end"],
        "label_cutoff_at": window["interval"]["label_cutoff_at"],
        "evaluation_id": baseline["configuration"]["evaluation_id"],
        "baseline_payload_sha256": baseline["integrity"]["payload_sha256"],
        "rollout_id": window["rollout"]["rollout_id"],
        "model_name": window["model"]["model_name"],
        "model_run_id": window["model"]["run_id"],
        "rule_id": rule["rule_id"],
        "rule_version": int(rule["rule_version"]),
        "reason_codes": sorted(reason_codes),
        "observed": dict(observed),
        "thresholds": dict(thresholds),
        "recommended_action": (
            "quarantine_labels_and_rebuild_window"
            if label_problem
            else "disable_rule_and_investigate"
            if severity == "critical"
            else "investigate_before_rollout"
        ),
        "automatic_rule_disable": False,
        "enforcement_authority": False,
    }


def compute_rule_monitoring_report(
    baseline: Mapping[str, Any],
    window: Mapping[str, Any],
    *,
    source_artifacts: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Evaluate one delayed-label window against its exact B5 baseline."""

    baseline_digest = _validate_baseline(baseline)
    window_digest = _validate_window(window)
    expected_identities = {
        "evaluation_id": baseline["configuration"]["evaluation_id"],
        "baseline_payload_sha256": baseline_digest,
        "dataset_id": baseline["dataset"]["dataset_id"],
        "benchmark": baseline["dataset"]["benchmark"],
        "split": baseline["dataset"]["split"],
        "model_name": baseline["model"]["model_name"],
        "run_id": baseline["model"]["run_id"],
        "rollout_id": baseline["rollback"]["rollout_id"],
    }
    actual_identities = {
        "evaluation_id": window["baseline"]["evaluation_id"],
        "baseline_payload_sha256": window["baseline"]["payload_sha256"],
        "dataset_id": window["dataset"]["dataset_id"],
        "benchmark": window["dataset"]["benchmark"],
        "split": window["dataset"]["split"],
        "model_name": window["model"]["model_name"],
        "run_id": window["model"]["run_id"],
        "rollout_id": window["rollout"]["rollout_id"],
    }
    if actual_identities != expected_identities:
        raise ValueError("monitoring window lineage does not match its B5 baseline")
    baseline_rules, window_rules = _identity_maps(baseline, window)
    threshold_by_rule = {
        (str(value["rule_id"]), int(value["rule_version"])): value
        for value in baseline["monitoring"]["per_rule_thresholds"]
    }
    if set(threshold_by_rule) != set(baseline_rules):
        raise ValueError("B5 monitoring thresholds do not cover every rule")
    requirements = baseline["monitoring"]["window_requirements"]
    counts = window["counts"]
    global_reasons = []
    max_bad_labels = int(
        baseline["configuration"]["monitoring"]
        ["maximum_circular_or_unknown_label_rows"]
    )
    if counts["circular_label_rows"] > max_bad_labels:
        global_reasons.append("circular_label_rows_above_maximum")
    if counts["unknown_label_rows"] > max_bad_labels:
        global_reasons.append("unknown_label_rows_above_maximum")
    rule_results = []
    alerts = []
    for identity in sorted(baseline_rules):
        baseline_rule = baseline_rules[identity]
        rule = window_rules[identity]
        thresholds = threshold_by_rule[identity]
        firings = int(rule["firings"])
        true_positives = int(rule["true_positives"])
        pair_rows = int(counts["pair_rows"])
        hands = int(counts["labeled_hands"])
        positives = int(counts["positive_labels"])
        firing_rate = firings / pair_rows
        precision = true_positives / firings if firings else 0.0
        recall = true_positives / positives if positives else None
        volume = firings * 1000.0 / hands
        observed = {
            "firings": firings,
            "true_positives": true_positives,
            "firing_rate": firing_rate,
            "precision": precision,
            "recall": recall,
            "alert_volume_per_1000_hands": volume,
        }
        reasons = list(global_reasons)
        eligible_reasons = []
        for name, actual, minimum in (
            ("labeled_hands", hands, int(requirements["minimum_labeled_hands"])),
            ("positive_labels", positives, int(requirements["minimum_positive_labels"])),
            ("rule_firings", firings, int(requirements["minimum_rule_firings"])),
        ):
            if actual < minimum:
                eligible_reasons.append(f"{name}_below_minimum:{actual}<{minimum}")
        if not rule["enabled"]:
            status = "disabled"
        elif reasons:
            status = "critical"
        elif eligible_reasons:
            status = "insufficient_data"
        else:
            allowed = thresholds["allowed_firing_rate"]
            if firing_rate < float(allowed["minimum"]):
                reasons.append("firing_rate_below_baseline_band")
            if firing_rate > float(allowed["maximum"]):
                reasons.append("firing_rate_above_baseline_band")
            if precision < float(thresholds["minimum_labeled_precision"]):
                reasons.append("precision_below_baseline_ratio")
            if volume > float(thresholds["maximum_alert_volume_per_1000_hands"]):
                reasons.append("alert_volume_above_baseline_ratio")
            status = "critical" if len(reasons) >= 2 else "warning" if reasons else "ok"
        eligible = status in {"ok", "warning", "critical"}
        result = {
            "rule_id": identity[0],
            "rule_version": identity[1],
            "rule_owner": baseline_rule["rule_owner"],
            "runtime": baseline_rule["runtime"],
            "enabled": bool(rule["enabled"]),
            "status": status,
            "status_code": STATUS_CODES[status],
            "eligible": eligible,
            "eligibility_reasons": eligible_reasons,
            "reason_codes": sorted(reasons),
            "observed": observed,
            "thresholds": dict(thresholds),
        }
        rule_results.append(result)
        if status in {"warning", "critical"}:
            alerts.append(
                _monitoring_alert(
                    baseline=baseline,
                    window=window,
                    rule=rule,
                    severity=status,
                    reason_codes=reasons,
                    observed=observed,
                    thresholds=thresholds,
                )
            )
    statuses = [value["status"] for value in rule_results]
    if "critical" in statuses:
        overall = "critical"
    elif "warning" in statuses:
        overall = "warning"
    elif "insufficient_data" in statuses:
        overall = "insufficient_data"
    elif "disabled" in statuses:
        overall = "disabled"
    else:
        overall = "ok"
    summary = {
        "status": overall,
        "status_code": STATUS_CODES[overall],
        "rules": len(rule_results),
        "ok_rules": statuses.count("ok"),
        "warning_rules": statuses.count("warning"),
        "critical_rules": statuses.count("critical"),
        "insufficient_data_rules": statuses.count("insufficient_data"),
        "disabled_rules": statuses.count("disabled"),
        "alerts": len(alerts),
    }
    return {
        "contract_version": RULE_MONITORING_REPORT_CONTRACT_VERSION,
        "baseline": {
            "evaluation_id": baseline["configuration"]["evaluation_id"],
            "payload_sha256": baseline_digest,
        },
        "window": {
            "window_id": window["window_id"],
            "payload_sha256": window_digest,
            "tenant_id": window["tenant_id"],
            "product_id": window["product_id"],
            "dataset": dict(window["dataset"]),
            "model": dict(window["model"]),
            "rollout": dict(window["rollout"]),
            "interval": dict(window["interval"]),
            "counts": dict(window["counts"]),
        },
        "summary": summary,
        "rule_results": rule_results,
        "alerts": alerts,
        "source_artifacts": dict(source_artifacts or {}),
        "controls": {
            "independent_labels_required": True,
            "thin_windows_pass_quality_gate": False,
            "automatic_rule_disable": False,
            "automatic_enforcement": False,
            "private_challenge_loaded": False,
            "model_probability_changed": False,
        },
    }


def _prometheus_escape(value: Any) -> str:
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _prometheus_labels(values: Mapping[str, Any]) -> str:
    return "{" + ",".join(
        f'{key}="{_prometheus_escape(value)}"' for key, value in sorted(values.items())
    ) + "}"


def render_prometheus_metrics(report: Mapping[str, Any]) -> str:
    """Render a Prometheus textfile for the scheduled monitoring job."""

    if report.get("contract_version") != RULE_MONITORING_REPORT_CONTRACT_VERSION:
        raise ValueError("unsupported rule-monitoring report")
    lines = [
        "# HELP poker_rule_monitor_status Rule monitor status: 0 ok, 1 warning, 2 critical, 3 insufficient_data, 4 disabled.",
        "# TYPE poker_rule_monitor_status gauge",
        "# HELP poker_rule_monitor_eligible Whether delayed-label reliability floors are met.",
        "# TYPE poker_rule_monitor_eligible gauge",
        "# HELP poker_rule_monitor_firing_rate Pair-row rule firing rate in the current window.",
        "# TYPE poker_rule_monitor_firing_rate gauge",
        "# HELP poker_rule_monitor_precision Independently labeled precision in the current window.",
        "# TYPE poker_rule_monitor_precision gauge",
        "# HELP poker_rule_monitor_recall Independently labeled recall in the current window.",
        "# TYPE poker_rule_monitor_recall gauge",
        "# HELP poker_rule_monitor_evidence_per_1000_hands Evidence events per 1000 complete hands.",
        "# TYPE poker_rule_monitor_evidence_per_1000_hands gauge",
    ]
    common = {
        "tenant_id": report["window"]["tenant_id"],
        "product_id": report["window"]["product_id"],
        "window_id": report["window"]["window_id"],
        "evaluation_id": report["baseline"]["evaluation_id"],
        "rollout_id": report["window"]["rollout"]["rollout_id"],
        "model_name": report["window"]["model"]["model_name"],
        "model_run_id": report["window"]["model"]["run_id"],
    }
    counts = report["window"]["counts"]
    common_labels = _prometheus_labels(common)
    lines.extend(
        [
            "# HELP poker_rule_monitor_labeled_hands Complete hands whose delayed labels are available in the window.",
            "# TYPE poker_rule_monitor_labeled_hands gauge",
            f"poker_rule_monitor_labeled_hands{common_labels} {counts['labeled_hands']}",
            "# HELP poker_rule_monitor_independent_label_rows Independently labeled pair rows in the window.",
            "# TYPE poker_rule_monitor_independent_label_rows gauge",
            "poker_rule_monitor_independent_label_rows"
            f"{common_labels} {counts['independently_labeled_rows']}",
            "# HELP poker_rule_monitor_positive_labels Independently confirmed positive pair labels in the window.",
            "# TYPE poker_rule_monitor_positive_labels gauge",
            f"poker_rule_monitor_positive_labels{common_labels} {counts['positive_labels']}",
        ]
    )
    for result in report["rule_results"]:
        labels = {
            **common,
            "rule_id": result["rule_id"],
            "rule_version": result["rule_version"],
            "runtime": result["runtime"],
        }
        encoded = _prometheus_labels(labels)
        observed = result["observed"]
        lines.append(f"poker_rule_monitor_status{encoded} {result['status_code']}")
        lines.append(f"poker_rule_monitor_eligible{encoded} {1 if result['eligible'] else 0}")
        lines.append(f"poker_rule_monitor_firing_rate{encoded} {observed['firing_rate']:.17g}")
        lines.append(f"poker_rule_monitor_precision{encoded} {observed['precision']:.17g}")
        if observed["recall"] is not None and math.isfinite(observed["recall"]):
            lines.append(f"poker_rule_monitor_recall{encoded} {observed['recall']:.17g}")
        lines.append(
            "poker_rule_monitor_evidence_per_1000_hands"
            f"{encoded} {observed['alert_volume_per_1000_hands']:.17g}"
        )
    lines.extend(
        [
            "# HELP poker_rule_monitor_alert Active deterministic rule-monitoring alert by reason.",
            "# TYPE poker_rule_monitor_alert gauge",
        ]
    )
    for alert in report["alerts"]:
        for reason in alert["reason_codes"]:
            labels = {
                **common,
                "alert_id": alert["alert_id"],
                "severity": alert["severity"],
                "rule_id": alert["rule_id"],
                "rule_version": alert["rule_version"],
                "reason_code": reason,
            }
            lines.append(f"poker_rule_monitor_alert{_prometheus_labels(labels)} 1")
    lines.extend(
        [
            "# HELP poker_rule_monitor_bad_label_rows Circular or unknown label rows in the window.",
            "# TYPE poker_rule_monitor_bad_label_rows gauge",
            "poker_rule_monitor_bad_label_rows"
            f"{_prometheus_labels(common)} "
            f"{counts['circular_label_rows'] + counts['unknown_label_rows']}",
        ]
    )
    return "\n".join(lines) + "\n"


def build_rule_monitoring_report(
    baseline_path: Path,
    window_path: Path,
    output_path: Path,
    prometheus_path: Path,
) -> dict[str, Any]:
    baseline = _load_json(baseline_path)
    window = _load_json(window_path)
    report = compute_rule_monitoring_report(
        baseline,
        window,
        source_artifacts={
            "rule_evaluation_report": {
                "path": baseline_path.name,
                "sha256": sha256(baseline_path),
            },
            "monitoring_window": {
                "path": window_path.name,
                "sha256": sha256(window_path),
            },
        },
    )
    sealed = _seal(report)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(sealed, indent=2, sort_keys=True) + "\n")
    prometheus_path.parent.mkdir(parents=True, exist_ok=True)
    prometheus_path.write_text(render_prometheus_metrics(sealed))
    return sealed


def validate_rule_monitoring_report(
    baseline_path: Path,
    window_path: Path,
    report_path: Path,
    prometheus_path: Path,
) -> dict[str, Any]:
    baseline = _load_json(baseline_path)
    window = _load_json(window_path)
    report = _load_json(report_path)
    if report.get("contract_version") != RULE_MONITORING_REPORT_CONTRACT_VERSION:
        raise ValueError("unsupported rule-monitoring report contract")
    _validate_integrity(report, owner="rule-monitoring report")
    expected = compute_rule_monitoring_report(
        baseline,
        window,
        source_artifacts={
            "rule_evaluation_report": {
                "path": baseline_path.name,
                "sha256": sha256(baseline_path),
            },
            "monitoring_window": {
                "path": window_path.name,
                "sha256": sha256(window_path),
            },
        },
    )
    actual = {
        key: value
        for key, value in report.items()
        if key not in {"generated_at", "integrity"}
    }
    if actual != expected:
        raise ValueError("rule-monitoring report does not deterministically recompute")
    expected_prometheus = render_prometheus_metrics(report)
    if prometheus_path.read_text() != expected_prometheus:
        raise ValueError("rule-monitoring Prometheus textfile is stale")
    return {
        "window_id": report["window"]["window_id"],
        "status": report["summary"]["status"],
        "rules": report["summary"]["rules"],
        "alerts": report["summary"]["alerts"],
        "insufficient_data_rules": report["summary"]["insufficient_data_rules"],
        "probability_changed": report["controls"]["model_probability_changed"],
    }
