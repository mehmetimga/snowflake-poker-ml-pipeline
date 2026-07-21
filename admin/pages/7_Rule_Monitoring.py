from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import plotly.express as px
import streamlit as st

from admin import data_access as da


st.set_page_config(page_title="Rules v2 Monitoring", layout="wide")
st.title("Rules v2 monitoring")
st.caption(
    "Delayed independent labels and shadow evidence only. This page cannot "
    "disable a rule or authorize user enforcement."
)

artifacts = da.rule_monitoring_artifacts()
report = artifacts.get("report")
baseline = artifacts.get("baseline")
if not report:
    st.info("No rule-monitoring report. Run `make rule-monitoring`.")
    st.stop()

summary = report["summary"]
c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Status", summary["status"])
c2.metric("Rules", summary["rules"])
c3.metric("Alerts", summary["alerts"])
c4.metric("Insufficient data", summary["insufficient_data_rules"])
c5.metric("Disabled", summary["disabled_rules"])

st.subheader("Immutable lineage")
lineage = {
    "tenant_id": report["window"]["tenant_id"],
    "product_id": report["window"]["product_id"],
    "window_id": report["window"]["window_id"],
    "evaluation_id": report["baseline"]["evaluation_id"],
    "baseline_sha256": report["baseline"]["payload_sha256"],
    "rollout_id": report["window"]["rollout"]["rollout_id"],
    "model_name": report["window"]["model"]["model_name"],
    "model_run_id": report["window"]["model"]["run_id"],
    "event_start": report["window"]["interval"]["event_start"],
    "event_end": report["window"]["interval"]["event_end"],
    "label_cutoff_at": report["window"]["interval"]["label_cutoff_at"],
}
st.dataframe(pd.DataFrame([lineage]), use_container_width=True, hide_index=True)

rows = []
for result in report["rule_results"]:
    observed = result["observed"]
    rows.append(
        {
            "rule_id": result["rule_id"],
            "version": result["rule_version"],
            "runtime": result["runtime"],
            "enabled": result["enabled"],
            "status": result["status"],
            "eligible": result["eligible"],
            "firings": observed["firings"],
            "firing_rate": observed["firing_rate"],
            "precision": observed["precision"],
            "recall": observed["recall"],
            "evidence_per_1000_hands": observed[
                "alert_volume_per_1000_hands"
            ],
            "reasons": ", ".join(
                result["reason_codes"] or result["eligibility_reasons"]
            ),
        }
    )
rule_frame = pd.DataFrame(rows)
st.subheader("Rule windows")
st.dataframe(rule_frame, use_container_width=True, hide_index=True)
st.plotly_chart(
    px.bar(
        rule_frame,
        x="rule_id",
        y="evidence_per_1000_hands",
        color="status",
        title="Evidence volume per 1,000 hands",
    ),
    use_container_width=True,
)

st.subheader("Monitoring alerts")
if report["alerts"]:
    alert_rows = []
    for alert in report["alerts"]:
        alert_rows.append(
            {
                "alert_id": alert["alert_id"],
                "severity": alert["severity"],
                "rule_id": alert["rule_id"],
                "rule_version": alert["rule_version"],
                "reason_codes": ", ".join(alert["reason_codes"]),
                "recommended_action": alert["recommended_action"],
                "automatic_rule_disable": alert["automatic_rule_disable"],
                "enforcement_authority": alert["enforcement_authority"],
            }
        )
    st.dataframe(pd.DataFrame(alert_rows), use_container_width=True, hide_index=True)
else:
    st.success("No eligible-window monitoring threshold is currently violated.")

if baseline:
    st.subheader("B5 segment reliability")
    segment_rows = []
    for segment in baseline["segment_analysis"]["segments"]:
        segment_rows.append(
            {
                "segment_family": segment["segment_family"],
                "segment_value": segment["segment_value"],
                "rule_id": segment["rule_id"],
                "status": segment["reliability"]["status"],
                "rows": segment["counts"]["rows"],
                "hands": segment["counts"]["hands"],
                "positives": segment["counts"]["positives"],
                "firings": segment["counts"]["firings"],
                "reasons": ", ".join(segment["reliability"]["reasons"]),
            }
        )
    st.dataframe(pd.DataFrame(segment_rows), use_container_width=True, hide_index=True)
