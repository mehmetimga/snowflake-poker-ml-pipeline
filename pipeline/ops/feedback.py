"""Validated analyst feedback contract for delayed supervised labels."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Mapping


DISPOSITIONS = {"confirmed_collusion", "false_positive", "inconclusive"}


def _timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("feedback timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


@dataclass(frozen=True)
class AnalystFeedback:
    tenant_id: str
    product_id: str
    feedback_id: str
    risk_score_event_id: str
    hand_id: str
    pair_key: str
    model_run_id: str
    disposition: str
    confidence: float | None
    reason_code: str
    evidence: Mapping[str, Any]
    analyst_subject: str
    reviewed_at: str
    label_available_at: str
    risk_alert_event_id: str | None = None

    def __post_init__(self) -> None:
        required = (
            self.tenant_id, self.product_id, self.feedback_id,
            self.risk_score_event_id, self.hand_id, self.pair_key,
            self.model_run_id, self.reason_code,
            self.analyst_subject,
        )
        if any(not value.strip() for value in required):
            raise ValueError("feedback identity and audit fields are required")
        if self.disposition not in DISPOSITIONS:
            raise ValueError(f"unsupported feedback disposition: {self.disposition}")
        if self.confidence is not None and not 0 <= self.confidence <= 1:
            raise ValueError("feedback confidence must be in [0, 1]")
        if _timestamp(self.label_available_at) < _timestamp(self.reviewed_at):
            raise ValueError("label cannot become available before review")

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "AnalystFeedback":
        return cls(**dict(raw))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def training_label(self) -> dict[str, Any] | None:
        if self.disposition == "inconclusive":
            return None
        return {
            "tenant_id": self.tenant_id,
            "product_id": self.product_id,
            "source_risk_score_event_id": self.risk_score_event_id,
            "source_feedback_id": self.feedback_id,
            "hand_id": self.hand_id,
            "pair_key": self.pair_key,
            "target": int(self.disposition == "confirmed_collusion"),
            "label_available_at": self.label_available_at,
            "provenance": "analyst_review",
        }
