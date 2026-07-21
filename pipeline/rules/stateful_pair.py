"""Offline oracle for the first stateful Java/Flink pair rule.

The object is intentionally serializable: tests can snapshot and restore it in
the same way Flink checkpoints the keyed JSON state used online.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone

from pipeline.events import (
    PairFeatureEvent,
    RuleEvidenceEvent,
    build_rule_evidence_event,
)


@dataclass(frozen=True)
class RepeatedFoldRuleConfig:
    rule_id: str = "pair.repeated-fold-to-partner-wins"
    rule_version: int = 1
    rule_owner: str = "risk-analytics"
    description: str = (
        "One direction records repeated fold-to-partner wins in a rolling event-time window."
    )
    effective_from: str = "2026-07-21T00:00:00Z"
    severity: str = "high"
    window_hours: int = 24
    minimum_hands: int = 5
    minimum_directional_count: int = 3
    directional_rate_threshold: float = 0.6
    allowed_lateness_seconds: int = 120
    correction_horizon_hours: int = 48


REPEATED_FOLD_RULE_CONFIG = RepeatedFoldRuleConfig()


@dataclass(frozen=True)
class StatefulPairObservation:
    event_id: uuid.UUID
    tenant_id: str
    product_id: str
    dataset_id: str
    dataset_split: str
    trace_id: uuid.UUID
    hand_id: str
    pair_key: str
    played_at: datetime
    emitted_at: datetime
    snapshot_revision: int
    a_fold_b_win: bool
    b_fold_a_win: bool
    feature_definition_version: str = "pair-features-v1"

    @classmethod
    def from_event(cls, event: PairFeatureEvent) -> "StatefulPairObservation":
        current = event.payload.current_hand
        return cls(
            event_id=event.event_id,
            tenant_id=event.tenant_id,
            product_id=event.product_id,
            dataset_id=event.dataset_id,
            dataset_split=event.dataset_split,
            trace_id=event.trace_id,
            hand_id=event.payload.hand_id,
            pair_key=event.payload.pair_key,
            played_at=event.payload.played_at,
            emitted_at=event.emitted_at,
            snapshot_revision=event.payload.snapshot_revision,
            a_fold_b_win=(current.fold_actions_a > 0 and current.won_amount_b > 0),
            b_fold_a_win=(current.fold_actions_b > 0 and current.won_amount_a > 0),
            feature_definition_version=event.payload.feature_definition_version,
        )


@dataclass(frozen=True)
class StatefulRuleResult:
    status: str
    evidence_event: RuleEvidenceEvent | None
    window_hand_count: int
    directional_count: int
    directional_rate: float


class RepeatedFoldWindowRule:
    """One canonical pair's deterministic rolling event-time state."""

    def __init__(self, config: RepeatedFoldRuleConfig | None = None) -> None:
        self.config = config or REPEATED_FOLD_RULE_CONFIG
        self._scope: tuple[str, str, str, str, str] | None = None
        self._max_event_time: datetime | None = None
        self._observations: dict[str, StatefulPairObservation] = {}

    def evaluate(
        self,
        observation: StatefulPairObservation,
        *,
        watermark: datetime | None = None,
    ) -> StatefulRuleResult:
        self._validate(observation)
        existing = self._observations.get(observation.hand_id)
        if existing is None and self._too_late(observation, watermark):
            return StatefulRuleResult("too_late", None, 0, 0, 0.0)
        if existing is not None:
            if observation.snapshot_revision < existing.snapshot_revision:
                return StatefulRuleResult("stale", None, 0, 0, 0.0)
            if observation.snapshot_revision == existing.snapshot_revision:
                if observation != existing:
                    raise ValueError("same hand revision has conflicting rule inputs")
                return self._evaluate_window(observation, "duplicate")
            if self._outside_correction_horizon(observation):
                return StatefulRuleResult("too_late_correction", None, 0, 0, 0.0)
            status = "corrected"
        else:
            status = "accepted"

        self._observations[observation.hand_id] = observation
        if self._max_event_time is None or observation.played_at > self._max_event_time:
            self._max_event_time = observation.played_at
        self._prune()
        return self._evaluate_window(observation, status)

    def snapshot(self) -> str:
        def encode(value: StatefulPairObservation) -> dict[str, object]:
            raw = asdict(value)
            for field in ("event_id", "trace_id"):
                raw[field] = str(raw[field])
            for field in ("played_at", "emitted_at"):
                raw[field] = (
                    raw[field]
                    .astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
            return raw

        return json.dumps(
            {
                "scope": list(self._scope) if self._scope else None,
                "max_event_time": (
                    self._max_event_time.astimezone(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z")
                    if self._max_event_time
                    else None
                ),
                "observations": {
                    hand_id: encode(value)
                    for hand_id, value in sorted(self._observations.items())
                },
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    @classmethod
    def restore(
        cls,
        value: str,
        config: RepeatedFoldRuleConfig | None = None,
    ) -> "RepeatedFoldWindowRule":
        raw = json.loads(value)
        rule = cls(config)
        if raw["scope"] is not None:
            rule._scope = tuple(raw["scope"])
        if raw["max_event_time"] is not None:
            rule._max_event_time = datetime.fromisoformat(raw["max_event_time"])
        for hand_id, item in raw["observations"].items():
            item["event_id"] = uuid.UUID(item["event_id"])
            item["trace_id"] = uuid.UUID(item["trace_id"])
            item["played_at"] = datetime.fromisoformat(item["played_at"])
            item["emitted_at"] = datetime.fromisoformat(item["emitted_at"])
            rule._observations[hand_id] = StatefulPairObservation(**item)
        return rule

    @property
    def state_size(self) -> int:
        return len(self._observations)

    def _validate(self, value: StatefulPairObservation) -> None:
        if value.snapshot_revision < 1:
            raise ValueError("snapshot revision must be positive")
        if value.played_at.tzinfo is None or value.emitted_at.tzinfo is None:
            raise ValueError("stateful rule timestamps must include timezone")
        players = value.pair_key.split(":")
        if len(players) != 2 or players[0] >= players[1]:
            raise ValueError("stateful rule pair key must be canonical")
        scope = (
            value.tenant_id,
            value.product_id,
            value.dataset_id,
            value.dataset_split,
            value.pair_key,
        )
        if self._scope is None:
            self._scope = scope
        elif self._scope != scope:
            raise ValueError("stateful pair rule cannot combine scoped pair keys")

    def _too_late(
        self,
        value: StatefulPairObservation,
        watermark: datetime | None,
    ) -> bool:
        if watermark is None:
            return False
        return value.played_at < watermark - timedelta(
            seconds=self.config.allowed_lateness_seconds
        )

    def _outside_correction_horizon(self, value: StatefulPairObservation) -> bool:
        if self._max_event_time is None:
            return False
        return value.played_at < self._max_event_time - timedelta(
            hours=self.config.correction_horizon_hours
        )

    def _prune(self) -> None:
        if self._max_event_time is None:
            return
        oldest = self._max_event_time - timedelta(
            hours=self.config.correction_horizon_hours
        )
        self._observations = {
            hand_id: value
            for hand_id, value in self._observations.items()
            if value.played_at >= oldest
        }

    def _evaluate_window(
        self,
        current: StatefulPairObservation,
        status: str,
    ) -> StatefulRuleResult:
        start = current.played_at - timedelta(hours=self.config.window_hours)
        window = [
            value
            for value in self._observations.values()
            if start <= value.played_at <= current.played_at
        ]
        a_count = sum(value.a_fold_b_win for value in window)
        b_count = sum(value.b_fold_a_win for value in window)
        if a_count >= b_count:
            direction, directional_count = "a_fold_b_win", a_count
        else:
            direction, directional_count = "b_fold_a_win", b_count
        hand_count = len(window)
        rate = round(directional_count / hand_count, 9) if hand_count else 0.0
        fired = (
            hand_count >= self.config.minimum_hands
            and directional_count >= self.config.minimum_directional_count
            and rate >= self.config.directional_rate_threshold
        )
        evidence_event = None
        if fired:
            evidence_event = build_rule_evidence_event(
                tenant_id=current.tenant_id,
                product_id=current.product_id,
                dataset_id=current.dataset_id,
                dataset_split=current.dataset_split,
                trace_id=current.trace_id,
                rule_id=self.config.rule_id,
                rule_version=self.config.rule_version,
                rule_owner=self.config.rule_owner,
                entity_type="pair",
                entity_key=current.pair_key,
                hand_id=current.hand_id,
                observation_revision=current.snapshot_revision,
                severity="high",
                raw_score=round(rate * 100.0, 9),
                evidence={
                    "window_hours": self.config.window_hours,
                    "window_hand_count": hand_count,
                    "direction": direction,
                    "directional_fold_win_count": directional_count,
                    "directional_fold_win_rate": rate,
                    "minimum_hands": self.config.minimum_hands,
                    "minimum_directional_count": (
                        self.config.minimum_directional_count
                    ),
                    "rate_threshold": self.config.directional_rate_threshold,
                    "source_pair_feature_event_id": str(current.event_id),
                    "snapshot_revision": current.snapshot_revision,
                },
                effective_at=current.played_at,
                emitted_at=current.emitted_at,
                feature_definition_version=current.feature_definition_version,
            )
        return StatefulRuleResult(
            status,
            evidence_event,
            hand_count,
            directional_count,
            rate,
        )
