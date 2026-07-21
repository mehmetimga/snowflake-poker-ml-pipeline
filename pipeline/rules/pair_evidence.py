"""Pure, inference-safe Rules v2 evaluation for one pair-feature event.

The evaluator deliberately consumes only values already present in
``poker.pair-features.v1``.  It performs no I/O and never changes the model
probability or the decision-policy result.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Mapping

from pipeline.events import (
    PairFeatureEvent,
    RuleEvidenceEvent,
    build_rule_evidence_event,
)


RuleOperator = Literal["eq", "gt"]


@dataclass(frozen=True)
class PairRuleDefinition:
    rule_id: str
    rule_version: int
    rule_owner: str
    description: str
    effective_from: str
    severity: Literal["info", "low", "medium", "high", "critical"]
    feature_group: Literal["current_hand", "context", "pair_history"]
    feature_name: str
    operator: RuleOperator
    threshold: float
    raw_score_multiplier: float
    benchmark_weight: float
    benchmark_aggregation: Literal["add", "max_directional"]


PAIR_RULE_DEFINITIONS: tuple[PairRuleDefinition, ...] = (
    PairRuleDefinition(
        "pair.one-folded-other-won",
        1,
        "risk-analytics",
        "One player folded in the current hand while the other won.",
        "2026-07-21T00:00:00Z",
        "medium",
        "current_hand",
        "one_folded_other_won",
        "eq",
        1.0,
        100.0,
        0.20,
        "add",
    ),
    PairRuleDefinition(
        "pair.same-device",
        1,
        "trust-platform",
        "Both players were linked to the same device in event-time context.",
        "2026-07-21T00:00:00Z",
        "high",
        "context",
        "same_device",
        "eq",
        1.0,
        100.0,
        0.20,
        "add",
    ),
    PairRuleDefinition(
        "pair.same-network",
        1,
        "trust-platform",
        "Both players were linked to the same network cluster in event-time context.",
        "2026-07-21T00:00:00Z",
        "medium",
        "context",
        "same_network",
        "eq",
        1.0,
        100.0,
        0.20,
        "add",
    ),
    PairRuleDefinition(
        "pair.outcome-asymmetry",
        1,
        "risk-analytics",
        "Prior-only pair winnings are asymmetric.",
        "2026-07-21T00:00:00Z",
        "low",
        "pair_history",
        "outcome_asymmetry",
        "gt",
        0.0,
        100.0,
        0.15,
        "add",
    ),
    PairRuleDefinition(
        "pair.a-fold-b-win-rate",
        1,
        "risk-analytics",
        "Prior-only rate at which player A folded and player B won is non-zero.",
        "2026-07-21T00:00:00Z",
        "medium",
        "pair_history",
        "a_fold_b_win_rate",
        "gt",
        0.0,
        100.0,
        0.25,
        "max_directional",
    ),
    PairRuleDefinition(
        "pair.b-fold-a-win-rate",
        1,
        "risk-analytics",
        "Prior-only rate at which player B folded and player A won is non-zero.",
        "2026-07-21T00:00:00Z",
        "medium",
        "pair_history",
        "b_fold_a_win_rate",
        "gt",
        0.0,
        100.0,
        0.25,
        "max_directional",
    ),
)


def _as_signal(value: Any, *, rule_id: str) -> float:
    if isinstance(value, bool):
        return float(value)
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"rule {rule_id} requires a numeric or boolean feature"
        ) from exc
    if not math.isfinite(numeric) or not 0.0 <= numeric <= 1.0:
        raise ValueError(f"rule {rule_id} feature must be finite and in [0, 1]")
    return numeric


def _signals_from_event(event: PairFeatureEvent) -> dict[tuple[str, str], float]:
    payload = event.payload
    groups: Mapping[str, Any] = {
        "current_hand": payload.current_hand,
        "context": payload.context,
        "pair_history": payload.pair_history,
    }
    signals: dict[tuple[str, str], float] = {}
    for definition in PAIR_RULE_DEFINITIONS:
        value = getattr(groups[definition.feature_group], definition.feature_name)
        signals[(definition.feature_group, definition.feature_name)] = _as_signal(
            value, rule_id=definition.rule_id
        )
    return signals


def _fires(definition: PairRuleDefinition, value: float) -> bool:
    if definition.operator == "eq":
        return value == definition.threshold
    return value > definition.threshold


def rules_only_pair_score(event: PairFeatureEvent) -> float:
    """Reproduce the existing rules-only benchmark exactly for one pair."""

    signals = _signals_from_event(event)
    by_name = {name: value for (_, name), value in signals.items()}
    return min(
        1.0,
        max(
            0.0,
            0.20 * by_name["one_folded_other_won"]
            + 0.20 * by_name["same_device"]
            + 0.20 * by_name["same_network"]
            + 0.15 * by_name["outcome_asymmetry"]
            + 0.25
            * max(
                by_name["a_fold_b_win_rate"],
                by_name["b_fold_a_win_rate"],
            ),
        ),
    )


def evaluate_pair_rules(
    event: PairFeatureEvent,
    *,
    emitted_at: datetime | None = None,
) -> list[RuleEvidenceEvent]:
    """Return one governed evidence event for each fired pair rule."""

    signals = _signals_from_event(event)
    emitted_at = emitted_at or datetime.now(timezone.utc)
    evidence_events: list[RuleEvidenceEvent] = []
    for definition in PAIR_RULE_DEFINITIONS:
        observed = signals[(definition.feature_group, definition.feature_name)]
        if not _fires(definition, observed):
            continue
        evidence_events.append(
            build_rule_evidence_event(
                tenant_id=event.tenant_id,
                product_id=event.product_id,
                dataset_id=event.dataset_id,
                dataset_split=event.dataset_split,
                trace_id=event.trace_id,
                rule_id=definition.rule_id,
                rule_version=definition.rule_version,
                rule_owner=definition.rule_owner,
                entity_type="pair",
                entity_key=event.payload.pair_key,
                hand_id=event.payload.hand_id,
                observation_revision=event.payload.snapshot_revision,
                severity=definition.severity,
                raw_score=observed * definition.raw_score_multiplier,
                evidence={
                    "feature_group": definition.feature_group,
                    "feature_name": definition.feature_name,
                    "observed_value": observed,
                    "operator": definition.operator,
                    "threshold": definition.threshold,
                    "benchmark_weight": definition.benchmark_weight,
                    "benchmark_aggregation": definition.benchmark_aggregation,
                    "source_pair_feature_event_id": str(event.event_id),
                    "snapshot_revision": event.payload.snapshot_revision,
                },
                effective_at=event.payload.played_at,
                emitted_at=emitted_at,
                feature_definition_version=event.payload.feature_definition_version,
            )
        )
    return evidence_events
