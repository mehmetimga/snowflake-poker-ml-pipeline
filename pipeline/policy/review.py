"""Offline oracle for the versioned review-routing decision policy."""

from __future__ import annotations

import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal, Sequence

from pydantic import BaseModel, ConfigDict, Field, model_validator

from pipeline.events import (
    PolicyRuleReference,
    ReviewDecisionEvent,
    ReviewDecisionPayload,
    RiskScoreEvent,
    RuleEvidenceEvent,
    stable_review_decision_id,
)


class _PolicyModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RulePolicySpec(_PolicyModel):
    rule_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9_.-]+$")
    rule_version: int = Field(ge=1)


class ModelThresholdPolicy(_PolicyModel):
    enabled: Literal[True] = True
    reason_code: Literal["model.threshold-exceeded"] = "model.threshold-exceeded"
    outcome: Literal["review_recommended"] = "review_recommended"


class ReviewActions(_PolicyModel):
    no_review: Literal["none"] = "none"
    review_recommended: Literal["analyst_review"] = "analyst_review"
    mandatory_review: Literal["analyst_review"] = "analyst_review"


class ReviewRolloutGates(_PolicyModel):
    maximum_review_rate: float = Field(ge=0, le=1)
    maximum_mandatory_review_rate: float = Field(ge=0, le=1)
    minimum_decisions: int = Field(ge=1)


class ReviewPolicyDefinition(_PolicyModel):
    schema_version: Literal[1] = 1
    policy_id: str = Field(min_length=1, pattern=r"^[a-z0-9][a-z0-9_.-]+$")
    policy_version: int = Field(ge=1)
    policy_owner: str = Field(min_length=1)
    effective_from: datetime
    mode: Literal["shadow", "enforced"]
    model_threshold: ModelThresholdPolicy
    soft_rules: tuple[RulePolicySpec, ...]
    hard_rules: tuple[RulePolicySpec, ...]
    unknown_rule_behavior: Literal["reject"] = "reject"
    data_quality_behavior: Literal["dead_letter"] = "dead_letter"
    actions: ReviewActions
    rollout_gates: ReviewRolloutGates

    @model_validator(mode="after")
    def validate_rule_categories(self) -> "ReviewPolicyDefinition":
        if self.effective_from.tzinfo is None:
            raise ValueError("review policy effective_from must include timezone")
        values = [
            (value.rule_id, value.rule_version)
            for value in (*self.soft_rules, *self.hard_rules)
        ]
        if len(values) != len(set(values)):
            raise ValueError("a rule version must have exactly one policy category")
        return self


class PolicyRuleInput(_PolicyModel):
    rule_event_id: uuid.UUID
    rule_id: str
    rule_version: int = Field(ge=1)


class PolicyEvaluationInput(_PolicyModel):
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    trace_id: uuid.UUID
    risk_score_event_id: uuid.UUID
    score_id: str = Field(min_length=32, max_length=64)
    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    decided_at: datetime
    model_threshold_exceeded: bool
    rule_evidence: tuple[PolicyRuleInput, ...] = ()


def load_review_policy(path: str | Path) -> ReviewPolicyDefinition:
    return ReviewPolicyDefinition.model_validate_json(Path(path).read_text())


def evaluate_review_inputs(
    value: PolicyEvaluationInput,
    policy: ReviewPolicyDefinition,
) -> ReviewDecisionEvent:
    """Classify governed evidence and create one replay-safe policy decision."""

    categories = {
        (rule.rule_id, rule.rule_version): "soft" for rule in policy.soft_rules
    }
    categories.update(
        {
            (rule.rule_id, rule.rule_version): "hard"
            for rule in policy.hard_rules
        }
    )
    references: list[PolicyRuleReference] = []
    for rule in value.rule_evidence:
        category = categories.get((rule.rule_id, rule.rule_version))
        if category is None:
            raise ValueError(
                f"rule {rule.rule_id}:v{rule.rule_version} is not governed by "
                f"{policy.policy_id}:v{policy.policy_version}"
            )
        references.append(
            PolicyRuleReference(
                rule_event_id=rule.rule_event_id,
                rule_id=rule.rule_id,
                rule_version=rule.rule_version,
                category=category,
            )
        )
    references.sort(key=lambda item: str(item.rule_event_id))
    hard = [value for value in references if value.category == "hard"]
    if hard:
        outcome, action = "mandatory_review", policy.actions.mandatory_review
    elif value.model_threshold_exceeded:
        outcome, action = "review_recommended", policy.actions.review_recommended
    else:
        outcome, action = "no_review", policy.actions.no_review
    reason_codes: list[str] = []
    if value.model_threshold_exceeded:
        reason_codes.append(policy.model_threshold.reason_code)
    reason_codes.extend(
        sorted(
            f"hard-rule.{rule.rule_id}.v{rule.rule_version}" for rule in hard
        )
    )
    decision_id = stable_review_decision_id(
        tenant_id=value.tenant_id,
        product_id=value.product_id,
        dataset_id=value.dataset_id,
        dataset_split=value.dataset_split,
        policy_id=policy.policy_id,
        policy_version=policy.policy_version,
        risk_score_event_id=value.risk_score_event_id,
    )
    return ReviewDecisionEvent(
        event_id=decision_id,
        tenant_id=value.tenant_id,
        product_id=value.product_id,
        dataset_id=value.dataset_id,
        dataset_split=value.dataset_split,
        occurred_at=value.played_at,
        emitted_at=value.decided_at,
        trace_id=value.trace_id,
        payload=ReviewDecisionPayload(
            decision_id=decision_id,
            risk_score_event_id=value.risk_score_event_id,
            score_id=value.score_id,
            hand_id=value.hand_id,
            table_id=value.table_id,
            played_at=value.played_at,
            policy_id=policy.policy_id,
            policy_version=policy.policy_version,
            policy_owner=policy.policy_owner,
            policy_mode=policy.mode,
            outcome=outcome,
            action=action,
            reason_codes=reason_codes,
            model_threshold_exceeded=value.model_threshold_exceeded,
            rule_evidence=references,
            decided_at=value.decided_at,
        ),
    )


def evaluate_review_policy(
    score: RiskScoreEvent,
    evidence_events: Sequence[RuleEvidenceEvent],
    policy: ReviewPolicyDefinition,
) -> ReviewDecisionEvent:
    """Evaluate the policy from validated score and rule-evidence contracts."""

    expected = list(score.payload.rule_evidence_event_ids)
    actual = [event.event_id for event in evidence_events]
    if expected != actual:
        raise ValueError("score rule-evidence references do not match policy inputs")
    for event in evidence_events:
        if (
            event.tenant_id != score.tenant_id
            or event.product_id != score.product_id
            or event.dataset_id != score.dataset_id
            or event.dataset_split != score.dataset_split
            or event.trace_id != score.trace_id
            or event.payload.hand_id != score.payload.hand_id
        ):
            raise ValueError("rule evidence does not match policy score scope")
    return evaluate_review_inputs(
        PolicyEvaluationInput(
            tenant_id=score.tenant_id,
            product_id=score.product_id,
            dataset_id=score.dataset_id,
            dataset_split=score.dataset_split,
            trace_id=score.trace_id,
            risk_score_event_id=score.event_id,
            score_id=score.payload.score_id,
            hand_id=score.payload.hand_id,
            table_id=score.payload.table_id,
            played_at=score.payload.played_at,
            decided_at=score.payload.scored_at,
            model_threshold_exceeded=score.payload.alert,
            rule_evidence=tuple(
                PolicyRuleInput(
                    rule_event_id=event.event_id,
                    rule_id=event.payload.rule_id,
                    rule_version=event.payload.rule_version,
                )
                for event in evidence_events
            ),
        ),
        policy,
    )
