"""Canonical version-one contracts for direct Kafka publishing.

Inference events deliberately exclude synthetic truth. Private label contracts
live in this module so their boundary is explicit, but labels are never accepted
as an inference payload.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, ClassVar, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


HAND_COMPLETED = "poker.hand.completed"
USER_CONTEXT_UPDATED = "poker.user-context.updated"
SESSION_STARTED = "poker.session.started"
ACCOUNT_LINK_UPDATED = "poker.account-link.updated"
PLAYER_HAND_CONTEXT_ENRICHED = "poker.hand-player-context.enriched"
PLAYER_HAND_CONTEXT_TOPIC = "poker.hand-player-context.v1"
PAIR_FEATURES_COMPUTED = "poker.pair-features.computed"
PAIR_FEATURES_TOPIC = "poker.pair-features.v1"
PAIR_FEATURE_DEFINITION_VERSION = "pair-features-v1"
RISK_SCORE_COMPUTED = "poker.risk-score.computed"
RISK_SCORES_TOPIC = "poker.risk-scores.v1"
RISK_ALERT_CREATED = "poker.risk-alert.created"
RISK_ALERTS_TOPIC = "poker.risk-alerts.v1"

TOPIC_BY_EVENT_TYPE: dict[str, str] = {
    HAND_COMPLETED: "poker.hands.raw.v1",
    USER_CONTEXT_UPDATED: "poker.user-context.v1",
    SESSION_STARTED: "poker.session-context.v1",
    ACCOUNT_LINK_UPDATED: "poker.account-links.v1",
}

_FORBIDDEN_INFERENCE_FIELDS = frozenset(
    {
        "collusion_group_id",
        "collusion_pair_id",
        "collusion_scenario",
        "is_collusive",
        "is_suspicious",
        "label",
        "label_available_at",
        "scenario_name",
    }
)


class _ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class HandAction(_ContractModel):
    sequence_no: int = Field(ge=0)
    player_id: str = Field(min_length=1)
    street: Literal["preflop", "flop", "turn", "river"]
    action_type: Literal["fold", "check", "call", "bet", "raise"]
    amount: float = Field(ge=0)


class HandPlayer(_ContractModel):
    player_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    position: Literal["SB", "BB", "UTG", "MP", "CO", "BTN"]
    stack_start: float = Field(gt=0)
    hole_cards: str = Field(min_length=5)
    won_amount: float = Field(ge=0)


class HandCompletedPayload(_ContractModel):
    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    dataset_split: str = Field(min_length=1)
    generator: Literal["pokerkit"]
    small_blind: float = Field(gt=0)
    big_blind: float = Field(gt=0)
    num_players: int = Field(ge=2)
    pot_size: float = Field(ge=0)
    board: list[str]
    actions: list[HandAction]
    players: list[HandPlayer]

    @model_validator(mode="after")
    def validate_internal_counts(self) -> "HandCompletedPayload":
        if self.num_players != len(self.players):
            raise ValueError("num_players must match the player payload count")
        sequences = [action.sequence_no for action in self.actions]
        if sequences != list(range(len(sequences))):
            raise ValueError("action sequence_no values must be contiguous from zero")
        player_ids = [player.player_id for player in self.players]
        if len(player_ids) != len(set(player_ids)):
            raise ValueError("hand player IDs must be unique")
        return self


class UserContextPayload(_ContractModel):
    user_id: str = Field(min_length=1)
    context_version: int = Field(ge=1)
    effective_at: datetime
    account_created_at: datetime
    country_bucket: str = Field(min_length=2)
    timezone: str = Field(min_length=1)
    acquisition_channel: Literal["organic", "affiliate", "paid", "referral"]
    kyc_level: Literal["pending", "basic", "verified"]
    account_status: Literal["active", "restricted", "suspended"]
    bankroll_bucket: Literal["low", "medium", "high"]
    preferred_stake_bucket: Literal["micro", "low", "medium", "high"]
    skill_rating: float = Field(ge=0, le=1)
    device_id: str = Field(min_length=1)
    network_cluster_id: str = Field(min_length=1)


class SessionPayload(_ContractModel):
    session_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    device_id: str = Field(min_length=1)
    network_cluster_id: str = Field(min_length=1)
    started_at: datetime
    status: Literal["active"] = "active"


class AccountLinkPayload(_ContractModel):
    link_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    related_user_id: str = Field(min_length=1)
    link_type: Literal["shared_device", "shared_network", "household"]
    confidence_bucket: Literal["low", "medium", "high"]
    link_version: int = Field(ge=1)
    effective_at: datetime

    @model_validator(mode="after")
    def validate_users_differ(self) -> "AccountLinkPayload":
        if self.user_id == self.related_user_id:
            raise ValueError("account link endpoints must differ")
        return self


class PlayerHandContextPayload(_ContractModel):
    """One hand/player row enriched with context valid at hand event time."""

    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    player: HandPlayer
    actions: list[HandAction]
    board: list[str]
    small_blind: float = Field(gt=0)
    big_blind: float = Field(gt=0)
    num_players: int = Field(ge=2)
    pot_size: float = Field(ge=0)
    source_hand_event_id: uuid.UUID
    context_status: Literal["matched", "matched_late", "missing", "corrected"]
    context_version: int | None = Field(default=None, ge=1)
    context_effective_at: datetime | None = None
    source_context_event_id: uuid.UUID | None = None
    context: UserContextPayload | None = None
    revision: int = Field(ge=1)
    allowed_lateness_ms: int = Field(ge=0)
    correction_window_ms: int = Field(ge=0)
    join_policy_version: Literal["event-time-user-context-v1"] = (
        "event-time-user-context-v1"
    )

    @model_validator(mode="after")
    def validate_context_join(self) -> "PlayerHandContextPayload":
        context_fields = (
            self.context_version,
            self.context_effective_at,
            self.source_context_event_id,
            self.context,
        )
        if self.context_status == "missing":
            if any(value is not None for value in context_fields):
                raise ValueError("missing joins must not contain context values")
            if self.revision != 1:
                raise ValueError("missing is an initial revision-one status")
            return self
        if any(value is None for value in context_fields):
            raise ValueError("matched joins must contain context values")
        assert self.context is not None
        if self.context.user_id != self.player.player_id:
            raise ValueError("joined context user_id must match player_id")
        if self.context.context_version != self.context_version:
            raise ValueError("context_version must match the joined context")
        if self.context.effective_at != self.context_effective_at:
            raise ValueError("context_effective_at must match the joined context")
        if self.context.effective_at > self.played_at:
            raise ValueError("future context cannot enrich a historical hand")
        if self.context_status == "corrected" and self.revision < 2:
            raise ValueError("corrected joins require revision two or higher")
        if self.context_status != "corrected" and self.revision != 1:
            raise ValueError("non-corrected joins must use revision one")
        return self


class EventEnvelope(_ContractModel):
    event_id: uuid.UUID
    event_type: str = Field(min_length=1)
    schema_version: Literal[1] = 1
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    trace_id: uuid.UUID
    payload: dict[str, Any]

    supported_event_types: ClassVar[frozenset[str]] = frozenset(TOPIC_BY_EVENT_TYPE)

    @field_validator("event_type")
    @classmethod
    def validate_event_type(cls, value: str) -> str:
        if value not in cls.supported_event_types:
            raise ValueError(f"unsupported event_type: {value}")
        return value

    @model_validator(mode="after")
    def validate_times(self) -> "EventEnvelope":
        if self.occurred_at.tzinfo is None or self.emitted_at.tzinfo is None:
            raise ValueError("event timestamps must include timezone information")
        return self


class PlayerHandContextEvent(_ContractModel):
    """Derived-event envelope emitted by the temporal context join."""

    event_id: uuid.UUID
    event_type: Literal["poker.hand-player-context.enriched"] = (
        PLAYER_HAND_CONTEXT_ENRICHED
    )
    schema_version: Literal[1] = 1
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    trace_id: uuid.UUID
    payload: PlayerHandContextPayload

    @model_validator(mode="after")
    def validate_times(self) -> "PlayerHandContextEvent":
        if self.occurred_at.tzinfo is None or self.emitted_at.tzinfo is None:
            raise ValueError("event timestamps must include timezone information")
        if self.occurred_at != self.payload.played_at:
            raise ValueError("derived occurred_at must equal hand played_at")
        return self


class UserHistoryFeatures(_ContractModel):
    """Player history strictly before the feature row's current hand."""

    hands_seen: int = Field(ge=0)
    total_won_amount: float = Field(ge=0)
    mean_won_amount: float = Field(ge=0)
    fold_rate: float = Field(ge=0, le=1)
    raise_rate: float = Field(ge=0, le=1)
    saw_flop_rate: float = Field(ge=0, le=1)


class CurrentHandPairFeatures(_ContractModel):
    position_index_a: int = Field(ge=0, le=5)
    position_index_b: int = Field(ge=0, le=5)
    position_gap: int = Field(ge=0, le=5)
    invested_amount_a: float = Field(ge=0)
    invested_amount_b: float = Field(ge=0)
    invested_pot_ratio_a: float = Field(ge=0)
    invested_pot_ratio_b: float = Field(ge=0)
    invested_abs_diff_ratio: float = Field(ge=0)
    won_amount_a: float = Field(ge=0)
    won_amount_b: float = Field(ge=0)
    outcome_abs_diff_ratio: float = Field(ge=0)
    aggressive_actions_a: int = Field(ge=0)
    aggressive_actions_b: int = Field(ge=0)
    fold_actions_a: int = Field(ge=0)
    fold_actions_b: int = Field(ge=0)
    both_saw_flop: bool
    both_saw_river: bool
    one_folded_other_won: bool


class PairContextFeatures(_ContractModel):
    context_missing_a: bool
    context_missing_b: bool
    skill_rating_a: float = Field(ge=0, le=1)
    skill_rating_b: float = Field(ge=0, le=1)
    skill_rating_abs_diff: float = Field(ge=0, le=1)
    account_age_days_a: float = Field(ge=0)
    account_age_days_b: float = Field(ge=0)
    account_age_abs_diff_days: float = Field(ge=0)
    same_country: bool
    same_timezone: bool
    same_acquisition_channel: bool
    same_device: bool
    same_network: bool
    bankroll_bucket_distance: int = Field(ge=0, le=2)
    preferred_stake_bucket_distance: int = Field(ge=0, le=3)


class PairHistoryFeatures(_ContractModel):
    """Shared pair history strictly before the current hand."""

    hands_together: int = Field(ge=0)
    total_won_amount_a: float = Field(ge=0)
    total_won_amount_b: float = Field(ge=0)
    outcome_asymmetry: float = Field(ge=0, le=1)
    a_fold_b_win_rate: float = Field(ge=0, le=1)
    b_fold_a_win_rate: float = Field(ge=0, le=1)
    both_saw_flop_rate: float = Field(ge=0, le=1)
    same_table_rate: float = Field(ge=0, le=1)
    last_seen_age_seconds: float | None = Field(default=None, ge=0)


class PairFeaturePayload(_ContractModel):
    """Post-hand features for one canonical unordered player pair."""

    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    pair_key: str = Field(min_length=3)
    player_a: str = Field(min_length=1)
    player_b: str = Field(min_length=1)
    num_players: int = Field(ge=2)
    source_hand_event_id: uuid.UUID
    source_player_context_event_id_a: uuid.UUID
    source_player_context_event_id_b: uuid.UUID
    source_revision_a: int = Field(ge=1)
    source_revision_b: int = Field(ge=1)
    context_status_a: Literal["matched", "matched_late", "missing", "corrected"]
    context_status_b: Literal["matched", "matched_late", "missing", "corrected"]
    context_version_a: int | None = Field(default=None, ge=1)
    context_version_b: int | None = Field(default=None, ge=1)
    snapshot_revision: int = Field(ge=1)
    feature_definition_version: Literal["pair-features-v1"] = (
        PAIR_FEATURE_DEFINITION_VERSION
    )
    current_hand: CurrentHandPairFeatures
    context: PairContextFeatures
    user_history_a: UserHistoryFeatures
    user_history_b: UserHistoryFeatures
    pair_history: PairHistoryFeatures

    @model_validator(mode="after")
    def validate_pair_snapshot(self) -> "PairFeaturePayload":
        if self.player_a >= self.player_b:
            raise ValueError("pair endpoints must be in canonical lexical order")
        if self.pair_key != f"{self.player_a}:{self.player_b}":
            raise ValueError("pair_key must use canonical player order")
        for status, version in (
            (self.context_status_a, self.context_version_a),
            (self.context_status_b, self.context_version_b),
        ):
            if (status == "missing") != (version is None):
                raise ValueError("missing context status and context version disagree")
        return self


class PairFeatureEvent(_ContractModel):
    """Derived-event envelope consumed by the Go risk scorer."""

    event_id: uuid.UUID
    event_type: Literal["poker.pair-features.computed"] = PAIR_FEATURES_COMPUTED
    schema_version: Literal[1] = 1
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    trace_id: uuid.UUID
    payload: PairFeaturePayload

    @model_validator(mode="after")
    def validate_times(self) -> "PairFeatureEvent":
        if self.occurred_at.tzinfo is None or self.emitted_at.tzinfo is None:
            raise ValueError("event timestamps must include timezone information")
        if self.occurred_at != self.payload.played_at:
            raise ValueError("derived occurred_at must equal hand played_at")
        return self


class PairRiskScore(_ContractModel):
    feature_event_id: uuid.UUID
    pair_key: str = Field(min_length=3)
    player_a: str = Field(min_length=1)
    player_b: str = Field(min_length=1)
    snapshot_revision: int = Field(ge=1)
    raw_probability: float = Field(ge=0, le=1)
    calibrated_probability: float = Field(ge=0, le=1)
    alert: bool

    @model_validator(mode="after")
    def validate_pair(self) -> "PairRiskScore":
        if self.player_a >= self.player_b:
            raise ValueError("risk-score pair endpoints must be canonical")
        if self.pair_key != f"{self.player_a}:{self.player_b}":
            raise ValueError("risk-score pair_key must use canonical player order")
        return self


class PlayerRiskScore(_ContractModel):
    player_id: str = Field(min_length=1)
    risk_probability: float = Field(ge=0, le=1)
    alert: bool


class RiskScorePayload(_ContractModel):
    """One complete-hand model decision with pair-level audit details."""

    score_id: str = Field(min_length=32, max_length=64)
    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    model_name: str = Field(min_length=1)
    model_run_id: str = Field(min_length=1)
    feature_definition_version: Literal["pair-features-v1"] = (
        PAIR_FEATURE_DEFINITION_VERSION
    )
    decision_policy_version: int = Field(default=1, ge=1)
    decision_threshold: float = Field(ge=0, le=1)
    service_implementation: str = Field(default="go-risk-scorer", min_length=1)
    service_build_version: str = Field(default="dev", min_length=1)
    scored_at: datetime
    pair_scores: list[PairRiskScore] = Field(min_length=15, max_length=15)
    player_scores: list[PlayerRiskScore] = Field(min_length=6, max_length=6)
    hand_risk_probability: float = Field(ge=0, le=1)
    alert: bool

    @model_validator(mode="after")
    def validate_complete_hand(self) -> "RiskScorePayload":
        pair_keys = [row.pair_key for row in self.pair_scores]
        player_ids = [row.player_id for row in self.player_scores]
        if len(pair_keys) != len(set(pair_keys)):
            raise ValueError("pair risk scores must be unique")
        if len(player_ids) != len(set(player_ids)):
            raise ValueError("player risk scores must be unique")
        highest = max(row.calibrated_probability for row in self.pair_scores)
        if abs(highest - self.hand_risk_probability) > 1e-9:
            raise ValueError("hand risk must equal the highest calibrated pair score")
        if self.alert != (self.hand_risk_probability >= self.decision_threshold):
            raise ValueError("hand alert must follow the versioned decision threshold")
        return self


class RiskScoreEvent(_ContractModel):
    """Derived score envelope published by the Go online scorer."""

    event_id: uuid.UUID
    event_type: Literal["poker.risk-score.computed"] = RISK_SCORE_COMPUTED
    schema_version: Literal[1] = 1
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    trace_id: uuid.UUID
    payload: RiskScorePayload

    @model_validator(mode="after")
    def validate_times(self) -> "RiskScoreEvent":
        if self.occurred_at.tzinfo is None or self.emitted_at.tzinfo is None:
            raise ValueError("event timestamps must include timezone information")
        if self.occurred_at != self.payload.played_at:
            raise ValueError("risk-score occurred_at must equal hand played_at")
        if self.emitted_at != self.payload.scored_at:
            raise ValueError("risk-score emitted_at must equal scored_at")
        return self


class RiskAlertPayload(_ContractModel):
    """Compact alert that references the complete risk-score audit event."""

    alert_id: uuid.UUID
    risk_score_event_id: uuid.UUID
    score_id: str = Field(min_length=32, max_length=64)
    hand_id: str = Field(min_length=1)
    table_id: str = Field(min_length=1)
    played_at: datetime
    model_name: str = Field(min_length=1)
    model_run_id: str = Field(min_length=1)
    feature_definition_version: Literal["pair-features-v1"] = (
        PAIR_FEATURE_DEFINITION_VERSION
    )
    decision_policy_version: int = Field(default=1, ge=1)
    decision_threshold: float = Field(ge=0, le=1)
    service_implementation: str = Field(default="go-risk-scorer", min_length=1)
    service_build_version: str = Field(default="dev", min_length=1)
    risk_probability: float = Field(ge=0, le=1)
    highest_risk_pair: PairRiskScore
    highest_risk_players: list[PlayerRiskScore] = Field(min_length=1, max_length=2)
    scored_at: datetime

    @model_validator(mode="after")
    def validate_alert(self) -> "RiskAlertPayload":
        if self.risk_probability < self.decision_threshold:
            raise ValueError("risk alert must meet the decision threshold")
        if abs(
            self.highest_risk_pair.calibrated_probability - self.risk_probability
        ) > 1e-9:
            raise ValueError("alert risk must equal the highest-risk pair score")
        expected_players = {
            self.highest_risk_pair.player_a,
            self.highest_risk_pair.player_b,
        }
        if {row.player_id for row in self.highest_risk_players} != expected_players:
            raise ValueError("alert players must be the highest-risk pair endpoints")
        return self


class RiskAlertEvent(_ContractModel):
    event_id: uuid.UUID
    event_type: Literal["poker.risk-alert.created"] = RISK_ALERT_CREATED
    schema_version: Literal[1] = 1
    tenant_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    dataset_id: str = Field(min_length=1)
    dataset_split: str = Field(min_length=1)
    occurred_at: datetime
    emitted_at: datetime
    trace_id: uuid.UUID
    payload: RiskAlertPayload

    @model_validator(mode="after")
    def validate_times(self) -> "RiskAlertEvent":
        if self.occurred_at.tzinfo is None or self.emitted_at.tzinfo is None:
            raise ValueError("event timestamps must include timezone information")
        if self.event_id != self.payload.alert_id:
            raise ValueError("alert event_id must equal payload alert_id")
        if self.occurred_at != self.payload.played_at:
            raise ValueError("risk-alert occurred_at must equal hand played_at")
        if self.emitted_at != self.payload.scored_at:
            raise ValueError("risk-alert emitted_at must equal scored_at")
        return self


class PlayerHandLabel(_ContractModel):
    example_id: uuid.UUID
    dataset_id: str
    dataset_split: str
    hand_id: str
    player_id: str
    is_suspicious: bool
    collusion_pair_id: str | None
    label_available_at: datetime
    provenance: Literal["synthetic"] = "synthetic"


class PairHandLabel(_ContractModel):
    example_id: uuid.UUID
    dataset_id: str
    dataset_split: str
    hand_id: str
    pair_key: str
    player_a: str
    player_b: str
    is_collusive: bool
    collusion_pair_id: str | None
    label_available_at: datetime
    provenance: Literal["synthetic"] = "synthetic"

    @model_validator(mode="after")
    def validate_pair(self) -> "PairHandLabel":
        if self.player_a >= self.player_b:
            raise ValueError("pair label endpoints must be in canonical lexical order")
        if self.pair_key != f"{self.player_a}:{self.player_b}":
            raise ValueError("pair_key must use canonical player order")
        return self


_PAYLOAD_MODEL_BY_TYPE: dict[str, type[_ContractModel]] = {
    HAND_COMPLETED: HandCompletedPayload,
    USER_CONTEXT_UPDATED: UserContextPayload,
    SESSION_STARTED: SessionPayload,
    ACCOUNT_LINK_UPDATED: AccountLinkPayload,
}


def assert_inference_safe(value: Any, path: str = "$") -> None:
    """Reject private truth fields anywhere in an inference event."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_text = str(key)
            if key_text.lower() in _FORBIDDEN_INFERENCE_FIELDS:
                raise ValueError(f"private label field found at {path}.{key_text}")
            assert_inference_safe(child, f"{path}.{key_text}")
    elif isinstance(value, (list, tuple)):
        for index, child in enumerate(value):
            assert_inference_safe(child, f"{path}[{index}]")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise ValueError("timestamps must include timezone information")
    return value.astimezone(timezone.utc)


def _stable_uuid(*parts: object) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, ":".join(str(part) for part in parts))


def validate_event(value: Mapping[str, Any] | EventEnvelope) -> EventEnvelope:
    """Validate both the common envelope and its domain-specific payload."""
    envelope = value if isinstance(value, EventEnvelope) else EventEnvelope.model_validate(value)
    assert_inference_safe(envelope.payload)
    payload_model = _PAYLOAD_MODEL_BY_TYPE[envelope.event_type]
    payload = payload_model.model_validate(envelope.payload).model_dump(mode="json")
    return envelope.model_copy(update={"payload": payload})


def build_event(
    *,
    event_type: str,
    aggregate_id: str,
    payload: Mapping[str, Any] | _ContractModel,
    dataset_id: str,
    dataset_split: str,
    occurred_at: datetime,
    emitted_at: datetime | None = None,
    tenant_id: str = "demo",
    product_id: str = "poker",
) -> EventEnvelope:
    """Build an event with replay-stable event and trace identifiers."""
    occurred_at = _as_utc(occurred_at)
    emitted_at = _as_utc(emitted_at or occurred_at)
    raw_payload = (
        payload.model_dump(mode="json")
        if isinstance(payload, _ContractModel)
        else dict(payload)
    )
    event = EventEnvelope(
        event_id=_stable_uuid(dataset_id, dataset_split, event_type, aggregate_id),
        event_type=event_type,
        tenant_id=tenant_id,
        product_id=product_id,
        dataset_id=dataset_id,
        dataset_split=dataset_split,
        occurred_at=occurred_at,
        emitted_at=emitted_at,
        trace_id=_stable_uuid(dataset_id, dataset_split, "trace", aggregate_id),
        payload=raw_payload,
    )
    return validate_event(event)


def event_partition_key(event: Mapping[str, Any] | EventEnvelope) -> str:
    """Return the canonical Kafka key for an event."""
    envelope = validate_event(event)
    payload = envelope.payload
    if envelope.event_type == HAND_COMPLETED:
        return str(payload["table_id"])
    if envelope.event_type == USER_CONTEXT_UPDATED:
        return str(payload["user_id"])
    if envelope.event_type == SESSION_STARTED:
        return str(payload["session_id"])
    if envelope.event_type == ACCOUNT_LINK_UPDATED:
        return str(payload["user_id"])
    raise ValueError(f"no partition key for {envelope.event_type}")


def contract_schema_bundle() -> dict[str, Any]:
    """Export deterministic JSON Schemas for non-Python consumers."""
    return {
        "schema_version": 1,
        "envelope": EventEnvelope.model_json_schema(),
        "payloads": {
            event_type: payload_model.model_json_schema()
            for event_type, payload_model in sorted(_PAYLOAD_MODEL_BY_TYPE.items())
        },
        "private_labels": {
            "player_hand": PlayerHandLabel.model_json_schema(),
            "pair_hand": PairHandLabel.model_json_schema(),
        },
        "derived_events": {
            PLAYER_HAND_CONTEXT_ENRICHED: PlayerHandContextEvent.model_json_schema(),
            PAIR_FEATURES_COMPUTED: PairFeatureEvent.model_json_schema(),
            RISK_SCORE_COMPUTED: RiskScoreEvent.model_json_schema(),
            RISK_ALERT_CREATED: RiskAlertEvent.model_json_schema(),
        },
        "topics": dict(sorted(TOPIC_BY_EVENT_TYPE.items())),
        "derived_topics": {
            PLAYER_HAND_CONTEXT_ENRICHED: PLAYER_HAND_CONTEXT_TOPIC,
            PAIR_FEATURES_COMPUTED: PAIR_FEATURES_TOPIC,
            RISK_SCORE_COMPUTED: RISK_SCORES_TOPIC,
            RISK_ALERT_CREATED: RISK_ALERTS_TOPIC,
        },
    }
