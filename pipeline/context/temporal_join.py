"""Framework-independent reference for the Flink event-time context join.

The implementation intentionally models only the externally visible policy.
It gives unit tests, batch backfills, and Snowflake parity checks an oracle that
does not depend on a running Flink cluster.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable, Mapping

from pipeline.events import (
    HAND_COMPLETED,
    PLAYER_HAND_CONTEXT_ENRICHED,
    USER_CONTEXT_UPDATED,
    EventEnvelope,
    PlayerHandContextEvent,
    PlayerHandContextPayload,
    UserContextPayload,
    validate_event,
)


def _epoch_ms(value: datetime) -> int:
    if value.tzinfo is None:
        raise ValueError("timestamps must include timezone information")
    return int(value.timestamp() * 1_000)


def _from_epoch_ms(value: int) -> datetime:
    return datetime.fromtimestamp(value / 1_000, tz=timezone.utc)


def _derived_event_id(
    hand: EventEnvelope,
    player_id: str,
    revision: int,
) -> uuid.UUID:
    name = ":".join(
        (
            hand.dataset_id,
            hand.dataset_split,
            PLAYER_HAND_CONTEXT_ENRICHED,
            str(hand.event_id),
            player_id,
            str(revision),
        )
    )
    return uuid.uuid5(uuid.NAMESPACE_URL, name)


def _context_order(event: EventEnvelope) -> tuple[datetime, int, str]:
    payload = UserContextPayload.model_validate(event.payload)
    return payload.effective_at, payload.context_version, str(event.event_id)


def select_context_as_of(
    context_events: Iterable[EventEnvelope | Mapping[str, object]],
    *,
    user_id: str,
    played_at: datetime,
) -> EventEnvelope | None:
    """Select the latest context whose effective time is not in the future."""
    candidates: list[EventEnvelope] = []
    versions: dict[int, EventEnvelope] = {}
    for raw in context_events:
        event = validate_event(raw)
        if event.event_type != USER_CONTEXT_UPDATED:
            raise ValueError("context history contains a non-context event")
        payload = UserContextPayload.model_validate(event.payload)
        if payload.user_id != user_id:
            continue
        previous = versions.get(payload.context_version)
        if previous is not None and previous.event_id != event.event_id:
            raise ValueError(
                f"conflicting context version {payload.context_version} for {user_id}"
            )
        versions[payload.context_version] = event
        if payload.effective_at <= played_at:
            candidates.append(event)
    return max(candidates, key=_context_order, default=None)


def enrich_player_hand(
    hand_event: EventEnvelope | Mapping[str, object],
    *,
    player_id: str,
    context_event: EventEnvelope | Mapping[str, object] | None,
    context_arrived_after_hand: bool = False,
    corrected: bool = False,
    revision: int = 1,
    allowed_lateness_ms: int = 30_000,
    correction_window_ms: int = 300_000,
    emitted_at: datetime | None = None,
) -> PlayerHandContextEvent:
    """Build one canonical derived record after a point-in-time selection."""
    hand = validate_event(hand_event)
    if hand.event_type != HAND_COMPLETED:
        raise ValueError("expected a hand-completed event")
    hand_payload = hand.payload
    players = [player for player in hand_payload["players"] if player["player_id"] == player_id]
    if len(players) != 1:
        raise ValueError(f"hand must contain player exactly once: {player_id}")

    selected: EventEnvelope | None = None
    context_payload: UserContextPayload | None = None
    if context_event is not None:
        selected = validate_event(context_event)
        if selected.event_type != USER_CONTEXT_UPDATED:
            raise ValueError("expected a user-context event")
        context_payload = UserContextPayload.model_validate(selected.payload)
        if context_payload.user_id != player_id:
            raise ValueError("context user_id must match player_id")
        played_at = datetime.fromisoformat(str(hand_payload["played_at"]))
        if context_payload.effective_at > played_at:
            raise ValueError("future context cannot enrich a historical hand")

    if selected is None:
        status = "missing"
    elif corrected:
        status = "corrected"
    elif context_arrived_after_hand:
        status = "matched_late"
    else:
        status = "matched"

    emitted = emitted_at or datetime.now(timezone.utc)
    payload = PlayerHandContextPayload(
        hand_id=hand_payload["hand_id"],
        table_id=hand_payload["table_id"],
        played_at=hand_payload["played_at"],
        player=players[0],
        actions=hand_payload["actions"],
        board=hand_payload["board"],
        small_blind=hand_payload["small_blind"],
        big_blind=hand_payload["big_blind"],
        num_players=hand_payload["num_players"],
        pot_size=hand_payload["pot_size"],
        source_hand_event_id=hand.event_id,
        context_status=status,
        context_version=(context_payload.context_version if context_payload else None),
        context_effective_at=(context_payload.effective_at if context_payload else None),
        source_context_event_id=(selected.event_id if selected else None),
        context=context_payload,
        revision=revision,
        allowed_lateness_ms=allowed_lateness_ms,
        correction_window_ms=correction_window_ms,
    )
    return PlayerHandContextEvent(
        event_id=_derived_event_id(hand, player_id, revision),
        tenant_id=hand.tenant_id,
        product_id=hand.product_id,
        dataset_id=hand.dataset_id,
        dataset_split=hand.dataset_split,
        occurred_at=hand.occurred_at,
        emitted_at=emitted,
        trace_id=hand.trace_id,
        payload=payload,
    )


@dataclass
class _ContextArrival:
    event: EventEnvelope
    sequence: int


@dataclass
class _PlayerHandState:
    hand: EventEnvelope
    player_id: str
    arrival_sequence: int
    due_at_ms: int
    cleanup_at_ms: int
    revision: int = 0
    selected_context_event_id: uuid.UUID | None = None
    emitted: bool = False


class TemporalContextJoinCore:
    """Small deterministic state machine mirroring the Java Flink operator."""

    def __init__(
        self,
        *,
        allowed_lateness_ms: int = 30_000,
        correction_window_ms: int = 300_000,
    ) -> None:
        if allowed_lateness_ms < 0 or correction_window_ms < 0:
            raise ValueError("join timing settings must be non-negative")
        self.allowed_lateness_ms = allowed_lateness_ms
        self.correction_window_ms = correction_window_ms
        self._sequence = 0
        self._watermark_ms = -(2**63)
        self._contexts: dict[str, list[_ContextArrival]] = {}
        self._player_hands: dict[tuple[uuid.UUID, str], _PlayerHandState] = {}
        self._seen_context_ids: set[uuid.UUID] = set()

    def process_hand(
        self, event: EventEnvelope | Mapping[str, object]
    ) -> list[PlayerHandContextEvent]:
        """Buffer a hand; output is controlled by the event-time watermark."""
        hand = validate_event(event)
        if hand.event_type != HAND_COMPLETED:
            raise ValueError("expected a hand-completed event")
        self._sequence += 1
        played_at_ms = _epoch_ms(datetime.fromisoformat(str(hand.payload["played_at"])))
        for player in hand.payload["players"]:
            key = (hand.event_id, str(player["player_id"]))
            self._player_hands.setdefault(
                key,
                _PlayerHandState(
                    hand=hand,
                    player_id=str(player["player_id"]),
                    arrival_sequence=self._sequence,
                    due_at_ms=played_at_ms + self.allowed_lateness_ms,
                    cleanup_at_ms=(
                        played_at_ms
                        + self.allowed_lateness_ms
                        + self.correction_window_ms
                    ),
                ),
            )
        return []

    def process_context(
        self, event: EventEnvelope | Mapping[str, object]
    ) -> list[PlayerHandContextEvent]:
        """Store context and emit revisions for still-correctable joined rows."""
        context = validate_event(event)
        if context.event_type != USER_CONTEXT_UPDATED:
            raise ValueError("expected a user-context event")
        if context.event_id in self._seen_context_ids:
            return []
        self._seen_context_ids.add(context.event_id)
        self._sequence += 1
        payload = UserContextPayload.model_validate(context.payload)
        arrivals = self._contexts.setdefault(payload.user_id, [])
        for arrival in arrivals:
            old = UserContextPayload.model_validate(arrival.event.payload)
            if old.context_version == payload.context_version:
                raise ValueError(
                    f"conflicting context version {payload.context_version} "
                    f"for {payload.user_id}"
                )
        arrivals.append(_ContextArrival(context, self._sequence))

        corrections: list[PlayerHandContextEvent] = []
        for state in self._player_hands.values():
            if state.player_id != payload.user_id or not state.emitted:
                continue
            if self._watermark_ms > state.cleanup_at_ms:
                continue
            selected = self._select(state)
            if selected is None or selected.event.event_id == state.selected_context_event_id:
                continue
            state.revision += 1
            state.selected_context_event_id = selected.event.event_id
            corrections.append(
                enrich_player_hand(
                    state.hand,
                    player_id=state.player_id,
                    context_event=selected.event,
                    corrected=True,
                    revision=state.revision,
                    allowed_lateness_ms=self.allowed_lateness_ms,
                    correction_window_ms=self.correction_window_ms,
                    emitted_at=context.emitted_at,
                )
            )
        return corrections

    def advance_watermark(self, watermark_ms: int) -> list[PlayerHandContextEvent]:
        """Emit due rows and discard rows after their correction horizon."""
        if watermark_ms < self._watermark_ms:
            raise ValueError("watermarks must be monotonically increasing")
        self._watermark_ms = watermark_ms
        output: list[PlayerHandContextEvent] = []
        for state in self._player_hands.values():
            if state.emitted or state.due_at_ms > watermark_ms:
                continue
            selected = self._select(state)
            state.revision = 1
            state.emitted = True
            state.selected_context_event_id = (
                selected.event.event_id if selected is not None else None
            )
            output.append(
                enrich_player_hand(
                    state.hand,
                    player_id=state.player_id,
                    context_event=(selected.event if selected is not None else None),
                    context_arrived_after_hand=(
                        selected is not None
                        and selected.sequence > state.arrival_sequence
                    ),
                    revision=state.revision,
                    allowed_lateness_ms=self.allowed_lateness_ms,
                    correction_window_ms=self.correction_window_ms,
                    emitted_at=_from_epoch_ms(watermark_ms),
                )
            )
        expired = [
            key
            for key, state in self._player_hands.items()
            if state.emitted and watermark_ms > state.cleanup_at_ms
        ]
        for key in expired:
            del self._player_hands[key]
        return output

    def _select(self, state: _PlayerHandState) -> _ContextArrival | None:
        contexts = self._contexts.get(state.player_id, [])
        played_at = datetime.fromisoformat(str(state.hand.payload["played_at"]))
        candidates = [
            arrival
            for arrival in contexts
            if UserContextPayload.model_validate(arrival.event.payload).effective_at
            <= played_at
        ]
        return max(candidates, key=lambda value: _context_order(value.event), default=None)
