"""Deterministic offline oracle for version-one pair-feature snapshots.

The state machine mirrors the native Flink job.  Rolling values are captured
before the current hand is applied, corrections reuse the original historical
snapshot, and duplicate deliveries never advance state.
"""

from __future__ import annotations

import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime
from itertools import combinations
from typing import Iterable, Mapping

from pipeline.events import (
    PAIR_FEATURES_COMPUTED,
    PAIR_FEATURE_DEFINITION_VERSION,
    CurrentHandPairFeatures,
    PairContextFeatures,
    PairFeatureEvent,
    PairFeaturePayload,
    PairHistoryFeatures,
    PlayerHandContextEvent,
    UserHistoryFeatures,
)


_POSITION_INDEX = {"UTG": 0, "MP": 1, "CO": 2, "BTN": 3, "SB": 4, "BB": 5}
_BANKROLL_INDEX = {"low": 0, "medium": 1, "high": 2}
_STAKE_INDEX = {"micro": 0, "low": 1, "medium": 2, "high": 3}
_EPSILON = 1e-9


def _q(value: float) -> float:
    """Canonical cross-language precision for feature contract floats."""
    return round(float(value), 9)


def canonical_pair(player_left: str, player_right: str) -> tuple[str, str, str]:
    player_a, player_b = sorted((str(player_left), str(player_right)))
    if player_a == player_b:
        raise ValueError("pair endpoints must differ")
    return player_a, player_b, f"{player_a}:{player_b}"


def _player_actions(event: PlayerHandContextEvent) -> list[object]:
    player_id = event.payload.player.player_id
    return [action for action in event.payload.actions if action.player_id == player_id]


def _action_summary(event: PlayerHandContextEvent) -> dict[str, float | int | bool]:
    actions = _player_actions(event)
    return {
        "invested": float(sum(action.amount for action in actions)),
        "aggressive": sum(action.action_type in ("bet", "raise") for action in actions),
        "folds": sum(action.action_type == "fold" for action in actions),
        "raised": any(action.action_type == "raise" for action in actions),
        "saw_flop": any(action.street == "flop" for action in actions),
        "saw_river": any(action.street == "river" for action in actions),
    }


@dataclass
class _UserAggregate:
    hands: int = 0
    total_won: float = 0.0
    fold_hands: int = 0
    raise_hands: int = 0
    saw_flop_hands: int = 0
    last_played_at: datetime | None = None

    def snapshot(self) -> UserHistoryFeatures:
        divisor = self.hands or 1
        return UserHistoryFeatures(
            hands_seen=self.hands,
            total_won_amount=_q(self.total_won),
            mean_won_amount=_q(self.total_won / divisor),
            fold_rate=_q(self.fold_hands / divisor),
            raise_rate=_q(self.raise_hands / divisor),
            saw_flop_rate=_q(self.saw_flop_hands / divisor),
        )

    def update(self, event: PlayerHandContextEvent) -> None:
        played_at = event.payload.played_at
        if self.last_played_at is not None and played_at < self.last_played_at:
            raise ValueError("new player hands must be processed in event-time order")
        summary = _action_summary(event)
        self.hands += 1
        self.total_won += event.payload.player.won_amount
        self.fold_hands += int(bool(summary["folds"]))
        self.raise_hands += int(bool(summary["raised"]))
        self.saw_flop_hands += int(bool(summary["saw_flop"]))
        self.last_played_at = played_at


@dataclass
class _PairAggregate:
    hands: int = 0
    total_won_a: float = 0.0
    total_won_b: float = 0.0
    a_fold_b_win_hands: int = 0
    b_fold_a_win_hands: int = 0
    both_saw_flop_hands: int = 0
    table_counts: Counter[str] = field(default_factory=Counter)
    last_played_at: datetime | None = None

    def snapshot(self, table_id: str, played_at: datetime) -> PairHistoryFeatures:
        divisor = self.hands or 1
        outcome_total = self.total_won_a + self.total_won_b
        age = None
        if self.last_played_at is not None:
            age = max(0.0, (played_at - self.last_played_at).total_seconds())
        return PairHistoryFeatures(
            hands_together=self.hands,
            total_won_amount_a=_q(self.total_won_a),
            total_won_amount_b=_q(self.total_won_b),
            outcome_asymmetry=(
                _q(abs(self.total_won_a - self.total_won_b) / (outcome_total + _EPSILON))
                if outcome_total > 0
                else 0.0
            ),
            a_fold_b_win_rate=_q(self.a_fold_b_win_hands / divisor),
            b_fold_a_win_rate=_q(self.b_fold_a_win_hands / divisor),
            both_saw_flop_rate=_q(self.both_saw_flop_hands / divisor),
            same_table_rate=_q(self.table_counts[table_id] / divisor),
            last_seen_age_seconds=(_q(age) if age is not None else None),
        )

    def update(
        self,
        event_a: PlayerHandContextEvent,
        event_b: PlayerHandContextEvent,
    ) -> None:
        played_at = event_a.payload.played_at
        if self.last_played_at is not None and played_at < self.last_played_at:
            raise ValueError("new pair hands must be processed in event-time order")
        summary_a = _action_summary(event_a)
        summary_b = _action_summary(event_b)
        won_a = event_a.payload.player.won_amount
        won_b = event_b.payload.player.won_amount
        self.hands += 1
        self.total_won_a += won_a
        self.total_won_b += won_b
        self.a_fold_b_win_hands += int(bool(summary_a["folds"]) and won_b > 0)
        self.b_fold_a_win_hands += int(bool(summary_b["folds"]) and won_a > 0)
        self.both_saw_flop_hands += int(
            bool(summary_a["saw_flop"]) and bool(summary_b["saw_flop"])
        )
        self.table_counts[event_a.payload.table_id] += 1
        self.last_played_at = played_at


def _current_hand_features(
    event_a: PlayerHandContextEvent,
    event_b: PlayerHandContextEvent,
) -> CurrentHandPairFeatures:
    summary_a = _action_summary(event_a)
    summary_b = _action_summary(event_b)
    payload = event_a.payload
    pot = max(payload.pot_size, _EPSILON)
    won_a = payload.player.won_amount
    won_b = event_b.payload.player.won_amount
    position_a = _POSITION_INDEX[payload.player.position]
    position_b = _POSITION_INDEX[event_b.payload.player.position]
    invested_a = float(summary_a["invested"])
    invested_b = float(summary_b["invested"])
    return CurrentHandPairFeatures(
        position_index_a=position_a,
        position_index_b=position_b,
        position_gap=abs(position_a - position_b),
        invested_amount_a=_q(invested_a),
        invested_amount_b=_q(invested_b),
        invested_pot_ratio_a=_q(invested_a / pot),
        invested_pot_ratio_b=_q(invested_b / pot),
        invested_abs_diff_ratio=_q(abs(invested_a - invested_b) / pot),
        won_amount_a=_q(won_a),
        won_amount_b=_q(won_b),
        outcome_abs_diff_ratio=_q(abs(won_a - won_b) / pot),
        aggressive_actions_a=int(summary_a["aggressive"]),
        aggressive_actions_b=int(summary_b["aggressive"]),
        fold_actions_a=int(summary_a["folds"]),
        fold_actions_b=int(summary_b["folds"]),
        both_saw_flop=bool(summary_a["saw_flop"] and summary_b["saw_flop"]),
        both_saw_river=bool(summary_a["saw_river"] and summary_b["saw_river"]),
        one_folded_other_won=bool(
            (summary_a["folds"] and won_b > 0)
            or (summary_b["folds"] and won_a > 0)
        ),
    )


def _context_features(
    event_a: PlayerHandContextEvent,
    event_b: PlayerHandContextEvent,
) -> PairContextFeatures:
    played_at = event_a.payload.played_at
    context_a = event_a.payload.context
    context_b = event_b.payload.context

    def age_days(context: object | None) -> float:
        if context is None:
            return 0.0
        return max(0.0, (played_at - context.account_created_at).total_seconds() / 86_400)

    def same(field_name: str) -> bool:
        return bool(
            context_a is not None
            and context_b is not None
            and getattr(context_a, field_name) == getattr(context_b, field_name)
        )

    skill_a = context_a.skill_rating if context_a is not None else 0.0
    skill_b = context_b.skill_rating if context_b is not None else 0.0
    age_a = age_days(context_a)
    age_b = age_days(context_b)
    bankroll_a = _BANKROLL_INDEX[context_a.bankroll_bucket] if context_a else 0
    bankroll_b = _BANKROLL_INDEX[context_b.bankroll_bucket] if context_b else 0
    stake_a = _STAKE_INDEX[context_a.preferred_stake_bucket] if context_a else 0
    stake_b = _STAKE_INDEX[context_b.preferred_stake_bucket] if context_b else 0
    return PairContextFeatures(
        context_missing_a=context_a is None,
        context_missing_b=context_b is None,
        skill_rating_a=_q(skill_a),
        skill_rating_b=_q(skill_b),
        skill_rating_abs_diff=_q(abs(skill_a - skill_b)),
        account_age_days_a=_q(age_a),
        account_age_days_b=_q(age_b),
        account_age_abs_diff_days=_q(abs(age_a - age_b)),
        same_country=same("country_bucket"),
        same_timezone=same("timezone"),
        same_acquisition_channel=same("acquisition_channel"),
        same_device=same("device_id"),
        same_network=same("network_cluster_id"),
        bankroll_bucket_distance=abs(bankroll_a - bankroll_b),
        preferred_stake_bucket_distance=abs(stake_a - stake_b),
    )


def _event_id(
    event_a: PlayerHandContextEvent,
    event_b: PlayerHandContextEvent,
    pair_key: str,
) -> uuid.UUID:
    return uuid.uuid5(
        uuid.NAMESPACE_URL,
        ":".join(
            (
                event_a.dataset_id,
                event_a.dataset_split,
                PAIR_FEATURES_COMPUTED,
                str(event_a.payload.source_hand_event_id),
                pair_key,
                str(event_a.event_id),
                str(event_b.event_id),
                PAIR_FEATURE_DEFINITION_VERSION,
            )
        ),
    )


class PairFeatureCore:
    """Correction-aware state machine used by batch backfills and tests."""

    def __init__(self) -> None:
        self._users: dict[str, _UserAggregate] = {}
        self._user_hand_history: dict[tuple[str, str], UserHistoryFeatures] = {}
        self._hands: dict[str, dict[str, PlayerHandContextEvent]] = {}
        self._pair_signatures: dict[tuple[str, str], tuple[uuid.UUID, uuid.UUID]] = {}
        self._pair_revisions: Counter[tuple[str, str]] = Counter()
        self._pairs: dict[str, _PairAggregate] = {}
        self._pair_hand_history: dict[tuple[str, str], PairHistoryFeatures] = {}

    def process(
        self, raw_event: PlayerHandContextEvent | Mapping[str, object]
    ) -> list[PairFeatureEvent]:
        event = (
            raw_event
            if isinstance(raw_event, PlayerHandContextEvent)
            else PlayerHandContextEvent.model_validate(raw_event)
        )
        payload = event.payload
        player_id = payload.player.player_id
        hand_id = payload.hand_id
        user_hand_key = (player_id, hand_id)
        if user_hand_key not in self._user_hand_history:
            aggregate = self._users.setdefault(player_id, _UserAggregate())
            self._user_hand_history[user_hand_key] = aggregate.snapshot()
            aggregate.update(event)

        hand = self._hands.setdefault(hand_id, {})
        previous = hand.get(player_id)
        if previous is not None:
            if event.payload.revision < previous.payload.revision:
                return []
            if event.payload.revision == previous.payload.revision:
                if event.event_id != previous.event_id:
                    raise ValueError("same player-hand revision has conflicting event IDs")
                return []
        hand[player_id] = event
        if len(hand) < payload.num_players:
            return []
        self._validate_complete_hand(hand.values())

        output: list[PairFeatureEvent] = []
        for player_a, player_b in combinations(sorted(hand), 2):
            event_a = hand[player_a]
            event_b = hand[player_b]
            _, _, pair_key = canonical_pair(player_a, player_b)
            identity = (hand_id, pair_key)
            signature = (event_a.event_id, event_b.event_id)
            if self._pair_signatures.get(identity) == signature:
                continue
            self._pair_signatures[identity] = signature
            self._pair_revisions[identity] += 1
            output.append(
                self._build_pair_event(
                    event_a,
                    event_b,
                    pair_key,
                    self._pair_revisions[identity],
                )
            )
        return output

    def process_many(
        self, events: Iterable[PlayerHandContextEvent | Mapping[str, object]]
    ) -> list[PairFeatureEvent]:
        return [output for event in events for output in self.process(event)]

    @staticmethod
    def _validate_complete_hand(events: Iterable[PlayerHandContextEvent]) -> None:
        values = list(events)
        first = values[0]
        expected = (
            first.dataset_id,
            first.dataset_split,
            first.payload.source_hand_event_id,
            first.payload.table_id,
            first.payload.played_at,
            first.payload.num_players,
        )
        if len(values) != first.payload.num_players:
            raise ValueError("assembled hand has an unexpected player count")
        for event in values[1:]:
            actual = (
                event.dataset_id,
                event.dataset_split,
                event.payload.source_hand_event_id,
                event.payload.table_id,
                event.payload.played_at,
                event.payload.num_players,
            )
            if actual != expected:
                raise ValueError("player rows disagree on hand metadata")

    def _build_pair_event(
        self,
        event_a: PlayerHandContextEvent,
        event_b: PlayerHandContextEvent,
        pair_key: str,
        snapshot_revision: int,
    ) -> PairFeatureEvent:
        payload_a = event_a.payload
        payload_b = event_b.payload
        pair_hand_key = (pair_key, payload_a.hand_id)
        pair_aggregate = self._pairs.setdefault(pair_key, _PairAggregate())
        pair_history = self._pair_hand_history.get(pair_hand_key)
        if pair_history is None:
            pair_history = pair_aggregate.snapshot(payload_a.table_id, payload_a.played_at)
            self._pair_hand_history[pair_hand_key] = pair_history
            pair_aggregate.update(event_a, event_b)

        payload = PairFeaturePayload(
            hand_id=payload_a.hand_id,
            table_id=payload_a.table_id,
            played_at=payload_a.played_at,
            pair_key=pair_key,
            player_a=payload_a.player.player_id,
            player_b=payload_b.player.player_id,
            num_players=payload_a.num_players,
            source_hand_event_id=payload_a.source_hand_event_id,
            source_player_context_event_id_a=event_a.event_id,
            source_player_context_event_id_b=event_b.event_id,
            source_revision_a=payload_a.revision,
            source_revision_b=payload_b.revision,
            context_status_a=payload_a.context_status,
            context_status_b=payload_b.context_status,
            context_version_a=payload_a.context_version,
            context_version_b=payload_b.context_version,
            snapshot_revision=snapshot_revision,
            current_hand=_current_hand_features(event_a, event_b),
            context=_context_features(event_a, event_b),
            user_history_a=self._user_hand_history[(payload_a.player.player_id, payload_a.hand_id)],
            user_history_b=self._user_hand_history[(payload_b.player.player_id, payload_b.hand_id)],
            pair_history=pair_history,
        )
        return PairFeatureEvent(
            event_id=_event_id(event_a, event_b, pair_key),
            tenant_id=event_a.tenant_id,
            product_id=event_a.product_id,
            dataset_id=event_a.dataset_id,
            dataset_split=event_a.dataset_split,
            occurred_at=payload_a.played_at,
            emitted_at=max(event_a.emitted_at, event_b.emitted_at),
            trace_id=event_a.trace_id,
            payload=payload,
        )
