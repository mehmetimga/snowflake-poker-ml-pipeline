"""Action-level pattern recognition for the realtime hot path."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from typing import Any

from pipeline.realtime.pair_memory import pair_key, pair_key_string


ACTION_PATTERN_COLUMNS = [
    "pattern_id",
    "pattern_type",
    "hand_id",
    "table_id",
    "played_at",
    "street",
    "sequence_no",
    "pair_key",
    "player_a",
    "player_b",
    "actor_player_id",
    "counterparty_player_id",
    "pattern_score",
    "evidence",
]

AGGRESSIVE_ACTIONS = {"bet", "raise"}
PASSIVE_ACTIONS = {"check", "call"}


def action_events_from_hand(hand: dict) -> list[dict]:
    """Expand one complete hand JSON event into normalized action events."""
    players = {
        str(player["player_id"]): player
        for player in hand.get("players", [])
    }
    big_blind = max(float(hand.get("big_blind") or 1.0), 1e-6)
    events = []
    for action in sorted(hand.get("actions", []), key=lambda item: int(item["sequence_no"])):
        player_id = str(action["player_id"])
        player = players.get(player_id, {})
        sequence_no = int(action["sequence_no"])
        amount = float(action.get("amount", 0.0))
        events.append(
            {
                "action_event_id": f"AE-{hand['hand_id']}-{sequence_no:04d}",
                "hand_id": str(hand["hand_id"]),
                "table_id": hand.get("table_id"),
                "played_at": hand.get("played_at"),
                "sequence_no": sequence_no,
                "player_id": player_id,
                "position": player.get("position"),
                "street": str(action.get("street", "")).lower(),
                "action_type": str(action.get("action_type", "")).lower(),
                "amount": amount,
                "amount_bb": amount / big_blind,
                "big_blind": big_blind,
            }
        )
    return events


def detect_action_patterns(
    events: Iterable[dict],
    max_gap: int = 3,
    min_call_amount_bb: float = 2.0,
) -> list[dict]:
    """Detect short action motifs that are useful candidates for pair review.

    This intentionally keeps the hot-path logic lightweight. It emits candidate
    pair signals, not final accusations: downstream scoring/Qdrant/pair memory
    still decide whether the action motif matters.
    """
    grouped: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        grouped[str(event["hand_id"])].append(event)

    patterns: list[dict] = []
    for hand_events in grouped.values():
        ordered = sorted(hand_events, key=lambda item: int(item["sequence_no"]))
        last_aggressive: dict | None = None
        preflop_raises: list[dict] = []
        last_by_street: dict[str, dict] = {}

        for event in ordered:
            action_type = str(event.get("action_type", "")).lower()
            street = str(event.get("street", "")).lower()
            actor = str(event["player_id"])

            if street == "preflop" and action_type == "raise":
                previous = _latest_other_actor(preflop_raises, actor)
                if previous is not None:
                    prior_raise_count = len({str(row["player_id"]) for row in preflop_raises})
                    score = min(0.95, 0.62 + (0.08 * min(prior_raise_count, 3)) + _amount_bonus(event))
                    patterns.append(
                        _pattern_row(
                            pattern_type="preflop_squeeze",
                            actor=actor,
                            counterparty=str(previous["player_id"]),
                            event=event,
                            score=score,
                            evidence={
                                "prior_raise_count": prior_raise_count,
                                "previous_sequence_no": previous["sequence_no"],
                                "amount_bb": round(float(event.get("amount_bb", 0.0)), 3),
                            },
                        )
                    )
                preflop_raises.append(event)

            if action_type == "fold" and _is_recent_other(last_aggressive, event, max_gap):
                gap = int(event["sequence_no"]) - int(last_aggressive["sequence_no"])
                street_bonus = 0.05 if street == "preflop" else 0.0
                score = min(0.90, 0.55 + street_bonus + _gap_bonus(gap, max_gap))
                patterns.append(
                    _pattern_row(
                        pattern_type="raise_fold_benefit",
                        actor=actor,
                        counterparty=str(last_aggressive["player_id"]),
                        event=event,
                        score=score,
                        evidence={
                            "aggressor_sequence_no": last_aggressive["sequence_no"],
                            "gap": gap,
                            "aggressor_action": last_aggressive["action_type"],
                        },
                    )
                )

            if (
                action_type == "call"
                and float(event.get("amount_bb", 0.0)) >= min_call_amount_bb
                and _is_recent_other(last_aggressive, event, max_gap)
            ):
                gap = int(event["sequence_no"]) - int(last_aggressive["sequence_no"])
                score = min(0.88, 0.50 + _gap_bonus(gap, max_gap) + _amount_bonus(event))
                patterns.append(
                    _pattern_row(
                        pattern_type="call_down_transfer",
                        actor=actor,
                        counterparty=str(last_aggressive["player_id"]),
                        event=event,
                        score=score,
                        evidence={
                            "aggressor_sequence_no": last_aggressive["sequence_no"],
                            "gap": gap,
                            "amount_bb": round(float(event.get("amount_bb", 0.0)), 3),
                        },
                    )
                )

            previous_same_street = last_by_street.get(street)
            if (
                street in {"flop", "turn", "river"}
                and action_type in PASSIVE_ACTIONS
                and previous_same_street is not None
                and str(previous_same_street["player_id"]) != actor
                and str(previous_same_street.get("action_type", "")).lower() in PASSIVE_ACTIONS
            ):
                gap = int(event["sequence_no"]) - int(previous_same_street["sequence_no"])
                if gap <= max_gap:
                    score = min(0.78, 0.42 + _gap_bonus(gap, max_gap))
                    patterns.append(
                        _pattern_row(
                            pattern_type="soft_play_passive_chain",
                            actor=actor,
                            counterparty=str(previous_same_street["player_id"]),
                            event=event,
                            score=score,
                            evidence={
                                "previous_sequence_no": previous_same_street["sequence_no"],
                                "gap": gap,
                                "previous_action": previous_same_street["action_type"],
                            },
                        )
                    )

            if action_type in AGGRESSIVE_ACTIONS:
                last_aggressive = event
            last_by_street[street] = event

    return patterns


def action_pattern_scores_by_player(patterns: Iterable[dict]) -> dict[tuple[str, str], float]:
    """Collapse pair-level action motifs to per-player scores for live scoring."""
    out: dict[tuple[str, str], float] = {}
    for pattern in patterns:
        hand_id = str(pattern["hand_id"])
        score = float(pattern.get("pattern_score", 0.0))
        for player_id in (pattern.get("player_a"), pattern.get("player_b")):
            if not player_id:
                continue
            key = (hand_id, str(player_id))
            out[key] = max(out.get(key, 0.0), score)
    return out


def _pattern_row(
    pattern_type: str,
    actor: str,
    counterparty: str,
    event: dict,
    score: float,
    evidence: dict[str, Any],
) -> dict:
    player_a, player_b = pair_key(actor, counterparty)
    sequence_no = int(event["sequence_no"])
    return {
        "pattern_id": f"AP-{event['hand_id']}-{sequence_no:04d}-{pattern_type}-{pair_key_string(actor, counterparty)}",
        "pattern_type": pattern_type,
        "hand_id": str(event["hand_id"]),
        "table_id": event.get("table_id"),
        "played_at": event.get("played_at"),
        "street": event.get("street"),
        "sequence_no": sequence_no,
        "pair_key": pair_key_string(actor, counterparty),
        "player_a": player_a,
        "player_b": player_b,
        "actor_player_id": actor,
        "counterparty_player_id": counterparty,
        "pattern_score": float(max(0.0, min(1.0, score))),
        "evidence": evidence,
    }


def _latest_other_actor(events: list[dict], actor: str) -> dict | None:
    for event in reversed(events):
        if str(event["player_id"]) != actor:
            return event
    return None


def _is_recent_other(previous: dict | None, event: dict, max_gap: int) -> bool:
    if previous is None:
        return False
    if str(previous["player_id"]) == str(event["player_id"]):
        return False
    gap = int(event["sequence_no"]) - int(previous["sequence_no"])
    return 0 < gap <= max_gap


def _gap_bonus(gap: int, max_gap: int) -> float:
    if max_gap <= 0:
        return 0.0
    return 0.12 * max(0.0, (max_gap - gap + 1) / max_gap)


def _amount_bonus(event: dict) -> float:
    return min(0.12, float(event.get("amount_bb", 0.0)) / 40.0)
