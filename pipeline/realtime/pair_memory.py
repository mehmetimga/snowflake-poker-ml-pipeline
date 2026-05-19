"""Rolling player-pair memory for realtime pattern recognition."""

from __future__ import annotations

import json
from dataclasses import dataclass
from itertools import combinations
from collections.abc import Mapping

import pandas as pd


PAIR_KEY_SEPARATOR = "|"
PAIR_MEMORY_COLUMNS = [
    "hand_id",
    "player_a",
    "player_b",
    "hands_together",
    "chip_transfer_ratio",
    "soft_play_frequency",
    "fold_benefit_ratio",
    "showdown_avoidance_rate",
    "pair_memory_score",
]


def pair_key(player_a: object, player_b: object) -> tuple[str, str]:
    a, b = sorted((str(player_a), str(player_b)))
    return a, b


def pair_key_string(player_a: object, player_b: object) -> str:
    a, b = pair_key(player_a, player_b)
    return f"{a}{PAIR_KEY_SEPARATOR}{b}"


def pair_update_records_from_hand(hand: dict) -> list[dict]:
    """Expand one complete hand event into normalized pair-memory updates."""
    rows = []
    hand_id = str(hand["hand_id"])
    players = sorted(hand.get("players") or [], key=lambda player: str(player["player_id"]))
    for left, right in combinations(players, 2):
        a, b = pair_key(left["player_id"], right["player_id"])
        won_a = left["won_amount"] if str(left["player_id"]) == a else right["won_amount"]
        won_b = right["won_amount"] if str(right["player_id"]) == b else left["won_amount"]
        rows.append(
            {
                "pair_key": pair_key_string(a, b),
                "hand_id": hand_id,
                "table_id": hand.get("table_id"),
                "played_at": hand.get("played_at"),
                "player_a": a,
                "player_b": b,
                "won_amount_a": float(won_a),
                "won_amount_b": float(won_b),
            }
        )
    return rows


@dataclass
class PairState:
    player_a: str
    player_b: str
    hands_together: int = 0
    won_amount_a: float = 0.0
    won_amount_b: float = 0.0
    no_cowin_hands: int = 0
    last_hand_id: str = ""
    last_seen_index: int = 0

    def update(self, hand_id: str, won_a: float, won_b: float, seen_index: int) -> None:
        self.hands_together += 1
        self.won_amount_a += won_a
        self.won_amount_b += won_b
        if not (won_a > 0 and won_b > 0):
            self.no_cowin_hands += 1
        self.last_hand_id = hand_id
        self.last_seen_index = seen_index

    @property
    def chip_transfer_ratio(self) -> float:
        return self.won_amount_b / (abs(self.won_amount_a) + 1e-6)

    @property
    def soft_play_frequency(self) -> float:
        if self.hands_together == 0:
            return 0.0
        return self.no_cowin_hands / self.hands_together

    @property
    def fold_benefit_ratio(self) -> float:
        if self.hands_together == 0:
            return 0.0
        mean_a = self.won_amount_a / self.hands_together
        mean_b = self.won_amount_b / self.hands_together
        return mean_b / (abs(mean_a) + 1e-6)

    @property
    def showdown_avoidance_rate(self) -> float:
        return self.soft_play_frequency

    @property
    def pair_memory_score(self) -> float:
        repeat = min(1.0, self.hands_together / 5.0)
        transfer = min(1.0, abs(self.chip_transfer_ratio) / 3.0)
        fold_benefit = min(1.0, abs(self.fold_benefit_ratio) / 3.0)
        score = (
            0.25 * repeat
            + 0.30 * self.soft_play_frequency
            + 0.25 * transfer
            + 0.20 * fold_benefit
        )
        return float(max(0.0, min(1.0, score)))

    def to_row(self, hand_id: str | None = None) -> dict:
        return {
            "hand_id": hand_id or self.last_hand_id,
            "player_a": self.player_a,
            "player_b": self.player_b,
            "hands_together": int(self.hands_together),
            "chip_transfer_ratio": float(self.chip_transfer_ratio),
            "soft_play_frequency": float(self.soft_play_frequency),
            "fold_benefit_ratio": float(self.fold_benefit_ratio),
            "showdown_avoidance_rate": float(self.showdown_avoidance_rate),
            "pair_memory_score": float(self.pair_memory_score),
        }

    def to_state_dict(self) -> dict:
        return {
            "player_a": self.player_a,
            "player_b": self.player_b,
            "hands_together": int(self.hands_together),
            "won_amount_a": float(self.won_amount_a),
            "won_amount_b": float(self.won_amount_b),
            "no_cowin_hands": int(self.no_cowin_hands),
            "last_hand_id": self.last_hand_id,
            "last_seen_index": int(self.last_seen_index),
        }

    @classmethod
    def from_state_dict(cls, raw: dict) -> "PairState":
        return cls(
            player_a=str(raw["player_a"]),
            player_b=str(raw["player_b"]),
            hands_together=int(raw.get("hands_together", 0)),
            won_amount_a=float(raw.get("won_amount_a", 0.0)),
            won_amount_b=float(raw.get("won_amount_b", 0.0)),
            no_cowin_hands=int(raw.get("no_cowin_hands", 0)),
            last_hand_id=str(raw.get("last_hand_id", "")),
            last_seen_index=int(raw.get("last_seen_index", 0)),
        )


def pair_state_from_update(update: dict, state: PairState | None = None) -> PairState:
    if state is None:
        state = PairState(
            player_a=str(update["player_a"]),
            player_b=str(update["player_b"]),
        )
    state.update(
        hand_id=str(update["hand_id"]),
        won_a=float(update.get("won_amount_a", 0.0)),
        won_b=float(update.get("won_amount_b", 0.0)),
        seen_index=state.last_seen_index + 1,
    )
    return state


def pair_memory_frame_for_hand(
    hand: dict,
    pair_memory_by_key: Mapping[str, object],
) -> pd.DataFrame:
    """Build pair-memory feature rows for pairs seated in a hand.

    `pair_memory_by_key` values can be dictionaries or serialized JSON rows from
    the `pair.memory` topic.
    """
    rows = []
    for update in pair_update_records_from_hand(hand):
        raw = pair_memory_by_key.get(update["pair_key"])
        if raw is None:
            continue
        row = json.loads(raw) if isinstance(raw, str) else dict(raw)
        row["hand_id"] = str(hand["hand_id"])
        row.setdefault("player_a", update["player_a"])
        row.setdefault("player_b", update["player_b"])
        for col in PAIR_MEMORY_COLUMNS:
            row.setdefault(col, 0.0 if col not in ("hand_id", "player_a", "player_b") else "")
        rows.append({col: row[col] for col in PAIR_MEMORY_COLUMNS})
    return pd.DataFrame(rows, columns=PAIR_MEMORY_COLUMNS)


class RollingPairMemory:
    """Small in-process rolling state keyed by normalized player pair."""

    def __init__(self, max_pairs: int = 10000) -> None:
        self.max_pairs = max_pairs
        self._states: dict[tuple[str, str], PairState] = {}
        self._seen_index = 0

    def __len__(self) -> int:
        return len(self._states)

    def update_from_players(self, players: pd.DataFrame) -> pd.DataFrame:
        if players.empty:
            return pd.DataFrame()

        rows = []
        for hand_id, group in players.groupby("hand_id"):
            ordered = group.sort_values("player_id")
            for left, right in combinations(ordered.to_dict("records"), 2):
                a, b = pair_key(left["player_id"], right["player_id"])
                won_a = float(left["won_amount"] if str(left["player_id"]) == a else right["won_amount"])
                won_b = float(right["won_amount"] if str(right["player_id"]) == b else left["won_amount"])
                state = self._states.get((a, b))
                if state is None:
                    state = PairState(player_a=a, player_b=b)
                    self._states[(a, b)] = state
                self._seen_index += 1
                state.update(str(hand_id), won_a, won_b, self._seen_index)
                rows.append(state.to_row(str(hand_id)))

        self._evict_if_needed()
        return pd.DataFrame(rows)

    def snapshot(self) -> pd.DataFrame:
        return pd.DataFrame([state.to_row() for state in self._states.values()])

    def _evict_if_needed(self) -> None:
        if self.max_pairs <= 0 or len(self._states) <= self.max_pairs:
            return
        keep = sorted(
            self._states.items(),
            key=lambda item: item[1].last_seen_index,
            reverse=True,
        )[: self.max_pairs]
        self._states = dict(keep)
