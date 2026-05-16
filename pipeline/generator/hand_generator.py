"""Synthetic 6-max NLHE cash hand generator with optional collusion injection.

All players, IDs, and chip flow are entirely synthetic. No real hand history
or player handle is referenced.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Optional

from .collusion_patterns import CollusionPair, CollusionPattern


_ADJECTIVES = [
    "Blaze", "Frost", "Storm", "Shadow", "Crimson", "Silent", "Ember", "Lunar",
    "Solar", "Iron", "Rapid", "Wild", "Steady", "Cold", "Golden", "Quiet",
]
_NOUNS = [
    "Phoenix", "Falcon", "Wolf", "Tiger", "Eagle", "Comet", "Drake", "Hawk",
    "Lynx", "Otter", "Raven", "Stoat", "Viper", "Whale", "Bison", "Jackal",
]
_STAKES = [(0.10, 0.25), (0.50, 1.00), (1.00, 2.00), (2.00, 5.00)]
_POSITIONS_6MAX = ["UTG", "MP", "CO", "BTN", "SB", "BB"]
_RANKS = "23456789TJQKA"
_SUITS = "cdhs"


def _make_name(rng: random.Random) -> str:
    return f"{rng.choice(_ADJECTIVES)}{rng.choice(_NOUNS)}{rng.randint(10, 99)}"


def _deal_card(deck: list[str], rng: random.Random) -> str:
    idx = rng.randrange(len(deck))
    return deck.pop(idx)


def _rank_value(card: str) -> int:
    return _RANKS.index(card[0])


def _hole_strength(c1: str, c2: str) -> float:
    """A rough hand strength score in [0, 1] for choosing actions."""
    r1, r2 = _rank_value(c1), _rank_value(c2)
    high = max(r1, r2)
    low = min(r1, r2)
    suited = c1[1] == c2[1]
    pair = r1 == r2
    score = (high + low) / 24.0
    if pair:
        score += 0.25
    if suited:
        score += 0.05
    if high - low <= 1:
        score += 0.03
    return min(1.0, score)


@dataclass
class GeneratorConfig:
    n_hands: int = 5000
    n_players: int = 200
    n_tables: int = 20
    n_colluding_pairs: int = 30
    seed: int = 42


@dataclass
class _Player:
    player_id: str
    name: str
    bankroll: float = 1000.0
    is_colluder: bool = False
    pair: Optional[CollusionPair] = None


class HandGenerator:
    """Build synthetic NLHE cash hands as plain JSON-serializable dicts."""

    def __init__(self, config: GeneratorConfig) -> None:
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.players = self._make_players()
        self.pairs = self._make_pairs()
        self._assign_pairs()
        self.tables = [f"table_{i:02d}" for i in range(config.n_tables)]
        self._t0 = datetime(2026, 5, 1, tzinfo=timezone.utc)

    def _make_players(self) -> list[_Player]:
        players: list[_Player] = []
        used: set[str] = set()
        for _ in range(self.cfg.n_players):
            name = _make_name(self.rng)
            while name in used:
                name = _make_name(self.rng)
            used.add(name)
            players.append(_Player(player_id=str(uuid.uuid4()), name=name))
        return players

    def _make_pairs(self) -> list[CollusionPair]:
        patterns = list(CollusionPattern)
        ids = list(range(self.cfg.n_players))
        self.rng.shuffle(ids)
        pairs: list[CollusionPair] = []
        for i in range(self.cfg.n_colluding_pairs):
            a, b = ids[2 * i], ids[2 * i + 1]
            pat = patterns[i % len(patterns)]
            pairs.append(
                CollusionPair(
                    pair_id=f"pair_{i:03d}",
                    player_a=self.players[a].player_id,
                    player_b=self.players[b].player_id,
                    pattern=pat,
                    intensity=self.rng.uniform(0.6, 0.9),
                )
            )
        return pairs

    def _assign_pairs(self) -> None:
        for pair in self.pairs:
            for p in self.players:
                if pair.involves(p.player_id):
                    p.is_colluder = True
                    p.pair = pair

    # ------------------------------------------------------------------ hands

    def iter_hands(self) -> Iterator[dict]:
        for i in range(self.cfg.n_hands):
            yield self._generate_hand(i)

    def _generate_hand(self, idx: int) -> dict:
        seats = self.rng.sample(self.players, k=6)
        sb, bb = self.rng.choice(_STAKES)
        played_at = self._t0 + timedelta(seconds=idx * 30)
        table_id = self.rng.choice(self.tables)

        deck = [r + s for r in _RANKS for s in _SUITS]
        self.rng.shuffle(deck)
        holes: dict[str, tuple[str, str]] = {}
        for p in seats:
            holes[p.player_id] = (_deal_card(deck, self.rng), _deal_card(deck, self.rng))
        board = [_deal_card(deck, self.rng) for _ in range(5)]

        # Identify active colluding pair seated at this hand, if any.
        active_pair: Optional[CollusionPair] = None
        for pair in self.pairs:
            members = [s for s in seats if pair.involves(s.player_id)]
            if len(members) == 2 and self.rng.random() < pair.intensity:
                active_pair = pair
                break

        actions, pot, invested, alive, last_street_seen = self._play_streets(
            seats, sb, bb, holes, active_pair
        )

        winners = self._settle(seats, alive, holes, board, pot)
        players_payload = []
        for p in seats:
            won = sum(w["amount"] for w in winners if w["player_id"] == p.player_id)
            players_payload.append(
                {
                    "player_id": p.player_id,
                    "name": p.name,
                    "position": _POSITIONS_6MAX[seats.index(p)],
                    "stack_start": 100.0 * bb,
                    "hole_cards": f"{holes[p.player_id][0]} {holes[p.player_id][1]}",
                    "won_amount": won,
                    "is_suspicious": active_pair is not None and active_pair.involves(p.player_id),
                    "collusion_pair_id": active_pair.pair_id if (active_pair and active_pair.involves(p.player_id)) else None,
                }
            )

        return {
            "hand_id": f"H-{idx:08d}",
            "table_id": table_id,
            "played_at": played_at.isoformat(),
            "small_blind": sb,
            "big_blind": bb,
            "num_players": len(seats),
            "pot_size": pot,
            "board": board[:last_street_seen],
            "actions": actions,
            "players": players_payload,
        }

    # ----------------------------------------------------------- street play

    def _play_streets(
        self,
        seats: list[_Player],
        sb: float,
        bb: float,
        holes: dict[str, tuple[str, str]],
        active_pair: Optional[CollusionPair],
    ) -> tuple[list[dict], float, dict[str, float], set[str], int]:
        invested: dict[str, float] = {p.player_id: 0.0 for p in seats}
        # Blinds
        invested[seats[4].player_id] += sb
        invested[seats[5].player_id] += bb
        pot = sb + bb
        actions: list[dict] = []
        sequence = 0

        def add(player_id: str, street: str, action_type: str, amount: float) -> None:
            nonlocal sequence
            actions.append(
                {
                    "sequence_no": sequence,
                    "player_id": player_id,
                    "street": street,
                    "action_type": action_type,
                    "amount": amount,
                }
            )
            sequence += 1

        # Preflop: action from UTG to BB
        alive: set[str] = {p.player_id for p in seats}
        preflop_order = seats[:4] + seats[4:6]  # UTG, MP, CO, BTN, SB, BB
        current_bet = bb
        preflop_raises = 0

        for p in preflop_order:
            if p.player_id not in alive:
                continue
            strength = _hole_strength(*holes[p.player_id])
            to_call = current_bet - invested[p.player_id]
            decision = self._preflop_decision(
                p, seats, strength, to_call, bb, active_pair, alive, preflop_raises
            )
            if decision == "fold":
                add(p.player_id, "preflop", "fold", 0.0)
                alive.discard(p.player_id)
            elif decision == "call":
                amount = max(to_call, 0.0)
                invested[p.player_id] += amount
                pot += amount
                add(p.player_id, "preflop", "call", amount)
            else:  # raise
                raise_to = current_bet * 3.0
                add_amt = raise_to - invested[p.player_id]
                invested[p.player_id] += add_amt
                pot += add_amt
                current_bet = raise_to
                preflop_raises += 1
                add(p.player_id, "preflop", "raise", add_amt)

        last_street_seen = 0
        if len(alive) < 2:
            return actions, pot, invested, alive, last_street_seen

        # Flop / Turn / River
        for street_idx, street_name in enumerate(("flop", "turn", "river")):
            last_street_seen = 3 + street_idx  # number of board cards revealed
            current_bet = 0.0
            for p in seats:
                if p.player_id not in alive:
                    continue
                strength = _hole_strength(*holes[p.player_id])
                decision = self._postflop_decision(
                    p, strength, current_bet, invested[p.player_id], pot, active_pair, alive, street_name
                )
                if decision == "fold":
                    add(p.player_id, street_name, "fold", 0.0)
                    alive.discard(p.player_id)
                elif decision == "check":
                    add(p.player_id, street_name, "check", 0.0)
                elif decision == "call":
                    amount = current_bet - 0.0  # postflop tracks per-street
                    pot += amount
                    invested[p.player_id] += amount
                    add(p.player_id, street_name, "call", amount)
                elif decision == "bet":
                    amount = max(bb * 2.0, pot * 0.6)
                    pot += amount
                    invested[p.player_id] += amount
                    current_bet = amount
                    add(p.player_id, street_name, "bet", amount)
            if len(alive) < 2:
                break

        return actions, pot, invested, alive, last_street_seen

    def _preflop_decision(
        self,
        player: _Player,
        seats: list[_Player],
        strength: float,
        to_call: float,
        bb: float,
        active_pair: Optional[CollusionPair],
        alive: set[str],
        preflop_raises: int,
    ) -> str:
        partner_in = (
            active_pair is not None
            and active_pair.involves(player.player_id)
            and active_pair.partner_of(player.player_id) in alive
        )
        # SQUEEZE_COLLUDE — re-raise to isolate when partner is in and there has been a preflop raise
        if (
            partner_in
            and active_pair.pattern == CollusionPattern.SQUEEZE_COLLUDE
            and preflop_raises >= 1
        ):
            return "raise"
        # FOLD_BENEFIT — one folds when partner has raised (clear up the pot)
        if (
            partner_in
            and active_pair.pattern == CollusionPattern.FOLD_BENEFIT
            and preflop_raises >= 1
        ):
            return "fold"

        if strength > 0.65:
            return "raise"
        if strength > 0.45 or to_call < bb * 0.6:
            return "call"
        return "fold"

    def _postflop_decision(
        self,
        player: _Player,
        strength: float,
        current_bet: float,
        already_in: float,
        pot: float,
        active_pair: Optional[CollusionPair],
        alive: set[str],
        street: str,
    ) -> str:
        partner_in = (
            active_pair is not None
            and active_pair.involves(player.player_id)
            and active_pair.partner_of(player.player_id) in alive
        )

        # SOFT_PLAY: never bet/raise if partner is still in pot — check or call only
        if partner_in and active_pair.pattern == CollusionPattern.SOFT_PLAY:
            if current_bet == 0.0:
                return "check"
            return "call" if strength > 0.35 else "fold"

        # CHIP_DUMP: weak hand calls down partner's bets
        if (
            partner_in
            and active_pair.pattern == CollusionPattern.CHIP_DUMP
            and current_bet > 0.0
        ):
            return "call"

        # HIGH_INVEST_FOLD synthetic signal: if heavily committed and weak, fold (creates rule-engine signal)
        invested_ratio = already_in / max(pot, 1e-6)
        if invested_ratio > 0.3 and strength < 0.3 and current_bet > 0:
            return "fold"

        if current_bet == 0.0:
            return "bet" if strength > 0.55 else "check"
        if strength > 0.6:
            return "bet"
        if strength > 0.4:
            return "call"
        return "fold"

    # ---------------------------------------------------------------- settle

    def _settle(
        self,
        seats: list[_Player],
        alive: set[str],
        holes: dict[str, tuple[str, str]],
        board: list[str],
        pot: float,
    ) -> list[dict]:
        if not alive:
            return []
        if len(alive) == 1:
            (winner,) = alive
            return [{"player_id": winner, "amount": pot}]
        # Showdown: pick highest holecard sum as a rough winner
        best, best_score = None, -1.0
        for pid in alive:
            score = _hole_strength(*holes[pid])
            if score > best_score:
                best, best_score = pid, score
        return [{"player_id": best, "amount": pot}]


def generate_hands(config: GeneratorConfig | None = None) -> Iterable[dict]:
    return HandGenerator(config or GeneratorConfig()).iter_hands()
