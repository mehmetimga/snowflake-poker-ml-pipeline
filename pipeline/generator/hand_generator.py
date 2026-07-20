"""Deterministic PokerKit-backed 6-max NLHE hand generator.

All players, IDs, chip flow, and labels are synthetic. PokerKit owns the game
state so emitted actions follow legal betting order and pots are settled with a
real Texas Hold'em evaluator. The local random generator only chooses players,
cards, and strategy decisions, which keeps a given seed exactly replayable.
"""

from __future__ import annotations

import random
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Optional

from pokerkit import Automation, BoardDealing, Card, ChipsPushing, Mode, NoLimitTexasHoldem

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

# PokerKit indexes a six-player blind game as SB, BB, UTG, MP, CO, BTN.
_POSITIONS_6MAX = ("SB", "BB", "UTG", "MP", "CO", "BTN")
_STREETS = ("preflop", "flop", "turn", "river")
_RANKS = "23456789TJQKA"
_SUITS = "cdhs"
_AUTOMATIONS = (
    Automation.ANTE_POSTING,
    Automation.BET_COLLECTION,
    Automation.BLIND_OR_STRADDLE_POSTING,
    Automation.CARD_BURNING,
    Automation.BOARD_DEALING,
    Automation.RUNOUT_COUNT_SELECTION,
    Automation.HOLE_CARDS_SHOWING_OR_MUCKING,
    Automation.HAND_KILLING,
    Automation.CHIPS_PUSHING,
    Automation.CHIPS_PULLING,
)


def _make_name(rng: random.Random) -> str:
    return f"{rng.choice(_ADJECTIVES)}{rng.choice(_NOUNS)}{rng.randint(10, 99)}"


def _rank_value(card: str) -> int:
    return _RANKS.index(card[0])


def _hole_strength(c1: str, c2: str) -> float:
    """Return a deliberately simple pre-flop policy score in ``[0, 1]``."""
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


@dataclass(frozen=True)
class GeneratorConfig:
    n_hands: int = 5000
    n_players: int = 200
    n_tables: int = 20
    n_colluding_pairs: int = 30
    seed: int = 42
    dataset_split: str = "live"
    dataset_id: str | None = None

    def __post_init__(self) -> None:
        if self.n_hands < 0:
            raise ValueError("n_hands must be non-negative")
        if self.n_players < 6:
            raise ValueError("n_players must be at least 6")
        if self.n_tables < 1:
            raise ValueError("n_tables must be positive")
        if self.n_colluding_pairs < 0 or self.n_colluding_pairs * 2 > self.n_players:
            raise ValueError("n_colluding_pairs must use at most n_players / 2 players")
        if not self.dataset_split or not self.dataset_split.replace("-", "").replace(
            "_", ""
        ).isalnum():
            raise ValueError("dataset_split must be a non-empty alphanumeric label")
        if self.dataset_id is not None and (
            not self.dataset_id
            or not self.dataset_id.replace("-", "").replace("_", "").isalnum()
        ):
            raise ValueError("dataset_id must be an alphanumeric label when provided")


@dataclass
class _Player:
    player_id: str
    name: str
    is_colluder: bool = False
    pair: Optional[CollusionPair] = None


class HandGenerator:
    """Build synthetic NLHE cash hands as plain JSON-serializable dictionaries."""

    def __init__(self, config: GeneratorConfig) -> None:
        self.cfg = config
        self.rng = random.Random(config.seed)
        self.players = self._make_players()
        self.pairs = self._make_pairs()
        self._assign_pairs()
        table_scope = (
            f"{config.dataset_id}_{config.dataset_split}"
            if config.dataset_id
            else config.dataset_split
        )
        self.tables = [f"{table_scope}_table_{i:02d}" for i in range(config.n_tables)]
        self._t0 = datetime(2026, 5, 1, tzinfo=timezone.utc)

    def _make_players(self) -> list[_Player]:
        players: list[_Player] = []
        used: set[str] = set()
        for index in range(self.cfg.n_players):
            name = _make_name(self.rng)
            while name in used:
                name = _make_name(self.rng)
            used.add(name)
            player_namespace = (
                f"pokerkit-player:{self.cfg.dataset_id}:{self.cfg.dataset_split}:"
                f"{self.cfg.seed}:{index}"
                if self.cfg.dataset_id
                else f"pokerkit-player:{self.cfg.dataset_split}:{self.cfg.seed}:{index}"
            )
            player_id = str(uuid.uuid5(uuid.NAMESPACE_URL, player_namespace))
            players.append(_Player(player_id=player_id, name=name))
        return players

    def _make_pairs(self) -> list[CollusionPair]:
        patterns = list(CollusionPattern)
        ids = list(range(self.cfg.n_players))
        self.rng.shuffle(ids)
        pairs: list[CollusionPair] = []
        for i in range(self.cfg.n_colluding_pairs):
            a, b = ids[2 * i], ids[2 * i + 1]
            pairs.append(
                CollusionPair(
                    pair_id=(
                        f"{self.cfg.dataset_id}_{self.cfg.dataset_split}_pair_{i:03d}"
                        if self.cfg.dataset_id
                        else f"{self.cfg.dataset_split}_pair_{i:03d}"
                    ),
                    player_a=self.players[a].player_id,
                    player_b=self.players[b].player_id,
                    pattern=patterns[i % len(patterns)],
                    intensity=self.rng.uniform(0.6, 0.9),
                )
            )
        return pairs

    def _assign_pairs(self) -> None:
        by_id = {player.player_id: player for player in self.players}
        for pair in self.pairs:
            for player_id in (pair.player_a, pair.player_b):
                by_id[player_id].is_colluder = True
                by_id[player_id].pair = pair

    def iter_hands(self) -> Iterator[dict]:
        for index in range(self.cfg.n_hands):
            yield self._generate_hand(index)

    def _generate_hand(self, index: int) -> dict:
        seats = self.rng.sample(self.players, k=6)
        small_blind, big_blind = self.rng.choice(_STAKES)
        small_blind_chips = int(round(small_blind * 100))
        big_blind_chips = int(round(big_blind * 100))
        stack_chips = big_blind_chips * 100
        played_at = self._t0 + timedelta(seconds=index * 30)

        active_pair = self._active_pair(seats)
        state = NoLimitTexasHoldem.create_state(
            _AUTOMATIONS,
            True,
            0,
            (small_blind_chips, big_blind_chips),
            big_blind_chips,
            (stack_chips,) * 6,
            6,
            mode=Mode.CASH_GAME,
        )

        # PokerKit owns consumption and validation. Supplying a deck shuffled by
        # our local RNG makes the simulation independent of module-global RNG.
        deck = [rank + suit for rank in _RANKS for suit in _SUITS]
        self.rng.shuffle(deck)
        state.deck_cards.clear()
        state.deck_cards.extend(Card.parse("".join(deck)))

        hole_lists: dict[int, list[str]] = {index: [] for index in range(6)}
        while state.hole_dealee_index is not None:
            player_index = state.hole_dealee_index
            operation = state.deal_hole()
            hole_lists[player_index].extend(repr(card) for card in operation.cards)
        holes = {
            player_index: (cards[0], cards[1])
            for player_index, cards in hole_lists.items()
        }

        actions: list[dict] = []
        invested = [0] * 6
        sequence = 0
        safety = 0
        while state.status:
            safety += 1
            if safety > 200:
                raise RuntimeError("PokerKit hand exceeded the action safety limit")

            actor_index = state.actor_index
            if actor_index is None:
                pending = {
                    "street": state.street_index,
                    "hole_dealee": state.hole_dealee_index,
                    "board_count": state.board_dealing_count,
                    "showdown": state.showdown_index,
                    "runout": tuple(state.runout_count_selector_indices),
                    "hand_killing": tuple(state.hand_killing_indices),
                    "chips_pulling": tuple(state.chips_pulling_indices),
                }
                raise RuntimeError(f"PokerKit state has no legal actor: {pending}")

            player = seats[actor_index]
            street = _STREETS[state.street_index]
            call_amount = int(state.checking_or_calling_amount or 0)
            current_bet = max(state.bets)
            strength = _hole_strength(*holes[actor_index])
            decision = self._decision(
                player=player,
                seats=seats,
                state_statuses=state.statuses,
                street=street,
                strength=strength,
                call_amount=call_amount,
                current_bet=current_bet,
                invested=invested[actor_index],
                pot=int(state.total_pot_amount),
                active_pair=active_pair,
                preflop_raises=sum(
                    action["street"] == "preflop" and action["action_type"] == "raise"
                    for action in actions
                ),
            )

            previous_bet = int(state.bets[actor_index])
            if decision == "fold" and state.can_fold():
                state.fold()
                action_type = "fold"
                amount = 0
            elif decision in {"bet", "raise"} and state.can_complete_bet_or_raise_to():
                minimum = int(state.min_completion_betting_or_raising_to_amount or 0)
                maximum = int(state.max_completion_betting_or_raising_to_amount or minimum)
                if current_bet:
                    target = max(minimum, current_bet * 3)
                else:
                    target = max(minimum, int(state.total_pot_amount * 0.6))
                target = min(target, maximum)
                operation = state.complete_bet_or_raise_to(target)
                amount = max(0, int(operation.amount) - previous_bet)
                action_type = "bet" if current_bet == 0 else "raise"
            elif state.can_check_or_call():
                operation = state.check_or_call()
                amount = int(operation.amount)
                action_type = "check" if amount == 0 else "call"
            elif state.can_fold():
                state.fold()
                action_type = "fold"
                amount = 0
            else:
                raise RuntimeError("PokerKit did not expose a legal betting operation")

            invested[actor_index] += amount
            actions.append(
                {
                    "sequence_no": sequence,
                    "player_id": player.player_id,
                    "street": street,
                    "action_type": action_type,
                    "amount": amount / 100.0,
                }
            )
            sequence += 1

        won = [0] * 6
        pot_chips = 0
        board: list[str] = []
        for operation in state.operations:
            if isinstance(operation, BoardDealing):
                board.extend(repr(card) for card in operation.cards)
            if isinstance(operation, ChipsPushing):
                pot_chips += int(operation.total_amount)
                for player_index, amount in enumerate(operation.amounts):
                    won[player_index] += int(amount)

        players_payload = []
        for player_index, player in enumerate(seats):
            pair_is_active = active_pair is not None and active_pair.involves(player.player_id)
            players_payload.append(
                {
                    "player_id": player.player_id,
                    "name": player.name,
                    "position": _POSITIONS_6MAX[player_index],
                    "stack_start": stack_chips / 100.0,
                    "hole_cards": " ".join(holes[player_index]),
                    "won_amount": won[player_index] / 100.0,
                    "is_suspicious": pair_is_active,
                    "collusion_pair_id": active_pair.pair_id if pair_is_active else None,
                }
            )

        split = self.cfg.dataset_split.lower()
        hand_prefix = (
            f"{self.cfg.dataset_id.upper()}-{split.upper()}"
            if self.cfg.dataset_id
            else split.upper()
        )
        return {
            "hand_id": f"{hand_prefix}-H-{index:08d}",
            "table_id": self.rng.choice(self.tables),
            "played_at": played_at.isoformat(),
            "dataset_split": split,
            "generator": "pokerkit",
            "small_blind": small_blind,
            "big_blind": big_blind,
            "num_players": len(seats),
            "pot_size": pot_chips / 100.0,
            "board": board,
            "actions": actions,
            "players": players_payload,
        }

    def _active_pair(self, seats: list[_Player]) -> Optional[CollusionPair]:
        seated_ids = {player.player_id for player in seats}
        for pair in self.pairs:
            if {pair.player_a, pair.player_b} <= seated_ids and self.rng.random() < pair.intensity:
                return pair
        return None

    def _decision(
        self,
        *,
        player: _Player,
        seats: list[_Player],
        state_statuses: list[bool],
        street: str,
        strength: float,
        call_amount: int,
        current_bet: int,
        invested: int,
        pot: int,
        active_pair: Optional[CollusionPair],
        preflop_raises: int,
    ) -> str:
        live_ids = {
            seats[index].player_id
            for index, status in enumerate(state_statuses)
            if status
        }
        partner_in = (
            active_pair is not None
            and active_pair.involves(player.player_id)
            and active_pair.partner_of(player.player_id) in live_ids
        )

        if street == "preflop":
            if (
                partner_in
                and active_pair.pattern == CollusionPattern.SQUEEZE_COLLUDE
                and preflop_raises >= 1
            ):
                return "raise"
            if (
                partner_in
                and active_pair.pattern == CollusionPattern.FOLD_BENEFIT
                and preflop_raises >= 1
            ):
                return "fold"
            if strength > 0.65:
                return "raise"
            if strength > 0.45 or call_amount < 0.6 * max(current_bet, 1):
                return "call"
            return "fold"

        if partner_in and active_pair.pattern == CollusionPattern.SOFT_PLAY:
            if call_amount == 0:
                return "check"
            return "call" if strength > 0.35 else "fold"

        if (
            partner_in
            and active_pair.pattern == CollusionPattern.CHIP_DUMP
            and call_amount > 0
        ):
            return "call"

        invested_ratio = invested / max(pot, 1)
        if invested_ratio > 0.3 and strength < 0.3 and call_amount > 0:
            return "fold"

        if call_amount == 0:
            return "bet" if strength > 0.55 else "check"
        if strength > 0.6:
            return "raise"
        if strength > 0.4:
            return "call"
        return "fold"


def generate_hands(config: GeneratorConfig | None = None) -> Iterable[dict]:
    return HandGenerator(config or GeneratorConfig()).iter_hands()
