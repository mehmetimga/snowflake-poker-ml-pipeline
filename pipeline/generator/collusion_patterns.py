"""Collusion patterns injected into the synthetic generator.

All patterns are educational/demo only and operate on fully synthetic data.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class CollusionPattern(str, Enum):
    SOFT_PLAY = "soft_play"          # pair members never both raise; check down post-flop
    CHIP_DUMP = "chip_dump"          # one player calls down vs partner's bet
    SQUEEZE_COLLUDE = "squeeze_collude"  # partner 3-bets to isolate victim
    FOLD_BENEFIT = "fold_benefit"    # one folds while other raises on multiway pots


@dataclass(frozen=True)
class CollusionPair:
    pair_id: str
    player_a: str
    player_b: str
    pattern: CollusionPattern
    intensity: float = 0.7  # 0-1 probability that pattern fires when both members are in a hand

    def involves(self, player_id: str) -> bool:
        return player_id in (self.player_a, self.player_b)

    def partner_of(self, player_id: str) -> Optional[str]:
        if player_id == self.player_a:
            return self.player_b
        if player_id == self.player_b:
            return self.player_a
        return None
