"""Individual rule predicates.

Each function returns True if the rule fires for the given player/hand stats.
"""

from __future__ import annotations

from .types import PlayerHandStats


def check_pre_mw(s: PlayerHandStats) -> bool:
    """Multiway pot: 2+ preflop raises and 3+ players reached the flop."""
    return s.preflop_raises_hand >= 2 and s.players_to_flop >= 3


def check_pre_ofold_commit(s: PlayerHandStats) -> bool:
    """Invested ≥50% of stack preflop, then folded preflop."""
    if s.stack_start <= 0:
        return False
    return (
        s.preflop_invested >= 0.5 * s.stack_start
        and s.folded_street == "preflop"
    )


def check_post_ofold_commit(s: PlayerHandStats) -> bool:
    """Invested ≥50% of stack total, saw flop, then folded postflop."""
    if s.stack_start <= 0:
        return False
    return (
        s.total_invested >= 0.5 * s.stack_start
        and s.saw_flop
        and s.folded_street in ("flop", "turn", "river")
    )


def check_high_invest_fold(s: PlayerHandStats) -> bool:
    """Invested ≥30% of stack and folded on any street."""
    if s.stack_start <= 0:
        return False
    return (
        s.total_invested >= 0.3 * s.stack_start
        and s.folded_street is not None
    )


def check_position_anomaly(s: PlayerHandStats) -> bool:
    """Late position + passive preflop + aggressive postflop."""
    in_late = s.position in ("BTN", "CO")
    return in_late and s.preflop_aggression < 0.5 and s.postflop_aggression > 1.5
