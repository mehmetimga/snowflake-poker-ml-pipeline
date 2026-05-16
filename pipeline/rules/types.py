from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class PlayerHandStats:
    hand_id: str
    player_id: str
    position: str
    stack_start: float
    total_invested: float
    preflop_raises_player: int
    preflop_raises_hand: int
    players_to_flop: int
    saw_flop: bool
    saw_river: bool
    folded_street: str | None
    preflop_invested: float
    postflop_invested: float
    preflop_aggression: float
    postflop_aggression: float


@dataclass
class RuleWeights:
    pre_mw: float = 20.0
    pre_ofold_commit: float = 30.0
    post_ofold_commit: float = 35.0
    high_invest_fold: float = 25.0
    position_anomaly: float = 10.0


@dataclass
class RuleFlags:
    flag_eligible: bool = False
    flag_pre_mw: bool = False
    flag_pre_ofold_commit: bool = False
    flag_post_ofold_commit: bool = False
    flag_high_invest_fold: bool = False
    flag_position_anomaly: bool = False
    rule_score: float = 0.0
    triggered_rules: list[str] = field(default_factory=list)
