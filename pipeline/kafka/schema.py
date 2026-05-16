from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class ActionEvent(BaseModel):
    sequence_no: int
    player_id: str
    street: str
    action_type: str
    amount: float


class PlayerEvent(BaseModel):
    player_id: str
    name: str
    position: str
    stack_start: float
    hole_cards: Optional[str] = None
    won_amount: float
    is_suspicious: bool = False
    collusion_pair_id: Optional[str] = None


class HandEvent(BaseModel):
    hand_id: str
    table_id: str
    played_at: str
    small_blind: float
    big_blind: float
    num_players: int
    pot_size: float
    board: list[str]
    actions: list[ActionEvent]
    players: list[PlayerEvent]


class AlertEvent(BaseModel):
    alert_id: str
    hand_id: str
    table_id: str
    suspicious_player_id: str
    risk_score: float
    risk_level: str
    triggered_rules: list[str]
    model_scores: dict[str, Any]
    created_at: str
