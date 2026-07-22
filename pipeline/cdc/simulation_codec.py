"""Simulation-only Protobuf codec for PostgreSQL BYTEA hand history."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from google.protobuf.message import DecodeError

from pipeline.events import HandCompletedPayload

from .hand_history import (
    CdcAdapterConfig,
    CdcRecordRejected,
    HandCompletedOutboxRow,
)
from .proto import poker_hand_history_v1_pb2 as hand_pb


SIMULATION_PROTOBUF_CODEC_VERSION = "poker-hand-protobuf-v1"
MICROS_PER_UNIT = 1_000_000


def _to_micros(value: object) -> int:
    try:
        scaled = Decimal(str(value)) * MICROS_PER_UNIT
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"invalid monetary value: {value!r}") from exc
    if scaled != scaled.to_integral_value():
        raise ValueError(f"monetary value exceeds six decimal places: {value!r}")
    result = int(scaled)
    if result < 0:
        raise ValueError("monetary values must be non-negative")
    return result


def _from_micros(value: int) -> float:
    return float(Decimal(value) / MICROS_PER_UNIT)


def public_hand_payload(hand: Mapping[str, Any]) -> HandCompletedPayload:
    """Drop simulator truth and retain only the canonical inference contract."""

    return HandCompletedPayload.model_validate(
        {
            "hand_id": hand["hand_id"],
            "table_id": hand["table_id"],
            "played_at": hand["played_at"],
            "dataset_split": hand["dataset_split"],
            "generator": hand["generator"],
            "small_blind": hand["small_blind"],
            "big_blind": hand["big_blind"],
            "num_players": hand["num_players"],
            "pot_size": hand["pot_size"],
            "board": list(hand["board"]),
            "actions": [
                {
                    "sequence_no": action["sequence_no"],
                    "player_id": action["player_id"],
                    "street": action["street"],
                    "action_type": action["action_type"],
                    "amount": action["amount"],
                }
                for action in hand["actions"]
            ],
            "players": [
                {
                    "player_id": player["player_id"],
                    "name": player["name"],
                    "position": player["position"],
                    "stack_start": player["stack_start"],
                    "hole_cards": player["hole_cards"],
                    "won_amount": player["won_amount"],
                }
                for player in hand["players"]
            ],
        }
    )


def encode_simulation_hand(
    payload: HandCompletedPayload,
    *,
    game_type: str,
) -> bytes:
    """Serialize one canonical hand into deterministic Protobuf bytes."""

    if not game_type or game_type.strip() != game_type:
        raise ValueError("game_type must be a non-empty normalized value")
    played_at = payload.played_at.astimezone(timezone.utc)
    if played_at.microsecond % 1000:
        raise ValueError("simulation Protobuf v1 supports millisecond timestamps")
    message = hand_pb.HandHistoryV1(
        hand_id=payload.hand_id,
        game_type=game_type,
        table_id=payload.table_id,
        played_at_unix_ms=int(played_at.timestamp() * 1000),
        dataset_split=payload.dataset_split,
        generator=payload.generator,
        small_blind_micros=_to_micros(payload.small_blind),
        big_blind_micros=_to_micros(payload.big_blind),
        num_players=payload.num_players,
        pot_size_micros=_to_micros(payload.pot_size),
        board=payload.board,
    )
    for action in payload.actions:
        message.actions.add(
            sequence_no=action.sequence_no,
            player_id=action.player_id,
            street=action.street,
            action_type=action.action_type,
            amount_micros=_to_micros(action.amount),
        )
    for player in payload.players:
        message.players.add(
            player_id=player.player_id,
            name=player.name,
            position=player.position,
            stack_start_micros=_to_micros(player.stack_start),
            hole_cards=player.hole_cards.split(),
            won_amount_micros=_to_micros(player.won_amount),
        )
    return message.SerializeToString(deterministic=True)


class SimulationProtobufV1Decoder:
    """Decode the local simulation format without changing canonical v1."""

    codec_version = SIMULATION_PROTOBUF_CODEC_VERSION

    def decode(
        self,
        payload: bytes,
        *,
        row: HandCompletedOutboxRow,
        config: CdcAdapterConfig,
    ) -> HandCompletedPayload:
        del config
        message = hand_pb.HandHistoryV1()
        try:
            message.ParseFromString(payload)
        except DecodeError as exc:
            raise CdcRecordRejected(
                "invalid_binary_payload", "simulation payload is not Protobuf v1"
            ) from exc
        if message.game_type != row.game_type:
            raise CdcRecordRejected(
                "game_type_mismatch",
                f"outbox game type {row.game_type!r} != binary {message.game_type!r}",
            )
        try:
            return HandCompletedPayload.model_validate(
                {
                    "hand_id": message.hand_id,
                    "table_id": message.table_id,
                    "played_at": datetime.fromtimestamp(
                        message.played_at_unix_ms / 1000,
                        tz=timezone.utc,
                    ),
                    "dataset_split": message.dataset_split,
                    "generator": message.generator,
                    "small_blind": _from_micros(message.small_blind_micros),
                    "big_blind": _from_micros(message.big_blind_micros),
                    "num_players": message.num_players,
                    "pot_size": _from_micros(message.pot_size_micros),
                    "board": list(message.board),
                    "actions": [
                        {
                            "sequence_no": action.sequence_no,
                            "player_id": action.player_id,
                            "street": action.street,
                            "action_type": action.action_type,
                            "amount": _from_micros(action.amount_micros),
                        }
                        for action in message.actions
                    ],
                    "players": [
                        {
                            "player_id": player.player_id,
                            "name": player.name,
                            "position": player.position,
                            "stack_start": _from_micros(player.stack_start_micros),
                            "hole_cards": " ".join(player.hole_cards),
                            "won_amount": _from_micros(player.won_amount_micros),
                        }
                        for player in message.players
                    ],
                }
            )
        except (ValueError, OverflowError) as exc:
            raise CdcRecordRejected(
                "invalid_canonical_payload",
                "decoded simulation hand violates canonical v1",
            ) from exc
