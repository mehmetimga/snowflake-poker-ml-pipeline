"""Inference-safe user context for the scheduled multi-table world."""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable, Mapping

from pipeline.events import UserContextPayload


_COUNTRIES = (
    ("TR", "Europe/Istanbul"),
    ("DE", "Europe/Berlin"),
    ("GB", "Europe/London"),
    ("CA", "America/Toronto"),
    ("BR", "America/Sao_Paulo"),
)
_ACQUISITION_CHANNELS = ("organic", "affiliate", "paid", "referral")
_KYC_LEVELS = ("pending", "basic", "verified")
_BANKROLL_BUCKETS = ("low", "medium", "high")
_STAKE_BUCKETS = ("micro", "low", "medium", "high")


def _stable_uuid(*parts: object) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            ":".join(str(part) for part in parts),
        )
    )


def build_multitable_user_contexts(
    player_ids: Iterable[str],
    *,
    dataset_id: str,
    split: str,
    seed: int,
    effective_anchor: datetime,
    group_rows: Iterable[Mapping[str, Any]] = (),
) -> tuple[list[UserContextPayload], dict[str, int]]:
    """Build normal context and apply inference-safe difficult negatives."""
    if effective_anchor.tzinfo is None or effective_anchor.utcoffset() is None:
        raise ValueError("effective_anchor must include timezone information")
    effective_anchor = effective_anchor.astimezone(timezone.utc)
    ids = tuple(player_ids)
    rng = random.Random(seed + 810_007)
    network_count = max(2, len(ids) // 8)
    profiles: dict[str, dict[str, Any]] = {}

    for index, player_id in enumerate(ids):
        country, timezone_name = rng.choice(_COUNTRIES)
        effective_at = effective_anchor + timedelta(seconds=index)
        device_owner = index - 1 if index > 0 and index % 12 == 0 else index
        profiles[player_id] = {
            "effective_at": effective_at,
            "account_created_at": effective_at - timedelta(days=rng.randint(30, 1_500)),
            "country_bucket": country,
            "timezone": timezone_name,
            "acquisition_channel": rng.choice(_ACQUISITION_CHANNELS),
            "kyc_level": rng.choices(_KYC_LEVELS, weights=(1, 3, 8), k=1)[0],
            "account_status": "active",
            "bankroll_bucket": rng.choice(_BANKROLL_BUCKETS),
            "preferred_stake_bucket": rng.choice(_STAKE_BUCKETS),
            "skill_rating": round(rng.uniform(0.08, 0.92), 6),
            "device_id": _stable_uuid(
                dataset_id,
                split,
                seed,
                "device",
                device_owner,
            ),
            "network_cluster_id": (
                f"{dataset_id}_{split}_network_" f"{rng.randrange(network_count):05d}"
            ),
        }

    same_device_groups = 0
    same_network_only_groups = 0
    for group in group_rows:
        if bool(group["is_collusive"]):
            continue
        members = tuple(str(value) for value in group["members"])
        if len(members) < 2:
            continue
        relationship = group.get("required_context_relationship")
        left = profiles[members[0]]
        for member_index, player_id in enumerate(members[1:], start=1):
            right = profiles[player_id]
            if relationship == "same_device":
                right["device_id"] = left["device_id"]
                right["network_cluster_id"] = left["network_cluster_id"]
            elif relationship == "same_network":
                right["network_cluster_id"] = left["network_cluster_id"]
                if right["device_id"] == left["device_id"]:
                    right["device_id"] = _stable_uuid(
                        dataset_id,
                        split,
                        seed,
                        "shared-network-distinct-device",
                        group["group_id"],
                        member_index,
                    )
        if relationship == "same_device":
            same_device_groups += 1
        elif relationship == "same_network":
            same_network_only_groups += 1

    contexts = [
        UserContextPayload(
            user_id=player_id,
            context_version=1,
            effective_at=profiles[player_id]["effective_at"],
            account_created_at=profiles[player_id]["account_created_at"],
            country_bucket=profiles[player_id]["country_bucket"],
            timezone=profiles[player_id]["timezone"],
            acquisition_channel=profiles[player_id]["acquisition_channel"],
            kyc_level=profiles[player_id]["kyc_level"],
            account_status=profiles[player_id]["account_status"],
            bankroll_bucket=profiles[player_id]["bankroll_bucket"],
            preferred_stake_bucket=profiles[player_id]["preferred_stake_bucket"],
            skill_rating=profiles[player_id]["skill_rating"],
            device_id=profiles[player_id]["device_id"],
            network_cluster_id=profiles[player_id]["network_cluster_id"],
        )
        for player_id in ids
    ]
    return contexts, {
        "context_rows": len(contexts),
        "same_device_hard_negative_groups": same_device_groups,
        "same_network_only_hard_negative_groups": same_network_only_groups,
    }
