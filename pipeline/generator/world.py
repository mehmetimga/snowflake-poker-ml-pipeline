"""Deterministic multi-stream poker world for real-time pipeline tests."""

from __future__ import annotations

import hashlib
import itertools
import json
import random
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from pathlib import Path
from typing import Any, Iterable

from pipeline.events import (
    ACCOUNT_LINK_UPDATED,
    HAND_COMPLETED,
    SESSION_STARTED,
    USER_CONTEXT_UPDATED,
    AccountLinkPayload,
    HandCompletedPayload,
    PairHandLabel,
    PlayerHandLabel,
    SessionPayload,
    UserContextPayload,
    build_event,
    contract_schema_bundle,
)
from pipeline.events.contracts import TOPIC_BY_EVENT_TYPE

from .dataset import FrozenDatasetConfig, SPLIT_NAMES, separate_hand_labels
from .hand_generator import GeneratorConfig, HandGenerator


_SEED_OFFSETS = {"train": 0, "validation": 10_000, "test": 20_000, "challenge": 30_000}
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
_COLLUDER_CONTEXT_POLICY = {
    # These are probabilistic correlations, not deterministic truth fields.
    # Normal users still share infrastructure and many colluding pairs do not.
    "same_network_probability": 0.55,
    "same_device_probability": 0.18,
    "same_country_timezone_probability": 0.55,
    "same_acquisition_probability": 0.35,
    "similar_account_age_probability": 0.45,
    "similar_skill_probability": 0.55,
    "same_bankroll_probability": 0.35,
    "same_stake_probability": 0.55,
}


@dataclass(frozen=True)
class RealtimeWorldConfig:
    dataset_id: str = "context-v1"
    tenant_id: str = "demo"
    product_id: str = "poker"
    frozen: FrozenDatasetConfig = field(default_factory=FrozenDatasetConfig)

    def __post_init__(self) -> None:
        if not self.dataset_id:
            raise ValueError("dataset_id must not be empty")
        if not self.tenant_id or not self.product_id:
            raise ValueError("tenant_id and product_id must not be empty")


@dataclass(frozen=True)
class _UserState:
    context: UserContextPayload
    session: SessionPayload


def _stable_uuid(*parts: object) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, ":".join(str(part) for part in parts))


def _json_line(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _utc_anchor(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include timezone information")
    return value.astimezone(timezone.utc)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _population_hash(player_ids: Iterable[str]) -> str:
    value = "\n".join(sorted(player_ids)).encode("utf-8")
    return hashlib.sha256(value).hexdigest()


class SyntheticPokerWorld:
    """One correlated source of hand, context, session, link, and label data."""

    def __init__(
        self,
        *,
        dataset_id: str,
        split: str,
        hand_count: int,
        n_players: int,
        n_tables: int,
        n_colluding_pairs: int,
        seed: int,
        tenant_id: str = "demo",
        product_id: str = "poker",
        hand_start_at: datetime | None = None,
        context_start_at: datetime | None = None,
    ) -> None:
        self.dataset_id = dataset_id
        self.split = split
        self.seed = seed
        self.tenant_id = tenant_id
        self.product_id = product_id
        self.hand_generator = HandGenerator(
            GeneratorConfig(
                n_hands=hand_count,
                n_players=n_players,
                n_tables=n_tables,
                n_colluding_pairs=n_colluding_pairs,
                seed=seed,
                dataset_split=split,
                dataset_id=dataset_id,
            ),
            start_at=hand_start_at,
        )
        self._context_rng = random.Random(seed + 700_001)
        if context_start_at is None:
            context_start_at = datetime(2026, 4, 30, tzinfo=timezone.utc)
        self._context_t0 = _utc_anchor(context_start_at, "context_start_at")
        self.users = self._make_users()
        self.account_links = self._make_account_links()

    @property
    def player_ids(self) -> tuple[str, ...]:
        return tuple(player.player_id for player in self.hand_generator.players)

    def _make_users(self) -> list[_UserState]:
        profiles: dict[str, dict[str, Any]] = {}
        network_count = max(2, len(self.hand_generator.players) // 8)
        for index, player in enumerate(self.hand_generator.players):
            country, timezone_name = self._context_rng.choice(_COUNTRIES)
            network_index = self._context_rng.randrange(network_count)
            network_id = f"{self.split}_network_{network_index:04d}"

            # A small number of normal users share devices. This keeps shared
            # infrastructure useful without making it a synthetic truth leak.
            device_owner_index = index - 1 if index > 0 and index % 12 == 0 else index
            device_id = str(
                _stable_uuid(
                    self.dataset_id,
                    self.split,
                    self.seed,
                    "device",
                    device_owner_index,
                )
            )
            effective_at = self._context_t0 + timedelta(seconds=index)
            profiles[player.player_id] = {
                "effective_at": effective_at,
                "account_created_at": effective_at
                - timedelta(days=self._context_rng.randint(30, 1_500)),
                "country_bucket": country,
                "timezone": timezone_name,
                "acquisition_channel": self._context_rng.choice(
                    _ACQUISITION_CHANNELS
                ),
                "kyc_level": self._context_rng.choices(
                    _KYC_LEVELS, weights=(1, 3, 8), k=1
                )[0],
                "bankroll_bucket": self._context_rng.choice(_BANKROLL_BUCKETS),
                "preferred_stake_bucket": self._context_rng.choice(_STAKE_BUCKETS),
                "skill_rating": round(self._context_rng.uniform(0.08, 0.92), 6),
                "device_id": device_id,
                "network_cluster_id": network_id,
            }

        # Add realistic, imperfect context correlation for known synthetic
        # relationships. These values remain ordinary context fields and never
        # expose the pair ID or target. Independent probabilities ensure the
        # positive and negative distributions overlap.
        for pair in self.hand_generator.pairs:
            left = profiles[pair.player_a]
            right = profiles[pair.player_b]
            shared_device = (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_device_probability"]
            )
            if (
                shared_device
                or self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_network_probability"]
            ):
                right["network_cluster_id"] = left["network_cluster_id"]
            if shared_device:
                right["device_id"] = left["device_id"]
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_country_timezone_probability"]
            ):
                right["country_bucket"] = left["country_bucket"]
                right["timezone"] = left["timezone"]
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_acquisition_probability"]
            ):
                right["acquisition_channel"] = left["acquisition_channel"]
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["similar_account_age_probability"]
            ):
                left_age = left["effective_at"] - left["account_created_at"]
                jitter_days = self._context_rng.randint(-14, 14)
                right["account_created_at"] = right["effective_at"] - left_age + timedelta(
                    days=jitter_days
                )
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["similar_skill_probability"]
            ):
                right["skill_rating"] = round(
                    min(
                        0.99,
                        max(
                            0.01,
                            left["skill_rating"]
                            + self._context_rng.uniform(-0.08, 0.08),
                        ),
                    ),
                    6,
                )
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_bankroll_probability"]
            ):
                right["bankroll_bucket"] = left["bankroll_bucket"]
            if (
                self._context_rng.random()
                < _COLLUDER_CONTEXT_POLICY["same_stake_probability"]
            ):
                right["preferred_stake_bucket"] = left[
                    "preferred_stake_bucket"
                ]

        users: list[_UserState] = []
        for index, player in enumerate(self.hand_generator.players):
            profile = profiles[player.player_id]
            context = UserContextPayload(
                user_id=player.player_id,
                context_version=1,
                effective_at=profile["effective_at"],
                account_created_at=profile["account_created_at"],
                country_bucket=profile["country_bucket"],
                timezone=profile["timezone"],
                acquisition_channel=profile["acquisition_channel"],
                kyc_level=profile["kyc_level"],
                account_status="active",
                bankroll_bucket=profile["bankroll_bucket"],
                preferred_stake_bucket=profile["preferred_stake_bucket"],
                skill_rating=profile["skill_rating"],
                device_id=profile["device_id"],
                network_cluster_id=profile["network_cluster_id"],
            )
            session = SessionPayload(
                session_id=str(
                    _stable_uuid(
                        self.dataset_id,
                        self.split,
                        self.seed,
                        "session",
                        index,
                    )
                ),
                user_id=player.player_id,
                device_id=profile["device_id"],
                network_cluster_id=profile["network_cluster_id"],
                started_at=profile["effective_at"] + timedelta(hours=12),
            )
            users.append(_UserState(context=context, session=session))
        return users

    def _make_account_links(self) -> list[AccountLinkPayload]:
        shuffled = list(self.users)
        self._context_rng.shuffle(shuffled)
        link_count = len(shuffled) // 10
        links: list[AccountLinkPayload] = []
        for index in range(link_count):
            left = shuffled[index * 2].context.user_id
            right = shuffled[index * 2 + 1].context.user_id
            user_id, related_user_id = sorted((left, right))
            effective_at = self._context_t0 + timedelta(minutes=30, seconds=index)
            links.append(
                AccountLinkPayload(
                    link_id=str(
                        _stable_uuid(
                            self.dataset_id,
                            self.split,
                            self.seed,
                            "account-link",
                            index,
                        )
                    ),
                    user_id=user_id,
                    related_user_id=related_user_id,
                    link_type=self._context_rng.choice(("shared_network", "household")),
                    confidence_bucket=self._context_rng.choice(("low", "medium")),
                    link_version=1,
                    effective_at=effective_at,
                )
            )
        return links

    def context_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for user in self.users:
            payload = user.context
            event = build_event(
                event_type=USER_CONTEXT_UPDATED,
                aggregate_id=f"{payload.user_id}:context:{payload.context_version}",
                payload=payload,
                dataset_id=self.dataset_id,
                dataset_split=self.split,
                occurred_at=payload.effective_at,
                tenant_id=self.tenant_id,
                product_id=self.product_id,
            )
            events.append(event.model_dump(mode="json"))
        return events

    def session_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for user in self.users:
            payload = user.session
            event = build_event(
                event_type=SESSION_STARTED,
                aggregate_id=payload.session_id,
                payload=payload,
                dataset_id=self.dataset_id,
                dataset_split=self.split,
                occurred_at=payload.started_at,
                tenant_id=self.tenant_id,
                product_id=self.product_id,
            )
            events.append(event.model_dump(mode="json"))
        return events

    def account_link_events(self) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        for payload in self.account_links:
            event = build_event(
                event_type=ACCOUNT_LINK_UPDATED,
                aggregate_id=f"{payload.link_id}:{payload.link_version}",
                payload=payload,
                dataset_id=self.dataset_id,
                dataset_split=self.split,
                occurred_at=payload.effective_at,
                tenant_id=self.tenant_id,
                product_id=self.product_id,
            )
            events.append(event.model_dump(mode="json"))
        return events

    def iter_hands_with_labels(
        self,
    ) -> Iterable[tuple[dict[str, Any], list[PlayerHandLabel], list[PairHandLabel]]]:
        for raw_hand in self.hand_generator.iter_hands():
            safe_hand, raw_player_labels = separate_hand_labels(raw_hand)
            payload = HandCompletedPayload.model_validate(safe_hand)
            played_at = payload.played_at
            hand_event = build_event(
                event_type=HAND_COMPLETED,
                aggregate_id=payload.hand_id,
                payload=payload,
                dataset_id=self.dataset_id,
                dataset_split=self.split,
                occurred_at=played_at,
                emitted_at=played_at + timedelta(seconds=1),
                tenant_id=self.tenant_id,
                product_id=self.product_id,
            )
            available_at = played_at + timedelta(days=7)
            player_labels = [
                PlayerHandLabel(
                    example_id=_stable_uuid(
                        self.dataset_id,
                        self.split,
                        "player-label",
                        raw_label["hand_id"],
                        raw_label["player_id"],
                    ),
                    dataset_id=self.dataset_id,
                    dataset_split=self.split,
                    hand_id=raw_label["hand_id"],
                    player_id=raw_label["player_id"],
                    is_suspicious=bool(raw_label["is_suspicious"]),
                    collusion_pair_id=raw_label["collusion_pair_id"],
                    label_available_at=available_at,
                )
                for raw_label in raw_player_labels
            ]
            label_by_player = {label.player_id: label for label in player_labels}
            pair_labels: list[PairHandLabel] = []
            for player_a, player_b in itertools.combinations(
                sorted(label_by_player),
                2,
            ):
                left = label_by_player[player_a]
                right = label_by_player[player_b]
                pair_id = (
                    left.collusion_pair_id
                    if left.is_suspicious
                    and right.is_suspicious
                    and left.collusion_pair_id is not None
                    and left.collusion_pair_id == right.collusion_pair_id
                    else None
                )
                pair_key = f"{player_a}:{player_b}"
                pair_labels.append(
                    PairHandLabel(
                        example_id=_stable_uuid(
                            self.dataset_id,
                            self.split,
                            "pair-label",
                            payload.hand_id,
                            pair_key,
                        ),
                        dataset_id=self.dataset_id,
                        dataset_split=self.split,
                        hand_id=payload.hand_id,
                        pair_key=pair_key,
                        player_a=player_a,
                        player_b=player_b,
                        is_collusive=pair_id is not None,
                        collusion_pair_id=pair_id,
                        label_available_at=available_at,
                    )
                )
            yield hand_event.model_dump(mode="json"), player_labels, pair_labels


def _write_rows(path: Path, rows: Iterable[Any]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w") as stream:
        for row in rows:
            stream.write(_json_line(row))
            count += 1
    return count


def build_realtime_world_dataset(
    output_dir: Path,
    config: RealtimeWorldConfig | None = None,
    *,
    hand_start_at: datetime | None = None,
    context_start_at: datetime | None = None,
) -> dict[str, Any]:
    """Write the canonical multi-stream dataset and a deterministic manifest."""
    cfg = config or RealtimeWorldConfig()
    if hand_start_at is not None:
        hand_start_at = _utc_anchor(hand_start_at, "hand_start_at")
    if context_start_at is not None:
        context_start_at = _utc_anchor(context_start_at, "context_start_at")
    output_dir.mkdir(parents=True, exist_ok=True)
    config_path = output_dir / "config.json"
    config_value = asdict(cfg)
    if hand_start_at is not None:
        config_value["hand_start_at"] = hand_start_at.astimezone(timezone.utc).isoformat()
    if context_start_at is not None:
        config_value["context_start_at"] = context_start_at.astimezone(timezone.utc).isoformat()
    config_path.write_text(_json_line(config_value))
    schemas_path = output_dir / "schemas.json"
    schemas_path.write_text(_json_line(contract_schema_bundle()))

    manifest: dict[str, Any] = {
        "schema_version": 1,
        "dataset_id": cfg.dataset_id,
        "generator": "SyntheticPokerWorld+PokerKit",
        "pokerkit_version": version("pokerkit"),
        "base_seed": cfg.frozen.seed,
        "context_signal_policy": _COLLUDER_CONTEXT_POLICY,
        "player_populations_disjoint": True,
        "artifacts": {
            "config.json": _sha256(config_path),
            "schemas.json": _sha256(schemas_path),
        },
        "expected_kafka_counts": {topic: 0 for topic in TOPIC_BY_EVENT_TYPE.values()},
        "splits": {},
    }
    populations: dict[str, set[str]] = {}

    for split in SPLIT_NAMES:
        hand_count = cfg.frozen.counts()[split]
        split_seed = cfg.frozen.seed + _SEED_OFFSETS[split]
        world = SyntheticPokerWorld(
            dataset_id=cfg.dataset_id,
            split=split,
            hand_count=hand_count,
            n_players=cfg.frozen.n_players,
            n_tables=cfg.frozen.n_tables,
            n_colluding_pairs=cfg.frozen.n_colluding_pairs,
            seed=split_seed,
            tenant_id=cfg.tenant_id,
            product_id=cfg.product_id,
            hand_start_at=hand_start_at,
            context_start_at=context_start_at,
        )
        populations[split] = set(world.player_ids)
        split_dir = output_dir / split
        events_dir = split_dir / "events"
        labels_dir = split_dir / ("private_labels" if split == "challenge" else "labels")
        snapshots_dir = split_dir / "snapshots"

        context_path = events_dir / "user_context.jsonl"
        session_path = events_dir / "sessions.jsonl"
        links_path = events_dir / "account_links.jsonl"
        hands_path = events_dir / "hands.jsonl"
        player_labels_path = labels_dir / "player_labels.jsonl"
        pair_labels_path = labels_dir / "pair_labels.jsonl"
        users_snapshot_path = snapshots_dir / "users.jsonl"

        context_count = _write_rows(context_path, world.context_events())
        session_count = _write_rows(session_path, world.session_events())
        link_count = _write_rows(links_path, world.account_link_events())
        _write_rows(
            users_snapshot_path,
            (user.context.model_dump(mode="json") for user in world.users),
        )

        hand_count_written = 0
        player_label_count = 0
        positive_player_count = 0
        pair_label_count = 0
        positive_pair_count = 0
        hands_path.parent.mkdir(parents=True, exist_ok=True)
        player_labels_path.parent.mkdir(parents=True, exist_ok=True)
        with (
            hands_path.open("w") as hands_file,
            player_labels_path.open("w") as player_labels_file,
            pair_labels_path.open("w") as pair_labels_file,
        ):
            for hand_event, player_labels, pair_labels in world.iter_hands_with_labels():
                hands_file.write(_json_line(hand_event))
                hand_count_written += 1
                for label in player_labels:
                    player_labels_file.write(_json_line(label))
                    player_label_count += 1
                    positive_player_count += int(label.is_suspicious)
                for label in pair_labels:
                    pair_labels_file.write(_json_line(label))
                    pair_label_count += 1
                    positive_pair_count += int(label.is_collusive)

        files = (
            context_path,
            session_path,
            links_path,
            hands_path,
            player_labels_path,
            pair_labels_path,
            users_snapshot_path,
        )
        file_hashes = {
            str(path.relative_to(output_dir)): _sha256(path)
            for path in files
        }
        manifest["artifacts"].update(file_hashes)
        manifest["expected_kafka_counts"][TOPIC_BY_EVENT_TYPE[HAND_COMPLETED]] += hand_count_written
        manifest["expected_kafka_counts"][TOPIC_BY_EVENT_TYPE[USER_CONTEXT_UPDATED]] += context_count
        manifest["expected_kafka_counts"][TOPIC_BY_EVENT_TYPE[SESSION_STARTED]] += session_count
        manifest["expected_kafka_counts"][TOPIC_BY_EVENT_TYPE[ACCOUNT_LINK_UPDATED]] += link_count
        manifest["splits"][split] = {
            "seed": split_seed,
            "hands": hand_count_written,
            "population_players": len(world.player_ids),
            "population_sha256": _population_hash(world.player_ids),
            "context_events": context_count,
            "session_events": session_count,
            "account_link_events": link_count,
            "player_label_rows": player_label_count,
            "positive_player_label_rows": positive_player_count,
            "pair_label_rows": pair_label_count,
            "positive_pair_label_rows": positive_pair_count,
            "pairs_per_six_player_hand": 15,
        }

    for left_index, left in enumerate(SPLIT_NAMES):
        for right in SPLIT_NAMES[left_index + 1 :]:
            overlap = populations[left] & populations[right]
            if overlap:
                raise RuntimeError(f"Player leakage between {left} and {right}: {len(overlap)}")

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest
