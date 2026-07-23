"""Deterministic 100-table scheduling and frozen dataset artifacts.

This module is the structural D1-D3 slice of the multi-table data plan.  It
owns capacity validation, user sessions, persistent seat intervals, and
independent table clocks.  Poker mechanics remain in :mod:`hand_generator`.
Scenario planning and alert-oracle selection are intentionally deferred to D4
and D6.
"""

from __future__ import annotations

import hashlib
import heapq
import itertools
import json
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from importlib.metadata import version
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Iterator, Mapping

from pipeline.events import (
    HAND_COMPLETED,
    HandCompletedPayload,
    PairHandLabel,
    PlayerHandLabel,
    build_event,
)

from .dataset import SPLIT_NAMES, separate_hand_labels
from .hand_generator import GeneratorConfig, HandGenerator
from .multitable_context import build_multitable_user_contexts
from .scenario_planner import ScenarioAssignment, ScenarioPlan, ScenarioPlanner


_SEED_OFFSETS = {
    "train": 0,
    "validation": 10_000,
    "test": 20_000,
    "challenge": 30_000,
}
_PROFILE_FIELDS = {
    "schema_version",
    "profile_id",
    "dataset_id",
    "split_hands",
    "registered_players",
    "daily_active_players",
    "peak_concurrent_players",
    "table_size_counts",
    "hands_per_table_hour",
    "simulated_day_hours",
    "max_simultaneous_tables",
    "simultaneous_table_distribution",
    "seat_rebalance_minutes",
    "minimum_session_minutes",
    "maximum_session_minutes",
    "n_colluding_pairs",
    "seed",
}


def _stable_uuid(*parts: object) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, ":".join(str(part) for part in parts))


def _utc(value: datetime, name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{name} must include timezone information")
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _json_line(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _population_hash(player_ids: Iterable[str]) -> str:
    return hashlib.sha256("\n".join(sorted(player_ids)).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class MultiTableProfile:
    """Resolved, versioned configuration for one frozen multi-table dataset."""

    schema_version: int
    profile_id: str
    dataset_id: str
    split_hands: Mapping[str, int]
    registered_players: int
    daily_active_players: int
    peak_concurrent_players: int
    table_size_counts: Mapping[int, int]
    hands_per_table_hour: float
    simulated_day_hours: float
    max_simultaneous_tables: int
    simultaneous_table_distribution: Mapping[int, float]
    seat_rebalance_minutes: int
    minimum_session_minutes: int
    maximum_session_minutes: int
    n_colluding_pairs: int
    seed: int

    def __post_init__(self) -> None:
        split_hands = {
            str(name): int(count) for name, count in self.split_hands.items()
        }
        table_size_counts = {
            int(size): int(count) for size, count in self.table_size_counts.items()
        }
        distribution = {
            int(count): float(weight)
            for count, weight in self.simultaneous_table_distribution.items()
        }
        object.__setattr__(self, "split_hands", MappingProxyType(split_hands))
        object.__setattr__(
            self,
            "table_size_counts",
            MappingProxyType(table_size_counts),
        )
        object.__setattr__(
            self,
            "simultaneous_table_distribution",
            MappingProxyType(distribution),
        )
        self._validate()

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "MultiTableProfile":
        unknown = set(raw) - _PROFILE_FIELDS
        missing = _PROFILE_FIELDS - set(raw)
        if unknown:
            raise ValueError(f"unknown multi-table profile field: {sorted(unknown)[0]}")
        if missing:
            raise ValueError(f"missing multi-table profile field: {sorted(missing)[0]}")
        return cls(
            schema_version=int(raw["schema_version"]),
            profile_id=str(raw["profile_id"]),
            dataset_id=str(raw["dataset_id"]),
            split_hands=dict(raw["split_hands"]),
            registered_players=int(raw["registered_players"]),
            daily_active_players=int(raw["daily_active_players"]),
            peak_concurrent_players=int(raw["peak_concurrent_players"]),
            table_size_counts={
                int(size): int(count)
                for size, count in dict(raw["table_size_counts"]).items()
            },
            hands_per_table_hour=float(raw["hands_per_table_hour"]),
            simulated_day_hours=float(raw["simulated_day_hours"]),
            max_simultaneous_tables=int(raw["max_simultaneous_tables"]),
            simultaneous_table_distribution={
                int(count): float(weight)
                for count, weight in dict(
                    raw["simultaneous_table_distribution"]
                ).items()
            },
            seat_rebalance_minutes=int(raw["seat_rebalance_minutes"]),
            minimum_session_minutes=int(raw["minimum_session_minutes"]),
            maximum_session_minutes=int(raw["maximum_session_minutes"]),
            n_colluding_pairs=int(raw["n_colluding_pairs"]),
            seed=int(raw["seed"]),
        )

    @classmethod
    def from_json(cls, path: Path) -> "MultiTableProfile":
        return cls.from_dict(json.loads(path.read_text()))

    @property
    def table_count(self) -> int:
        return sum(self.table_size_counts.values())

    @property
    def concurrent_seats(self) -> int:
        return sum(size * count for size, count in self.table_size_counts.items())

    @property
    def expected_tables_per_active_player(self) -> float:
        return sum(
            count * weight
            for count, weight in self.simultaneous_table_distribution.items()
        )

    @property
    def cohort_count(self) -> int:
        return self.daily_active_players // self.peak_concurrent_players

    @property
    def cohort_minutes(self) -> float:
        return self.simulated_day_hours * 60.0 / self.cohort_count

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "profile_id": self.profile_id,
            "dataset_id": self.dataset_id,
            "split_hands": {split: self.split_hands[split] for split in SPLIT_NAMES},
            "registered_players": self.registered_players,
            "daily_active_players": self.daily_active_players,
            "peak_concurrent_players": self.peak_concurrent_players,
            "table_size_counts": {
                str(size): self.table_size_counts[size]
                for size in sorted(self.table_size_counts)
            },
            "hands_per_table_hour": self.hands_per_table_hour,
            "simulated_day_hours": self.simulated_day_hours,
            "max_simultaneous_tables": self.max_simultaneous_tables,
            "simultaneous_table_distribution": {
                str(count): self.simultaneous_table_distribution[count]
                for count in sorted(self.simultaneous_table_distribution)
            },
            "seat_rebalance_minutes": self.seat_rebalance_minutes,
            "minimum_session_minutes": self.minimum_session_minutes,
            "maximum_session_minutes": self.maximum_session_minutes,
            "n_colluding_pairs": self.n_colluding_pairs,
            "seed": self.seed,
        }

    def _validate(self) -> None:
        if self.schema_version != 1:
            raise ValueError("multi-table profile schema_version must be 1")
        for name, value in (
            ("profile_id", self.profile_id),
            ("dataset_id", self.dataset_id),
        ):
            if not value or not value.replace("-", "").replace("_", "").isalnum():
                raise ValueError(f"{name} must be an alphanumeric label")
        if set(self.split_hands) != set(SPLIT_NAMES):
            raise ValueError(
                "split_hands must define train, validation, test, and challenge"
            )
        if any(count < 0 for count in self.split_hands.values()):
            raise ValueError("split hand counts must be non-negative")
        if self.registered_players < 6:
            raise ValueError("registered_players must be at least 6")
        if not (
            1
            <= self.peak_concurrent_players
            <= self.daily_active_players
            <= self.registered_players
        ):
            raise ValueError(
                "player counts must satisfy peak <= daily active <= registered"
            )
        if self.daily_active_players % self.peak_concurrent_players:
            raise ValueError(
                "daily_active_players must be divisible by peak_concurrent_players"
            )
        if set(self.table_size_counts) != {4, 5, 6}:
            raise ValueError("table_size_counts must define 4, 5, and 6")
        if any(count < 0 for count in self.table_size_counts.values()):
            raise ValueError("table size counts must be non-negative")
        if self.table_count < 1:
            raise ValueError("at least one table is required")
        if self.peak_concurrent_players < max(
            size for size, count in self.table_size_counts.items() if count
        ):
            raise ValueError("peak_concurrent_players cannot fill the largest table")
        if self.hands_per_table_hour <= 0:
            raise ValueError("hands_per_table_hour must be positive")
        if not 0 < self.simulated_day_hours <= 24:
            raise ValueError("simulated_day_hours must be in (0, 24]")
        if self.max_simultaneous_tables < 1:
            raise ValueError("max_simultaneous_tables must be positive")
        expected_keys = set(range(1, self.max_simultaneous_tables + 1))
        if set(self.simultaneous_table_distribution) != expected_keys:
            raise ValueError(
                "simultaneous_table_distribution must define every count "
                "from 1 through max_simultaneous_tables"
            )
        if any(weight < 0 for weight in self.simultaneous_table_distribution.values()):
            raise ValueError("simultaneous table weights must be non-negative")
        if not math.isclose(
            sum(self.simultaneous_table_distribution.values()),
            1.0,
            rel_tol=0,
            abs_tol=1e-9,
        ):
            raise ValueError("simultaneous table weights must sum to 1")
        expected_capacity = (
            self.peak_concurrent_players * self.expected_tables_per_active_player
        )
        if expected_capacity + 1e-9 < self.concurrent_seats:
            raise ValueError(
                "multi-table distribution cannot fill the configured seats"
            )
        if not 1 <= self.seat_rebalance_minutes <= self.simulated_day_hours * 60:
            raise ValueError("seat_rebalance_minutes is outside the active day")
        if not (1 <= self.minimum_session_minutes <= self.maximum_session_minutes):
            raise ValueError("session minute bounds are invalid")
        if not (
            self.minimum_session_minutes
            <= self.cohort_minutes
            <= self.maximum_session_minutes
        ):
            raise ValueError(
                "daily/peak player counts create sessions outside configured bounds"
            )
        if (
            self.n_colluding_pairs < 0
            or self.n_colluding_pairs * 2 > self.registered_players
        ):
            raise ValueError(
                "n_colluding_pairs must use at most half the registered players"
            )


@dataclass(frozen=True)
class SessionInterval:
    session_id: str
    player_id: str
    started_at: datetime
    ended_at: datetime
    simultaneous_tables: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "player_id": self.player_id,
            "started_at": _iso(self.started_at),
            "ended_at": _iso(self.ended_at),
            "simultaneous_tables": self.simultaneous_tables,
        }


@dataclass(frozen=True)
class SeatAssignment:
    table_id: str
    seat_index: int
    player_id: str
    session_id: str
    effective_from: datetime
    effective_to: datetime
    simultaneous_tables: int

    def to_dict(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
            "seat_index": self.seat_index,
            "player_id": self.player_id,
            "session_id": self.session_id,
            "effective_from": _iso(self.effective_from),
            "effective_to": _iso(self.effective_to),
            "simultaneous_tables": self.simultaneous_tables,
        }


@dataclass(frozen=True)
class ScheduledHand:
    global_index: int
    table_id: str
    table_hand_sequence_no: int
    played_at: datetime
    seat_player_ids: tuple[str, ...]
    seat_simultaneous_tables: tuple[int, ...]
    seat_effective_from: datetime
    seat_effective_to: datetime


@dataclass
class _DailySchedule:
    cohorts: tuple[tuple[str, ...], ...]
    quotas: tuple[Mapping[str, int], ...]
    sessions: tuple[SessionInterval, ...]
    session_by_player: Mapping[str, SessionInterval]


class MultiTableScheduler:
    """Schedule hands across independent table clocks and active-user cohorts."""

    def __init__(
        self,
        generator: HandGenerator,
        profile: MultiTableProfile,
        *,
        start_at: datetime,
        seed: int,
    ) -> None:
        if len(generator.tables) != profile.table_count:
            raise ValueError("generator table count does not match profile")
        self.generator = generator
        self.profile = profile
        self.start_at = _utc(start_at, "start_at")
        self.seed = seed
        self._clock_rng = random.Random(seed + 310_003)
        self._daily: dict[int, _DailySchedule] = {}
        self._rosters: dict[
            tuple[int, int, int],
            Mapping[str, tuple[str, ...]],
        ] = {}
        self._session_records: list[SessionInterval] = []
        self._seat_records: list[SeatAssignment] = []

    @property
    def session_records(self) -> tuple[SessionInterval, ...]:
        return tuple(self._session_records)

    @property
    def seat_assignment_records(self) -> tuple[SeatAssignment, ...]:
        return tuple(self._seat_records)

    def iter_hands(self, count: int) -> Iterator[ScheduledHand]:
        if count < 0:
            raise ValueError("scheduled hand count must be non-negative")
        mean_interval = 3600.0 / self.profile.hands_per_table_hour
        heap: list[tuple[datetime, int, int]] = []
        for table_index in range(self.profile.table_count):
            jitter = self._clock_rng.uniform(0.0, mean_interval)
            heapq.heappush(
                heap,
                (
                    self.start_at + timedelta(seconds=jitter),
                    table_index,
                    0,
                ),
            )

        for global_index in range(count):
            played_at, table_index, table_sequence = heapq.heappop(heap)
            table_id = self.generator.tables[table_index]
            roster = list(self._roster_at(played_at)[table_id])
            rotation = table_sequence % len(roster)
            ordered_seats = tuple(roster[rotation:] + roster[:rotation])
            (
                day_index,
                cohort_index,
                _,
                effective_from,
                effective_to,
            ) = self._time_coordinates(played_at)
            quotas = self._daily_schedule(day_index).quotas[cohort_index]
            yield ScheduledHand(
                global_index=global_index,
                table_id=table_id,
                table_hand_sequence_no=table_sequence,
                played_at=played_at,
                seat_player_ids=ordered_seats,
                seat_simultaneous_tables=tuple(
                    quotas[player_id] for player_id in ordered_seats
                ),
                seat_effective_from=effective_from,
                seat_effective_to=effective_to,
            )

            interval = mean_interval * self._clock_rng.uniform(0.80, 1.20)
            next_time = self._next_active_time(
                played_at + timedelta(seconds=interval),
                mean_interval=mean_interval,
            )
            heapq.heappush(
                heap,
                (next_time, table_index, table_sequence + 1),
            )

    def _next_active_time(
        self,
        candidate: datetime,
        *,
        mean_interval: float,
    ) -> datetime:
        elapsed = candidate - self.start_at
        day_index = max(0, int(elapsed.total_seconds() // 86_400))
        day_start = self.start_at + timedelta(days=day_index)
        active_end = day_start + timedelta(hours=self.profile.simulated_day_hours)
        if candidate < active_end:
            return candidate
        next_day = day_start + timedelta(days=1)
        return next_day + timedelta(seconds=self._clock_rng.uniform(0.0, mean_interval))

    def _time_coordinates(
        self,
        played_at: datetime,
    ) -> tuple[int, int, int, datetime, datetime]:
        elapsed_seconds = (played_at - self.start_at).total_seconds()
        day_index = max(0, int(elapsed_seconds // 86_400))
        day_start = self.start_at + timedelta(days=day_index)
        active_seconds = (played_at - day_start).total_seconds()
        cohort_seconds = self.profile.cohort_minutes * 60.0
        cohort_index = min(
            self.profile.cohort_count - 1,
            int(active_seconds // cohort_seconds),
        )
        epoch_seconds = self.profile.seat_rebalance_minutes * 60
        epoch_index = int(active_seconds // epoch_seconds)
        cohort_start = day_start + timedelta(seconds=cohort_index * cohort_seconds)
        cohort_end = min(
            day_start + timedelta(seconds=(cohort_index + 1) * cohort_seconds),
            day_start + timedelta(hours=self.profile.simulated_day_hours),
        )
        epoch_start = day_start + timedelta(seconds=epoch_index * epoch_seconds)
        epoch_end = min(
            epoch_start + timedelta(seconds=epoch_seconds),
            day_start + timedelta(hours=self.profile.simulated_day_hours),
        )
        return (
            day_index,
            cohort_index,
            epoch_index,
            max(cohort_start, epoch_start),
            min(cohort_end, epoch_end),
        )

    def _daily_schedule(self, day_index: int) -> _DailySchedule:
        existing = self._daily.get(day_index)
        if existing is not None:
            return existing

        rng = random.Random(self.seed + 1_000_003 * (day_index + 1))
        players = rng.sample(
            [player.player_id for player in self.generator.players],
            self.profile.daily_active_players,
        )
        cohort_size = self.profile.peak_concurrent_players
        cohorts = tuple(
            tuple(players[index : index + cohort_size])
            for index in range(0, len(players), cohort_size)
        )
        day_start = self.start_at + timedelta(days=day_index)
        cohort_duration = timedelta(minutes=self.profile.cohort_minutes)
        all_sessions: list[SessionInterval] = []
        quotas: list[Mapping[str, int]] = []
        for cohort_index, cohort in enumerate(cohorts):
            cohort_rng = random.Random(
                self.seed + 1_000_003 * (day_index + 1) + 10_007 * (cohort_index + 1)
            )
            cohort_quotas = self._assign_quotas(cohort, cohort_rng)
            quotas.append(MappingProxyType(cohort_quotas))
            started_at = day_start + cohort_index * cohort_duration
            ended_at = started_at + cohort_duration
            for player_id in cohort:
                all_sessions.append(
                    SessionInterval(
                        session_id=str(
                            _stable_uuid(
                                self.profile.dataset_id,
                                self.seed,
                                "multitable-session",
                                day_index,
                                cohort_index,
                                player_id,
                            )
                        ),
                        player_id=player_id,
                        started_at=started_at,
                        ended_at=ended_at,
                        simultaneous_tables=cohort_quotas[player_id],
                    )
                )
        schedule = _DailySchedule(
            cohorts=cohorts,
            quotas=tuple(quotas),
            sessions=tuple(all_sessions),
            session_by_player=MappingProxyType(
                {session.player_id: session for session in all_sessions}
            ),
        )
        self._daily[day_index] = schedule
        self._session_records.extend(all_sessions)
        return schedule

    def _assign_quotas(
        self,
        players: tuple[str, ...],
        rng: random.Random,
    ) -> dict[str, int]:
        population = len(players)
        raw_counts = {
            table_count: population * weight
            for table_count, weight in self.profile.simultaneous_table_distribution.items()
        }
        bucket_counts = {
            table_count: int(math.floor(raw)) for table_count, raw in raw_counts.items()
        }
        unassigned = population - sum(bucket_counts.values())
        remainders = sorted(
            raw_counts,
            key=lambda count: (
                raw_counts[count] - bucket_counts[count],
                -count,
            ),
            reverse=True,
        )
        for index in range(unassigned):
            bucket_counts[remainders[index % len(remainders)]] += 1

        shuffled = list(players)
        rng.shuffle(shuffled)
        counts: list[int] = []
        for table_count in sorted(bucket_counts):
            counts.extend([table_count] * bucket_counts[table_count])
        rng.shuffle(counts)
        quotas = dict(zip(shuffled, counts, strict=True))

        total = sum(quotas.values())
        while total > self.profile.concurrent_seats:
            candidates = [player_id for player_id, count in quotas.items() if count > 1]
            if not candidates:
                raise ValueError("cannot reduce player quotas to seat capacity")
            player_id = rng.choice(candidates)
            quotas[player_id] -= 1
            total -= 1
        while total < self.profile.concurrent_seats:
            candidates = [
                player_id
                for player_id, count in quotas.items()
                if count < self.profile.max_simultaneous_tables
            ]
            if not candidates:
                raise ValueError("cannot increase player quotas to seat capacity")
            player_id = rng.choice(candidates)
            quotas[player_id] += 1
            total += 1
        return quotas

    def _roster_at(self, played_at: datetime) -> Mapping[str, tuple[str, ...]]:
        (
            day_index,
            cohort_index,
            epoch_index,
            effective_from,
            effective_to,
        ) = self._time_coordinates(played_at)
        key = (day_index, cohort_index, epoch_index)
        existing = self._rosters.get(key)
        if existing is not None:
            return existing

        daily = self._daily_schedule(day_index)
        players = daily.cohorts[cohort_index]
        quotas = daily.quotas[cohort_index]
        roster_rng = random.Random(
            self.seed
            + 1_000_003 * (day_index + 1)
            + 10_007 * (cohort_index + 1)
            + 101 * (epoch_index + 1)
        )
        rosters = self._assign_tables(players, quotas, roster_rng)
        frozen = MappingProxyType(
            {table_id: tuple(seats) for table_id, seats in rosters.items()}
        )
        self._rosters[key] = frozen

        for table_id in self.generator.tables:
            for seat_index, player_id in enumerate(frozen[table_id]):
                session = daily.session_by_player[player_id]
                self._seat_records.append(
                    SeatAssignment(
                        table_id=table_id,
                        seat_index=seat_index,
                        player_id=player_id,
                        session_id=session.session_id,
                        effective_from=effective_from,
                        effective_to=effective_to,
                        simultaneous_tables=quotas[player_id],
                    )
                )
        return frozen

    def _assign_tables(
        self,
        players: tuple[str, ...],
        quotas: Mapping[str, int],
        rng: random.Random,
    ) -> dict[str, list[str]]:
        sizes = list(
            itertools.chain.from_iterable(
                [size] * count
                for size, count in sorted(self.profile.table_size_counts.items())
            )
        )
        rng.shuffle(sizes)
        table_capacities = dict(zip(self.generator.tables, sizes, strict=True))

        for attempt in range(30):
            attempt_rng = random.Random(rng.randrange(2**63) + attempt)
            rosters = {table_id: [] for table_id in self.generator.tables}
            remaining = dict(table_capacities)
            player_order = list(players)
            attempt_rng.shuffle(player_order)
            player_order.sort(key=lambda player_id: quotas[player_id], reverse=True)
            valid = True
            for player_id in player_order:
                selected: set[str] = set()
                for _ in range(quotas[player_id]):
                    candidates = [
                        table_id
                        for table_id, capacity in remaining.items()
                        if capacity > 0 and table_id not in selected
                    ]
                    if not candidates:
                        valid = False
                        break
                    max_remaining = max(remaining[table_id] for table_id in candidates)
                    best = [
                        table_id
                        for table_id in candidates
                        if remaining[table_id] == max_remaining
                    ]
                    table_id = attempt_rng.choice(best)
                    rosters[table_id].append(player_id)
                    remaining[table_id] -= 1
                    selected.add(table_id)
                if not valid:
                    break
            if valid and all(capacity == 0 for capacity in remaining.values()):
                return rosters
        raise RuntimeError("could not construct unique multi-table seat assignment")


class MultiTablePokerWorld:
    """Correlate a scheduled table world with legal PokerKit hands and labels."""

    def __init__(
        self,
        profile: MultiTableProfile,
        *,
        split: str,
        hand_count: int,
        seed: int,
        start_at: datetime,
        scenario_plan: ScenarioPlan | None = None,
        tenant_id: str = "demo",
        product_id: str = "poker",
    ) -> None:
        if split not in SPLIT_NAMES:
            raise ValueError(f"unsupported dataset split: {split}")
        self.profile = profile
        self.split = split
        self.seed = seed
        self.tenant_id = tenant_id
        self.product_id = product_id
        self.generator = HandGenerator(
            GeneratorConfig(
                n_hands=0,
                n_players=profile.registered_players,
                n_tables=profile.table_count,
                n_colluding_pairs=profile.n_colluding_pairs,
                seed=seed,
                dataset_split=split,
                dataset_id=profile.dataset_id,
            ),
            start_at=start_at,
        )
        self.scheduler = MultiTableScheduler(
            self.generator,
            profile,
            start_at=start_at,
            seed=seed,
        )
        self.hand_count = hand_count
        self.scenario_planner = (
            ScenarioPlanner(
                scenario_plan,
                dataset_id=profile.dataset_id,
                split=split,
                hand_count=hand_count,
                table_count=profile.table_count,
                hands_per_table_hour=profile.hands_per_table_hour,
                seed=seed,
            )
            if scenario_plan is not None
            else None
        )
        self.scenario_summary: dict[str, Any] | None = None

    @property
    def player_ids(self) -> tuple[str, ...]:
        return tuple(player.player_id for player in self.generator.players)

    def iter_hands_with_labels(
        self,
    ) -> Iterator[
        tuple[
            dict[str, Any],
            list[PlayerHandLabel],
            list[PairHandLabel],
            ScheduledHand,
        ]
    ]:
        for scheduled in self.scheduler.iter_hands(self.hand_count):
            scenario = (
                self.scenario_planner.assignment_for(scheduled)
                if self.scenario_planner is not None
                else None
            )
            raw_hand = self.generator.generate_hand(
                scheduled.global_index,
                table_id=scheduled.table_id,
                seat_player_ids=scheduled.seat_player_ids,
                played_at=scheduled.played_at,
                planned_collusion_pair=(
                    scenario.planned_pair() if scenario is not None else None
                ),
                allow_random_collusion=self.scenario_planner is None,
            )
            safe_hand, raw_player_labels = separate_hand_labels(raw_hand)
            payload = HandCompletedPayload.model_validate(safe_hand)
            hand_event = build_event(
                event_type=HAND_COMPLETED,
                aggregate_id=payload.hand_id,
                payload=payload,
                dataset_id=self.profile.dataset_id,
                dataset_split=self.split,
                occurred_at=payload.played_at,
                emitted_at=payload.played_at + timedelta(seconds=1),
                tenant_id=self.tenant_id,
                product_id=self.product_id,
            )
            available_at = payload.played_at + timedelta(days=7)
            raw_labels_by_player = {
                str(raw_label["player_id"]): raw_label
                for raw_label in raw_player_labels
            }
            player_labels = [
                PlayerHandLabel(
                    example_id=_stable_uuid(
                        self.profile.dataset_id,
                        self.split,
                        "player-label",
                        payload.hand_id,
                        player.player_id,
                    ),
                    dataset_id=self.profile.dataset_id,
                    dataset_split=self.split,
                    hand_id=payload.hand_id,
                    player_id=player.player_id,
                    is_suspicious=self._is_suspicious_player(
                        player.player_id,
                        scenario,
                        raw_labels_by_player,
                    ),
                    collusion_pair_id=self._collusion_group_for_player(
                        player.player_id,
                        scenario,
                        raw_labels_by_player,
                    ),
                    label_available_at=available_at,
                )
                for player in payload.players
            ]
            labels_by_player = {label.player_id: label for label in player_labels}
            pair_labels: list[PairHandLabel] = []
            for player_a, player_b in itertools.combinations(
                sorted(labels_by_player),
                2,
            ):
                left = labels_by_player[player_a]
                right = labels_by_player[player_b]
                collusion_pair_id = (
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
                            self.profile.dataset_id,
                            self.split,
                            "pair-label",
                            payload.hand_id,
                            pair_key,
                        ),
                        dataset_id=self.profile.dataset_id,
                        dataset_split=self.split,
                        hand_id=payload.hand_id,
                        pair_key=pair_key,
                        player_a=player_a,
                        player_b=player_b,
                        is_collusive=collusion_pair_id is not None,
                        collusion_pair_id=collusion_pair_id,
                        label_available_at=available_at,
                    )
                )
            if scenario is not None and self.scenario_planner is not None:
                self.scenario_planner.record_hand(
                    scenario,
                    hand_id=payload.hand_id,
                    played_at=payload.played_at,
                )
            yield (
                hand_event.model_dump(mode="json"),
                player_labels,
                pair_labels,
                scheduled,
            )
        if self.scenario_planner is not None:
            self.scenario_summary = self.scenario_planner.finalize()

    @staticmethod
    def _is_suspicious_player(
        player_id: str,
        scenario: ScenarioAssignment | None,
        raw_labels_by_player: Mapping[str, dict[str, Any]],
    ) -> bool:
        if scenario is not None:
            return scenario.is_collusive and player_id in scenario.members
        return bool(raw_labels_by_player[player_id]["is_suspicious"])

    @staticmethod
    def _collusion_group_for_player(
        player_id: str,
        scenario: ScenarioAssignment | None,
        raw_labels_by_player: Mapping[str, dict[str, Any]],
    ) -> str | None:
        if scenario is not None:
            return (
                scenario.group_id
                if scenario.is_collusive and player_id in scenario.members
                else None
            )
        value = raw_labels_by_player[player_id]["collusion_pair_id"]
        return str(value) if value is not None else None


def build_multitable_dataset(
    output_dir: Path,
    profile: MultiTableProfile,
    *,
    scenario_plan: ScenarioPlan | None = None,
    start_at: datetime | None = None,
    tenant_id: str = "demo",
    product_id: str = "poker",
) -> dict[str, Any]:
    """Write deterministic multi-table events, labels, schedules, and hashes."""
    if start_at is None:
        start_at = datetime(2026, 8, 1, tzinfo=timezone.utc)
    start_at = _utc(start_at, "start_at")
    output_dir.mkdir(parents=True, exist_ok=True)

    config_path = output_dir / "config.json"
    config_path.write_text(_json_line(profile.to_dict()))
    scenario_config_path: Path | None = None
    if scenario_plan is not None:
        scenario_config_path = output_dir / "scenario_config.json"
        scenario_config_path.write_text(_json_line(scenario_plan.to_dict()))
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "dataset_id": profile.dataset_id,
        "profile_id": profile.profile_id,
        "generator": "MultiTablePokerWorld+PokerKit",
        "generator_version": "multitable-v1",
        "pokerkit_version": version("pokerkit"),
        "base_seed": profile.seed,
        "player_populations_disjoint": True,
        "requested": {
            "tables": profile.table_count,
            "concurrent_seats": profile.concurrent_seats,
            "registered_players": profile.registered_players,
            "daily_active_players": profile.daily_active_players,
            "peak_concurrent_players": profile.peak_concurrent_players,
            "table_size_counts": {
                str(size): count
                for size, count in sorted(profile.table_size_counts.items())
            },
            "expected_tables_per_active_player": (
                profile.expected_tables_per_active_player
            ),
        },
        "scenario_plan_id": (
            scenario_plan.scenario_plan_id if scenario_plan is not None else None
        ),
        "artifacts": {"config.json": _sha256(config_path)},
        "splits": {},
    }
    if scenario_config_path is not None:
        manifest["artifacts"]["scenario_config.json"] = _sha256(scenario_config_path)
    populations: dict[str, set[str]] = {}

    for split in SPLIT_NAMES:
        split_seed = profile.seed + _SEED_OFFSETS[split]
        world = MultiTablePokerWorld(
            profile,
            split=split,
            hand_count=profile.split_hands[split],
            seed=split_seed,
            start_at=start_at,
            scenario_plan=scenario_plan,
            tenant_id=tenant_id,
            product_id=product_id,
        )
        populations[split] = set(world.player_ids)
        split_dir = output_dir / split
        events_dir = split_dir / "events"
        labels_dir = split_dir / (
            "private_labels" if split == "challenge" else "labels"
        )
        schedule_dir = split_dir / "schedule"
        snapshots_dir = split_dir / "snapshots"
        for directory in (events_dir, labels_dir, schedule_dir, snapshots_dir):
            directory.mkdir(parents=True, exist_ok=True)

        hands_path = events_dir / "hands.jsonl"
        player_labels_path = labels_dir / "player_labels.jsonl"
        pair_labels_path = labels_dir / "pair_labels.jsonl"
        hand_schedule_path = schedule_dir / "hands.jsonl"
        hand_count = 0
        player_label_count = 0
        pair_label_count = 0
        positive_player_count = 0
        positive_pair_count = 0
        players_observed: set[str] = set()
        table_hand_sizes: Counter[int] = Counter()
        first_played_at: datetime | None = None
        last_played_at: datetime | None = None

        with (
            hands_path.open("w") as hands_file,
            player_labels_path.open("w") as player_labels_file,
            pair_labels_path.open("w") as pair_labels_file,
            hand_schedule_path.open("w") as schedule_file,
        ):
            for (
                event,
                player_labels,
                pair_labels,
                scheduled,
            ) in world.iter_hands_with_labels():
                hands_file.write(_json_line(event))
                hand_count += 1
                played_at = datetime.fromisoformat(
                    str(event["payload"]["played_at"]).replace("Z", "+00:00")
                )
                first_played_at = (
                    played_at
                    if first_played_at is None
                    else min(first_played_at, played_at)
                )
                last_played_at = (
                    played_at
                    if last_played_at is None
                    else max(last_played_at, played_at)
                )
                player_ids = [
                    str(player["player_id"]) for player in event["payload"]["players"]
                ]
                players_observed.update(player_ids)
                table_hand_sizes[len(player_ids)] += 1
                schedule_file.write(
                    _json_line(
                        {
                            "hand_id": event["payload"]["hand_id"],
                            "table_id": scheduled.table_id,
                            "table_hand_sequence_no": (
                                scheduled.table_hand_sequence_no
                            ),
                            "played_at": _iso(scheduled.played_at),
                            "num_players": len(scheduled.seat_player_ids),
                        }
                    )
                )
                for label in player_labels:
                    player_labels_file.write(_json_line(label))
                    player_label_count += 1
                    positive_player_count += int(label.is_suspicious)
                for label in pair_labels:
                    pair_labels_file.write(_json_line(label))
                    pair_label_count += 1
                    positive_pair_count += int(label.is_collusive)

        sessions_path = schedule_dir / "sessions.jsonl"
        seats_path = schedule_dir / "seat_assignments.jsonl"
        with sessions_path.open("w") as stream:
            for session in world.scheduler.session_records:
                stream.write(_json_line(session.to_dict()))
        with seats_path.open("w") as stream:
            for assignment in world.scheduler.seat_assignment_records:
                stream.write(_json_line(assignment.to_dict()))

        scenario_paths: tuple[Path, ...] = ()
        if world.scenario_planner is not None:
            cases_path = labels_dir / "scenario_cases.jsonl"
            groups_path = labels_dir / "group_labels.jsonl"
            scenario_hands_path = labels_dir / "scenario_hand_labels.jsonl"
            with cases_path.open("w") as stream:
                for row in world.scenario_planner.case_rows:
                    stream.write(_json_line(row))
            with groups_path.open("w") as stream:
                for row in world.scenario_planner.group_rows:
                    stream.write(_json_line(row))
            with scenario_hands_path.open("w") as stream:
                for row in world.scenario_planner.hand_rows:
                    stream.write(_json_line(row))
            scenario_paths = (cases_path, groups_path, scenario_hands_path)

        context_rows, context_summary = build_multitable_user_contexts(
            world.player_ids,
            dataset_id=profile.dataset_id,
            split=split,
            seed=split_seed,
            effective_anchor=start_at - timedelta(days=1),
            group_rows=(
                world.scenario_planner.case_rows
                if world.scenario_planner is not None
                else ()
            ),
        )
        users_snapshot_path = snapshots_dir / "users.jsonl"
        with users_snapshot_path.open("w") as stream:
            for context in context_rows:
                stream.write(_json_line(context))

        concurrency_histogram = Counter(
            assignment.simultaneous_tables
            for assignment in world.scheduler.seat_assignment_records
        )
        session_concurrency_histogram = Counter(
            session.simultaneous_tables for session in world.scheduler.session_records
        )
        files = (
            hands_path,
            player_labels_path,
            pair_labels_path,
            hand_schedule_path,
            sessions_path,
            seats_path,
            users_snapshot_path,
            *scenario_paths,
        )
        split_hashes = {
            str(path.relative_to(output_dir)): _sha256(path) for path in files
        }
        manifest["artifacts"].update(split_hashes)
        manifest["splits"][split] = {
            "seed": split_seed,
            "hands": hand_count,
            "registered_players": len(world.player_ids),
            "population_sha256": _population_hash(world.player_ids),
            "players_observed_in_hands": len(players_observed),
            "sessions": len(world.scheduler.session_records),
            "seat_assignments": len(world.scheduler.seat_assignment_records),
            "player_label_rows": player_label_count,
            "positive_player_label_rows": positive_player_count,
            "pair_label_rows": pair_label_count,
            "positive_pair_label_rows": positive_pair_count,
            "table_size_hand_histogram": {
                str(size): table_hand_sizes.get(size, 0) for size in (4, 5, 6)
            },
            "seat_assignment_concurrency_histogram": {
                str(table_count): concurrency_histogram.get(table_count, 0)
                for table_count in range(
                    1,
                    profile.max_simultaneous_tables + 1,
                )
            },
            "session_concurrency_histogram": {
                str(table_count): session_concurrency_histogram.get(
                    table_count,
                    0,
                )
                for table_count in range(
                    1,
                    profile.max_simultaneous_tables + 1,
                )
            },
            "first_played_at": (
                _iso(first_played_at) if first_played_at is not None else None
            ),
            "last_played_at": (
                _iso(last_played_at) if last_played_at is not None else None
            ),
            "user_context": context_summary,
            "scenarios": world.scenario_summary,
            "artifacts": split_hashes,
        }

    for left_index, left in enumerate(SPLIT_NAMES):
        for right in SPLIT_NAMES[left_index + 1 :]:
            overlap = populations[left] & populations[right]
            if overlap:
                raise RuntimeError(
                    f"player leakage between {left} and {right}: {len(overlap)}"
                )

    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest
