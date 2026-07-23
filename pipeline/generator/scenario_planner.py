"""Private, deterministic multi-hand scenario planning for synthetic data."""

from __future__ import annotations

import itertools
import math
import random
import uuid
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping

from .collusion_patterns import CollusionPair, CollusionPattern

if TYPE_CHECKING:
    from .multitable import ScheduledHand


POSITIVE_FAMILIES = tuple(pattern.value for pattern in CollusionPattern)
HARD_NEGATIVE_FAMILIES = (
    "legitimate_multitabler",
    "innocent_household",
    "shared_network",
    "frequent_coplayer",
)
_PLAN_FIELDS = {
    "schema_version",
    "scenario_plan_id",
    "positive_hand_rate",
    "hard_negative_hand_rate",
    "hands_per_case",
    "minimum_cases_per_positive_family",
    "minimum_cases_per_hard_negative_family",
    "positive_family_weights",
    "hard_negative_family_weights",
    "ring_cases_per_split",
    "ring_members",
    "seed_offset",
}
_CONTEXT_RELATIONSHIP = {
    "legitimate_multitabler": None,
    "innocent_household": "same_device",
    "shared_network": "same_network",
    "frequent_coplayer": None,
}
_RING_PATTERNS = (
    CollusionPattern.FOLD_BENEFIT,
    CollusionPattern.CHIP_DUMP,
    CollusionPattern.SOFT_PLAY,
)


def _stable_uuid(*parts: object) -> str:
    return str(
        uuid.uuid5(
            uuid.NAMESPACE_URL,
            ":".join(str(part) for part in parts),
        )
    )


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class ScenarioPlan:
    """Versioned prevalence and case-shape configuration."""

    schema_version: int
    scenario_plan_id: str
    positive_hand_rate: float
    hard_negative_hand_rate: float
    hands_per_case: int
    minimum_cases_per_positive_family: int
    minimum_cases_per_hard_negative_family: int
    positive_family_weights: Mapping[str, float]
    hard_negative_family_weights: Mapping[str, float]
    ring_cases_per_split: int
    ring_members: int
    seed_offset: int

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "positive_family_weights",
            MappingProxyType(
                {
                    str(name): float(weight)
                    for name, weight in self.positive_family_weights.items()
                }
            ),
        )
        object.__setattr__(
            self,
            "hard_negative_family_weights",
            MappingProxyType(
                {
                    str(name): float(weight)
                    for name, weight in self.hard_negative_family_weights.items()
                }
            ),
        )
        self._validate()

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "ScenarioPlan":
        unknown = set(raw) - _PLAN_FIELDS
        missing = _PLAN_FIELDS - set(raw)
        if unknown:
            raise ValueError(f"unknown scenario plan field: {sorted(unknown)[0]}")
        if missing:
            raise ValueError(f"missing scenario plan field: {sorted(missing)[0]}")
        return cls(
            schema_version=int(raw["schema_version"]),
            scenario_plan_id=str(raw["scenario_plan_id"]),
            positive_hand_rate=float(raw["positive_hand_rate"]),
            hard_negative_hand_rate=float(raw["hard_negative_hand_rate"]),
            hands_per_case=int(raw["hands_per_case"]),
            minimum_cases_per_positive_family=int(
                raw["minimum_cases_per_positive_family"]
            ),
            minimum_cases_per_hard_negative_family=int(
                raw["minimum_cases_per_hard_negative_family"]
            ),
            positive_family_weights=dict(raw["positive_family_weights"]),
            hard_negative_family_weights=dict(raw["hard_negative_family_weights"]),
            ring_cases_per_split=int(raw["ring_cases_per_split"]),
            ring_members=int(raw["ring_members"]),
            seed_offset=int(raw["seed_offset"]),
        )

    @classmethod
    def from_json(cls, path: Path) -> "ScenarioPlan":
        import json

        return cls.from_dict(json.loads(path.read_text()))

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "scenario_plan_id": self.scenario_plan_id,
            "positive_hand_rate": self.positive_hand_rate,
            "hard_negative_hand_rate": self.hard_negative_hand_rate,
            "hands_per_case": self.hands_per_case,
            "minimum_cases_per_positive_family": (
                self.minimum_cases_per_positive_family
            ),
            "minimum_cases_per_hard_negative_family": (
                self.minimum_cases_per_hard_negative_family
            ),
            "positive_family_weights": dict(
                sorted(self.positive_family_weights.items())
            ),
            "hard_negative_family_weights": dict(
                sorted(self.hard_negative_family_weights.items())
            ),
            "ring_cases_per_split": self.ring_cases_per_split,
            "ring_members": self.ring_members,
            "seed_offset": self.seed_offset,
        }

    def _validate(self) -> None:
        if self.schema_version != 1:
            raise ValueError("scenario plan schema_version must be 1")
        if (
            not self.scenario_plan_id
            or not self.scenario_plan_id.replace("-", "").replace("_", "").isalnum()
        ):
            raise ValueError("scenario_plan_id must be an alphanumeric label")
        for name, rate in (
            ("positive_hand_rate", self.positive_hand_rate),
            ("hard_negative_hand_rate", self.hard_negative_hand_rate),
        ):
            if not 0 <= rate < 1:
                raise ValueError(f"{name} must be in [0, 1)")
        if self.positive_hand_rate + self.hard_negative_hand_rate >= 1:
            raise ValueError("combined scenario hand rates must be below 1")
        if self.hands_per_case < 1:
            raise ValueError("hands_per_case must be positive")
        if self.minimum_cases_per_positive_family < 0:
            raise ValueError("minimum positive cases cannot be negative")
        if self.minimum_cases_per_hard_negative_family < 0:
            raise ValueError("minimum hard-negative cases cannot be negative")
        self._validate_weights(
            self.positive_family_weights,
            POSITIVE_FAMILIES,
            "positive_family_weights",
        )
        self._validate_weights(
            self.hard_negative_family_weights,
            HARD_NEGATIVE_FAMILIES,
            "hard_negative_family_weights",
        )
        if self.ring_cases_per_split < 0:
            raise ValueError("ring_cases_per_split cannot be negative")
        if not 3 <= self.ring_members <= 6:
            raise ValueError("ring_members must be between 3 and 6")

    @staticmethod
    def _validate_weights(
        values: Mapping[str, float],
        expected: tuple[str, ...],
        name: str,
    ) -> None:
        if set(values) != set(expected):
            raise ValueError(f"{name} must define exactly {', '.join(expected)}")
        if any(weight < 0 for weight in values.values()):
            raise ValueError(f"{name} cannot contain negative weights")
        if not math.isclose(
            sum(values.values()),
            1.0,
            rel_tol=0,
            abs_tol=1e-9,
        ):
            raise ValueError(f"{name} must sum to 1")


@dataclass(frozen=True)
class ScenarioAssignment:
    case_id: str
    group_id: str
    case_kind: str
    scenario_family: str
    members: tuple[str, ...]
    behavior_pair: tuple[str, str] | None
    behavior_pattern: CollusionPattern | None
    hand_index: int
    planned_hands: int
    required_context_relationship: str | None

    @property
    def is_collusive(self) -> bool:
        return self.case_kind in {"positive_pair", "positive_ring"}

    def planned_pair(self) -> CollusionPair | None:
        if self.behavior_pair is None or self.behavior_pattern is None:
            return None
        return CollusionPair(
            pair_id=self.group_id,
            player_a=self.behavior_pair[0],
            player_b=self.behavior_pair[1],
            pattern=self.behavior_pattern,
            intensity=1.0,
        )


@dataclass(frozen=True)
class _CaseSpec:
    ordinal: int
    case_kind: str
    scenario_family: str
    target_index: int
    member_count: int
    required_context_relationship: str | None


@dataclass
class _ActiveCase:
    spec: _CaseSpec
    case_id: str
    group_id: str
    table_id: str
    members: tuple[str, ...]
    started_at: datetime
    hand_ids: list[str] = field(default_factory=list)
    played_at: list[datetime] = field(default_factory=list)


class ScenarioPlanner:
    """Schedule complete, non-overlapping cases on already valid table rosters."""

    def __init__(
        self,
        plan: ScenarioPlan,
        *,
        dataset_id: str,
        split: str,
        hand_count: int,
        table_count: int,
        hands_per_table_hour: float,
        seed: int,
    ) -> None:
        self.plan = plan
        self.dataset_id = dataset_id
        self.split = split
        self.hand_count = hand_count
        self.table_count = table_count
        self.hands_per_table_hour = hands_per_table_hour
        self.seed = seed
        self.rng = random.Random(seed + plan.seed_offset)
        self._specs = self._build_specs()
        self._next_spec = 0
        self._active_by_table: dict[str, _ActiveCase] = {}
        self._active_by_id: dict[str, _ActiveCase] = {}
        self._used_members: set[str] = set()
        self._case_rows: list[dict[str, Any]] = []
        self._group_rows: list[dict[str, Any]] = []
        self._hand_rows: list[dict[str, Any]] = []

    @property
    def case_rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._case_rows)

    @property
    def group_rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._group_rows)

    @property
    def hand_rows(self) -> tuple[dict[str, Any], ...]:
        return tuple(self._hand_rows)

    def assignment_for(
        self,
        scheduled: "ScheduledHand",
    ) -> ScenarioAssignment | None:
        active = self._active_by_table.get(scheduled.table_id)
        if active is not None:
            if not set(active.members) <= set(scheduled.seat_player_ids):
                raise RuntimeError("scenario members left before case completion")
            return self._assignment(active)

        if self._next_spec >= len(self._specs):
            return None
        spec = self._specs[self._next_spec]
        if scheduled.global_index < spec.target_index:
            return None
        if not self._has_support_window(scheduled):
            return None
        members = self._select_members(spec, scheduled)
        if members is None:
            return None

        case_id = _stable_uuid(
            self.dataset_id,
            self.split,
            self.plan.scenario_plan_id,
            "case",
            spec.ordinal,
        )
        group_id = _stable_uuid(
            self.dataset_id,
            self.split,
            self.plan.scenario_plan_id,
            "group",
            spec.ordinal,
        )
        active = _ActiveCase(
            spec=spec,
            case_id=case_id,
            group_id=group_id,
            table_id=scheduled.table_id,
            members=members,
            started_at=scheduled.played_at,
        )
        self._active_by_table[scheduled.table_id] = active
        self._active_by_id[case_id] = active
        self._used_members.update(members)
        self._next_spec += 1
        return self._assignment(active)

    def record_hand(
        self,
        assignment: ScenarioAssignment,
        *,
        hand_id: str,
        played_at: datetime,
    ) -> None:
        active = self._active_by_id[assignment.case_id]
        if assignment.hand_index != len(active.hand_ids):
            raise RuntimeError("scenario hand index is not contiguous")
        active.hand_ids.append(hand_id)
        active.played_at.append(played_at)
        label_available_at = played_at + timedelta(days=7)
        self._hand_rows.append(
            {
                "schema_version": 1,
                "dataset_id": self.dataset_id,
                "dataset_split": self.split,
                "case_id": assignment.case_id,
                "group_id": assignment.group_id,
                "case_kind": assignment.case_kind,
                "scenario_family": assignment.scenario_family,
                "hand_id": hand_id,
                "hand_index": assignment.hand_index,
                "members": list(assignment.members),
                "behavior_pair": (
                    list(assignment.behavior_pair)
                    if assignment.behavior_pair is not None
                    else None
                ),
                "is_collusive": assignment.is_collusive,
                "required_context_relationship": (
                    assignment.required_context_relationship
                ),
                "label_available_at": _iso(label_available_at),
                "provenance": "synthetic",
            }
        )
        if len(active.hand_ids) == self.plan.hands_per_case:
            self._complete(active)

    def finalize(self) -> dict[str, Any]:
        if self._active_by_id:
            raise RuntimeError(
                f"{len(self._active_by_id)} scenario cases did not complete"
            )
        if self._next_spec != len(self._specs):
            raise RuntimeError(
                f"only {self._next_spec} of {len(self._specs)} cases started"
            )
        case_kind_counts = Counter(row["case_kind"] for row in self._case_rows)
        family_counts = Counter(row["scenario_family"] for row in self._case_rows)
        hand_kind_counts = Counter(row["case_kind"] for row in self._hand_rows)
        positive_pair_rows = sum(
            (
                len(row["members"]) * (len(row["members"]) - 1) // 2
                if row["is_collusive"]
                else 0
            )
            for row in self._hand_rows
        )
        return {
            "scenario_plan_id": self.plan.scenario_plan_id,
            "planned_cases": len(self._specs),
            "completed_cases": len(self._case_rows),
            "case_kind_counts": dict(sorted(case_kind_counts.items())),
            "scenario_family_counts": dict(sorted(family_counts.items())),
            "scenario_hand_counts": dict(sorted(hand_kind_counts.items())),
            "positive_pair_label_rows_expected": positive_pair_rows,
        }

    def _assignment(self, active: _ActiveCase) -> ScenarioAssignment:
        hand_index = len(active.hand_ids)
        behavior_pair: tuple[str, str] | None = None
        behavior_pattern: CollusionPattern | None = None
        if active.spec.case_kind == "positive_pair":
            behavior_pair = (active.members[0], active.members[1])
            behavior_pattern = CollusionPattern(active.spec.scenario_family)
        elif active.spec.case_kind == "positive_ring":
            pairs = tuple(itertools.combinations(active.members, 2))
            behavior_pair = pairs[hand_index % len(pairs)]
            behavior_pattern = _RING_PATTERNS[hand_index % len(_RING_PATTERNS)]
        return ScenarioAssignment(
            case_id=active.case_id,
            group_id=active.group_id,
            case_kind=active.spec.case_kind,
            scenario_family=active.spec.scenario_family,
            members=active.members,
            behavior_pair=behavior_pair,
            behavior_pattern=behavior_pattern,
            hand_index=hand_index,
            planned_hands=self.plan.hands_per_case,
            required_context_relationship=(active.spec.required_context_relationship),
        )

    def _select_members(
        self,
        spec: _CaseSpec,
        scheduled: "ScheduledHand",
    ) -> tuple[str, ...] | None:
        active_members = {
            player_id
            for active in self._active_by_id.values()
            for player_id in active.members
        }
        eligible = [
            (player_id, table_count)
            for player_id, table_count in zip(
                scheduled.seat_player_ids,
                scheduled.seat_simultaneous_tables,
                strict=True,
            )
            if player_id not in active_members
        ]
        unused = [value for value in eligible if value[0] not in self._used_members]
        available = unused if self._can_select(spec, unused) else eligible
        if not self._can_select(spec, available):
            return None
        self.rng.shuffle(available)
        if spec.scenario_family == "legitimate_multitabler":
            multi = [value for value in available if value[1] >= 2]
            if not multi:
                return None
            first = multi[0]
            remainder = [value for value in available if value[0] != first[0]]
            selected = [first, *remainder[: spec.member_count - 1]]
        else:
            selected = available[: spec.member_count]
        return tuple(player_id for player_id, _ in selected)

    @staticmethod
    def _can_select(
        spec: _CaseSpec,
        values: list[tuple[str, int]],
    ) -> bool:
        if len(values) < spec.member_count:
            return False
        if spec.scenario_family == "legitimate_multitabler":
            return any(table_count >= 2 for _, table_count in values)
        return True

    def _has_support_window(self, scheduled: "ScheduledHand") -> bool:
        mean_interval_seconds = 3600.0 / self.hands_per_table_hour
        required_seconds = self.plan.hands_per_case * mean_interval_seconds * 1.25
        return (
            scheduled.seat_effective_to - scheduled.played_at
        ).total_seconds() >= required_seconds

    def _complete(self, active: _ActiveCase) -> None:
        ended_at = active.played_at[-1]
        label_available_at = ended_at + timedelta(days=7)
        common = {
            "schema_version": 1,
            "dataset_id": self.dataset_id,
            "dataset_split": self.split,
            "case_id": active.case_id,
            "group_id": active.group_id,
            "case_kind": active.spec.case_kind,
            "scenario_family": active.spec.scenario_family,
            "members": list(active.members),
            "is_collusive": active.spec.case_kind in {"positive_pair", "positive_ring"},
            "active_from": _iso(active.started_at),
            "active_to": _iso(ended_at),
            "label_available_at": _iso(label_available_at),
            "provenance": "synthetic",
        }
        self._case_rows.append(
            {
                **common,
                "table_id": active.table_id,
                "planned_hands": self.plan.hands_per_case,
                "actual_hands": len(active.hand_ids),
                "hand_ids": list(active.hand_ids),
                "required_context_relationship": (
                    active.spec.required_context_relationship
                ),
            }
        )
        self._group_rows.append(common)
        del self._active_by_table[active.table_id]
        del self._active_by_id[active.case_id]

    def _build_specs(self) -> tuple[_CaseSpec, ...]:
        positive_cases = max(
            self.plan.minimum_cases_per_positive_family * len(POSITIVE_FAMILIES),
            round(
                self.hand_count
                * self.plan.positive_hand_rate
                / self.plan.hands_per_case
            ),
        )
        hard_negative_cases = max(
            self.plan.minimum_cases_per_hard_negative_family
            * len(HARD_NEGATIVE_FAMILIES),
            round(
                self.hand_count
                * self.plan.hard_negative_hand_rate
                / self.plan.hands_per_case
            ),
        )
        values: list[tuple[str, str, int, str | None]] = []
        values.extend(
            (
                "positive_pair",
                family,
                2,
                None,
            )
            for family in self._weighted_families(
                positive_cases,
                self.plan.positive_family_weights,
                self.plan.minimum_cases_per_positive_family,
            )
        )
        values.extend(
            ("positive_ring", "multi_account_ring", self.plan.ring_members, None)
            for _ in range(self.plan.ring_cases_per_split)
        )
        values.extend(
            (
                "hard_negative",
                family,
                2,
                _CONTEXT_RELATIONSHIP[family],
            )
            for family in self._weighted_families(
                hard_negative_cases,
                self.plan.hard_negative_family_weights,
                self.plan.minimum_cases_per_hard_negative_family,
            )
        )
        self.rng.shuffle(values)
        if not values:
            return ()

        latest_start = max(
            0,
            self.hand_count
            - math.ceil(self.plan.hands_per_case * self.table_count * 1.5),
        )
        targets = [
            int((index + 1) * latest_start / (len(values) + 1))
            for index in range(len(values))
        ]
        return tuple(
            _CaseSpec(
                ordinal=ordinal,
                case_kind=kind,
                scenario_family=family,
                target_index=targets[ordinal],
                member_count=member_count,
                required_context_relationship=relationship,
            )
            for ordinal, (kind, family, member_count, relationship) in enumerate(values)
        )

    def _weighted_families(
        self,
        total: int,
        weights: Mapping[str, float],
        minimum_each: int,
    ) -> list[str]:
        counts = {family: minimum_each for family in weights}
        remaining = total - sum(counts.values())
        if remaining < 0:
            raise ValueError("case total is below required family minimum")
        raw = {family: weight * remaining for family, weight in weights.items()}
        for family, value in raw.items():
            counts[family] += int(math.floor(value))
        leftover = total - sum(counts.values())
        order = sorted(
            weights,
            key=lambda family: (
                raw[family] - math.floor(raw[family]),
                family,
            ),
            reverse=True,
        )
        for index in range(leftover):
            counts[order[index % len(order)]] += 1
        values = [family for family in sorted(counts) for _ in range(counts[family])]
        self.rng.shuffle(values)
        return values
