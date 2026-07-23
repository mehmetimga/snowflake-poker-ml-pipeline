"""Deterministic, training-excluded alert-acceptance data product."""

from __future__ import annotations

import hashlib
import itertools
import json
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

from pipeline.context import enrich_player_hand
from pipeline.events import (
    HAND_COMPLETED,
    RISK_ALERT_CREATED,
    RISK_SCORE_COMPUTED,
    USER_CONTEXT_UPDATED,
    HandCompletedPayload,
    PairFeatureEvent,
    PairHandLabel,
    RuleEvidenceEvent,
    UserContextPayload,
    assert_inference_safe,
    build_event,
    stable_review_decision_id,
    validate_event,
)
from pipeline.features import PairFeatureCore
from pipeline.ml.pair_dataset import flatten_pair_feature
from pipeline.ml.pair_inference import PairOnnxScorer
from pipeline.policy.review import load_review_policy
from pipeline.rules import (
    RepeatedFoldWindowRule,
    StatefulPairObservation,
    evaluate_pair_rules,
)

from .collusion_patterns import CollusionPair, CollusionPattern
from .dataset import separate_hand_labels
from .hand_generator import GeneratorConfig, HandGenerator


ALERT_ACCEPTANCE_SCHEMA_VERSION = 1
_PROFILE_FIELDS = {
    "schema_version",
    "profile_id",
    "dataset_id",
    "dataset_split",
    "tenant_id",
    "product_id",
    "seed",
    "player_count",
    "table_count",
    "candidate_hand_limit",
    "repeated_fold_hands",
    "same_device_hands",
    "same_network_hands",
    "innocent_household_hands",
    "multitabler_control_hands",
    "minimum_model_alerts",
}
_CASE_SPECS = (
    (
        "repeated_fold_to_partner_wins",
        "rule_positive",
        True,
        "same_device",
    ),
    ("suspicious_same_device", "rule_positive", True, "same_device"),
    ("suspicious_same_network", "rule_positive", True, "same_network"),
    ("innocent_household", "hard_negative", False, "same_device"),
    ("legitimate_multitabler", "activity_control", False, "none"),
)


def _json_line(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    return json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"


def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open() as stream:
        for line in stream:
            if line.strip():
                yield json.loads(line)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_uuid(*parts: object) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, ":".join(str(part) for part in parts))


def _go_stable_uuid(*parts: str) -> uuid.UUID:
    """Mirror Go's stableUUID, including fmt.Sprint([]string) framing."""

    digest = bytearray(
        hashlib.sha256(("[" + " ".join(parts) + "]").encode("utf-8")).digest()[:16]
    )
    digest[6] = (digest[6] & 0x0F) | 0x50
    digest[8] = (digest[8] & 0x3F) | 0x80
    return uuid.UUID(bytes=bytes(digest))


def _score_id(model_run_id: str, events: Iterable[PairFeatureEvent]) -> str:
    ordered = sorted(events, key=lambda event: event.payload.pair_key)
    parts = [model_run_id]
    parts.extend(
        f"{event.payload.pair_key}:{event.payload.snapshot_revision}:{event.event_id}"
        for event in ordered
    )
    return hashlib.sha256("|".join(parts).encode("utf-8")).digest()[:16].hex()


def _canonical_pair(left: str, right: str) -> tuple[str, str, str]:
    player_a, player_b = sorted((left, right))
    return player_a, player_b, f"{player_a}:{player_b}"


@dataclass(frozen=True)
class AlertAcceptanceProfile:
    schema_version: int
    profile_id: str
    dataset_id: str
    dataset_split: str
    tenant_id: str
    product_id: str
    seed: int
    player_count: int
    table_count: int
    candidate_hand_limit: int
    repeated_fold_hands: int
    same_device_hands: int
    same_network_hands: int
    innocent_household_hands: int
    multitabler_control_hands: int
    minimum_model_alerts: int

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "AlertAcceptanceProfile":
        unknown = set(raw) - _PROFILE_FIELDS
        missing = _PROFILE_FIELDS - set(raw)
        if unknown:
            raise ValueError(
                f"unknown alert-acceptance profile field: {sorted(unknown)[0]}"
            )
        if missing:
            raise ValueError(
                f"missing alert-acceptance profile field: {sorted(missing)[0]}"
            )
        value = cls(**{key: raw[key] for key in _PROFILE_FIELDS})
        value._validate()
        return value

    @classmethod
    def from_json(cls, path: Path) -> "AlertAcceptanceProfile":
        return cls.from_dict(json.loads(path.read_text()))

    def _validate(self) -> None:
        if self.schema_version != ALERT_ACCEPTANCE_SCHEMA_VERSION:
            raise ValueError("alert-acceptance schema_version must be 1")
        if self.dataset_split != "acceptance":
            raise ValueError("alert-acceptance dataset_split must be acceptance")
        if self.player_count < 30 or self.table_count < 5:
            raise ValueError(
                "alert acceptance requires at least 30 players and 5 tables"
            )
        if self.repeated_fold_hands < 6 or self.candidate_hand_limit < 20:
            raise ValueError("repeated-fold acceptance capacity is too small")
        if (
            min(
                self.same_device_hands,
                self.same_network_hands,
                self.innocent_household_hands,
            )
            < 1
        ):
            raise ValueError("rule and hard-negative cases require hands")
        if self.multitabler_control_hands < 2:
            raise ValueError("multitabler control requires at least two hands")
        if self.minimum_model_alerts < 10:
            raise ValueError("D6 requires at least ten model alerts")

    def to_dict(self) -> dict[str, Any]:
        return {key: getattr(self, key) for key in _PROFILE_FIELDS}

    @property
    def output_hand_count(self) -> int:
        return (
            self.repeated_fold_hands
            + self.same_device_hands
            + self.same_network_hands
            + self.innocent_household_hands
            + self.multitabler_control_hands
        )


@dataclass(frozen=True)
class AlertAcceptanceBuildConfig:
    output_dir: Path
    model_dir: Path
    benchmark_dir: Path
    profile: AlertAcceptanceProfile
    review_policy_path: Path = Path("schemas/policies/review-policy-v1.json")
    start_at: datetime = datetime(2026, 9, 1, tzinfo=timezone.utc)

    def __post_init__(self) -> None:
        if self.start_at.tzinfo is None or self.start_at.utcoffset() is None:
            raise ValueError("alert-acceptance start_at must include timezone")
        if self.output_dir.resolve() in {
            self.model_dir.resolve(),
            self.benchmark_dir.resolve(),
        }:
            raise ValueError("alert-acceptance output must be a separate directory")


@dataclass(frozen=True)
class _Case:
    case_id: str
    case_kind: str
    scenario_family: str
    members: tuple[str, str]
    pair_key: str
    hand_ids: tuple[str, ...]
    is_collusive: bool
    required_context_relationship: str
    label_available_at: datetime

    def to_dict(self, profile: AlertAcceptanceProfile) -> dict[str, Any]:
        return {
            "schema_version": ALERT_ACCEPTANCE_SCHEMA_VERSION,
            "dataset_id": profile.dataset_id,
            "dataset_split": profile.dataset_split,
            "case_id": self.case_id,
            "case_kind": self.case_kind,
            "scenario_family": self.scenario_family,
            "members": list(self.members),
            "pair_key": self.pair_key,
            "hand_ids": list(self.hand_ids),
            "is_collusive": self.is_collusive,
            "required_context_relationship": self.required_context_relationship,
            "label_available_at": self.label_available_at.isoformat().replace(
                "+00:00", "Z"
            ),
            "provenance": "synthetic",
        }


def _context_payloads(
    profile: AlertAcceptanceProfile,
    player_ids: list[str],
    start_at: datetime,
) -> dict[str, UserContextPayload]:
    countries = (
        ("TR", "Europe/Istanbul"),
        ("GB", "Europe/London"),
        ("DE", "Europe/Berlin"),
        ("US", "America/New_York"),
        ("BR", "America/Sao_Paulo"),
        ("CA", "America/Toronto"),
    )
    bankrolls = ("low", "medium", "high")
    stakes = ("micro", "low", "medium", "high")
    contexts: dict[str, UserContextPayload] = {}
    for index, player_id in enumerate(player_ids):
        country, timezone_name = countries[index % len(countries)]
        contexts[player_id] = UserContextPayload(
            user_id=player_id,
            context_version=1,
            effective_at=start_at - timedelta(days=1),
            account_created_at=start_at - timedelta(days=100 + index * 47),
            country_bucket=country,
            timezone=timezone_name,
            acquisition_channel=("organic" if index % 2 else "paid"),
            kyc_level="verified",
            account_status="active",
            bankroll_bucket=bankrolls[index % len(bankrolls)],
            preferred_stake_bucket=stakes[index % len(stakes)],
            skill_rating=round(0.08 + (index % 10) * 0.085, 6),
            device_id=f"{profile.dataset_id}_device_{index:03d}",
            network_cluster_id=f"{profile.dataset_id}_network_{index:03d}",
        )

    for group_index, (left_index, right_index, same_device) in enumerate(
        ((0, 1, True), (6, 7, True), (12, 13, False), (18, 19, True))
    ):
        left_id = player_ids[left_index]
        right_id = player_ids[right_index]
        left = contexts[left_id]
        strong_common = {
            "country_bucket": "TR",
            "timezone": "Europe/Istanbul",
            "acquisition_channel": "organic",
            "preferred_stake_bucket": "medium",
            "network_cluster_id": (
                f"{profile.dataset_id}_strong_network_{group_index}"
            ),
        }
        contexts[left_id] = left.model_copy(
            update={
                **strong_common,
                "account_created_at": start_at
                - timedelta(days=1_000 + group_index * 20),
                "skill_rating": 0.70 - group_index * 0.03,
                "bankroll_bucket": "medium",
                "device_id": (f"{profile.dataset_id}_strong_device_{group_index}"),
            }
        )
        contexts[right_id] = contexts[right_id].model_copy(
            update={
                **strong_common,
                "account_created_at": start_at
                - timedelta(days=1_006 + group_index * 20),
                "skill_rating": 0.67 - group_index * 0.03,
                "bankroll_bucket": "high",
                "device_id": (
                    f"{profile.dataset_id}_strong_device_{group_index}"
                    if same_device
                    else f"{profile.dataset_id}_distinct_device_{group_index}"
                ),
            }
        )

    # The legitimate multitabler control is deliberately dissimilar and
    # unlinked even though its first member appears at two tables.
    contexts[player_ids[24]] = contexts[player_ids[24]].model_copy(
        update={
            "account_created_at": start_at - timedelta(days=1_400),
            "skill_rating": 0.12,
            "preferred_stake_bucket": "micro",
            "country_bucket": "TR",
            "timezone": "Europe/Istanbul",
        }
    )
    contexts[player_ids[25]] = contexts[player_ids[25]].model_copy(
        update={
            "account_created_at": start_at - timedelta(days=90),
            "skill_rating": 0.91,
            "preferred_stake_bucket": "high",
            "country_bucket": "US",
            "timezone": "America/New_York",
        }
    )
    return contexts


def _hand_event(
    raw: Mapping[str, Any],
    profile: AlertAcceptanceProfile,
) -> Any:
    safe, _ = separate_hand_labels(dict(raw))
    payload = HandCompletedPayload.model_validate(safe)
    return build_event(
        event_type=HAND_COMPLETED,
        aggregate_id=payload.hand_id,
        payload=payload,
        dataset_id=profile.dataset_id,
        dataset_split=profile.dataset_split,
        occurred_at=payload.played_at,
        emitted_at=payload.played_at + timedelta(seconds=1),
        tenant_id=profile.tenant_id,
        product_id=profile.product_id,
    )


def _directional_fold_win(raw: Mapping[str, Any], player_a: str, player_b: str) -> bool:
    players = {str(row["player_id"]): row for row in raw["players"]}
    a_folded = any(
        str(action["player_id"]) == player_a and action["action_type"] == "fold"
        for action in raw["actions"]
    )
    return a_folded and float(players[player_b]["won_amount"]) > 0


def _generate_hands_and_cases(
    profile: AlertAcceptanceProfile,
    start_at: datetime,
) -> tuple[list[Any], list[_Case], dict[str, UserContextPayload]]:
    generator = HandGenerator(
        GeneratorConfig(
            n_hands=0,
            n_players=profile.player_count,
            n_tables=profile.table_count,
            n_colluding_pairs=0,
            seed=profile.seed,
            dataset_id=profile.dataset_id,
            dataset_split=profile.dataset_split,
        ),
        start_at=start_at,
    )
    player_ids = [player.player_id for player in generator.players]
    rosters = [tuple(player_ids[index : index + 6]) for index in range(0, 30, 6)]
    target_pairs = [_canonical_pair(roster[0], roster[1]) for roster in rosters]
    contexts = _context_payloads(profile, player_ids, start_at)
    generated: dict[str, list[Any]] = {family: [] for family, *_ in _CASE_SPECS}
    candidate_index = 0

    repeated_pair = CollusionPair(
        pair_id=f"{profile.dataset_id}_repeated_fold_pair",
        player_a=target_pairs[0][0],
        player_b=target_pairs[0][1],
        pattern=CollusionPattern.FOLD_BENEFIT,
        intensity=1.0,
    )
    while (
        len(generated["repeated_fold_to_partner_wins"]) < profile.repeated_fold_hands
        and candidate_index < profile.candidate_hand_limit
    ):
        raw = generator.generate_hand(
            candidate_index,
            table_id=generator.tables[0],
            seat_player_ids=(
                target_pairs[0][0],
                target_pairs[0][1],
                *[value for value in rosters[0] if value not in target_pairs[0][:2]],
            ),
            played_at=start_at + timedelta(seconds=candidate_index * 30),
            planned_collusion_pair=repeated_pair,
            allow_random_collusion=False,
        )
        candidate_index += 1
        if _directional_fold_win(raw, target_pairs[0][0], target_pairs[0][1]):
            generated["repeated_fold_to_partner_wins"].append(_hand_event(raw, profile))
    if len(generated["repeated_fold_to_partner_wins"]) != profile.repeated_fold_hands:
        raise RuntimeError("could not generate the repeated-fold acceptance sequence")

    plans = (
        (
            "suspicious_same_device",
            1,
            profile.same_device_hands,
            CollusionPattern.SOFT_PLAY,
        ),
        (
            "suspicious_same_network",
            2,
            profile.same_network_hands,
            CollusionPattern.CHIP_DUMP,
        ),
        (
            "innocent_household",
            3,
            profile.innocent_household_hands,
            None,
        ),
        (
            "legitimate_multitabler",
            4,
            profile.multitabler_control_hands,
            None,
        ),
    )
    for family, roster_index, hand_count, pattern in plans:
        pair = target_pairs[roster_index]
        planned = (
            CollusionPair(
                pair_id=f"{profile.dataset_id}_{family}_pair",
                player_a=pair[0],
                player_b=pair[1],
                pattern=pattern,
                intensity=1.0,
            )
            if pattern is not None
            else None
        )
        roster = (
            pair[0],
            pair[1],
            *[value for value in rosters[roster_index] if value not in pair[:2]],
        )
        case_anchor = start_at + timedelta(
            seconds=(candidate_index + roster_index * 5) * 30
        )
        for ordinal in range(hand_count):
            raw = generator.generate_hand(
                candidate_index,
                table_id=generator.tables[
                    (
                        (roster_index + ordinal) % profile.table_count
                        if family == "legitimate_multitabler"
                        else roster_index
                    )
                ],
                seat_player_ids=roster,
                played_at=case_anchor
                + timedelta(
                    seconds=(
                        ordinal if family == "legitimate_multitabler" else ordinal * 30
                    )
                ),
                planned_collusion_pair=planned,
                allow_random_collusion=False,
            )
            generated[family].append(_hand_event(raw, profile))
            candidate_index += 1

    cases: list[_Case] = []
    for case_ordinal, (
        family,
        case_kind,
        is_collusive,
        relationship,
    ) in enumerate(_CASE_SPECS):
        pair = target_pairs[case_ordinal]
        hand_ids = tuple(str(event.payload["hand_id"]) for event in generated[family])
        case_id = str(
            _stable_uuid(
                profile.dataset_id,
                profile.dataset_split,
                "alert-acceptance-case",
                family,
            )
        )
        latest = max(event.occurred_at for event in generated[family])
        cases.append(
            _Case(
                case_id=case_id,
                case_kind=case_kind,
                scenario_family=family,
                members=(pair[0], pair[1]),
                pair_key=pair[2],
                hand_ids=hand_ids,
                is_collusive=is_collusive,
                required_context_relationship=relationship,
                label_available_at=latest + timedelta(days=7),
            )
        )
    hands = sorted(
        itertools.chain.from_iterable(generated.values()),
        key=lambda event: (event.occurred_at, str(event.event_id)),
    )
    if len(hands) != profile.output_hand_count:
        raise RuntimeError("alert-acceptance hand count changed")
    return hands, cases, contexts


def _context_events(
    profile: AlertAcceptanceProfile,
    contexts: Mapping[str, UserContextPayload],
) -> dict[str, Any]:
    return {
        player_id: build_event(
            event_type=USER_CONTEXT_UPDATED,
            aggregate_id=f"{player_id}:context:{payload.context_version}",
            payload=payload,
            dataset_id=profile.dataset_id,
            dataset_split=profile.dataset_split,
            occurred_at=payload.effective_at,
            tenant_id=profile.tenant_id,
            product_id=profile.product_id,
        )
        for player_id, payload in contexts.items()
    }


def _pair_features_and_evidence(
    hands: Iterable[Any],
    context_events: Mapping[str, Any],
) -> tuple[list[PairFeatureEvent], list[RuleEvidenceEvent]]:
    core = PairFeatureCore()
    stateful: dict[str, RepeatedFoldWindowRule] = {}
    feature_events: list[PairFeatureEvent] = []
    evidence_events: list[RuleEvidenceEvent] = []
    for hand in hands:
        enriched = [
            enrich_player_hand(
                hand,
                player_id=str(player["player_id"]),
                context_event=context_events[str(player["player_id"])],
                emitted_at=hand.emitted_at,
            )
            for player in sorted(
                hand.payload["players"],
                key=lambda value: str(value["player_id"]),
            )
        ]
        for event in core.process_many(enriched):
            stateful_rule = stateful.setdefault(
                event.payload.pair_key,
                RepeatedFoldWindowRule(),
            )
            stateful_result = stateful_rule.evaluate(
                StatefulPairObservation.from_event(event)
            )
            upstream: list[RuleEvidenceEvent] = []
            if stateful_result.evidence_event is not None:
                upstream.append(stateful_result.evidence_event)
            raw = event.model_dump(mode="json")
            raw["upstream_rule_evidence"] = [
                value.model_dump(mode="json") for value in upstream
            ]
            transport_event = PairFeatureEvent.model_validate(raw)
            feature_events.append(transport_event)
            evidence_events.extend(upstream)
            evidence_events.extend(
                evaluate_pair_rules(
                    transport_event,
                    emitted_at=transport_event.emitted_at,
                )
            )
    return feature_events, evidence_events


def _evidence_expectations(
    cases: Iterable[_Case],
    evidence_events: Iterable[RuleEvidenceEvent],
) -> list[dict[str, Any]]:
    case_by_family = {case.scenario_family: case for case in cases}
    events = list(evidence_events)

    def count(case: _Case, rule_id: str) -> tuple[int, list[str]]:
        matching = [
            event
            for event in events
            if event.payload.entity_key == case.pair_key
            and event.payload.hand_id in case.hand_ids
            and event.payload.rule_id == rule_id
        ]
        return len(matching), sorted({event.payload.hand_id for event in matching})

    requirements = (
        (
            "repeated_fold_to_partner_wins",
            "pair.one-folded-other-won",
            "must_fire",
            6,
            "precise-current-hand-fold-benefit",
        ),
        (
            "repeated_fold_to_partner_wins",
            "pair.same-device",
            "must_fire",
            6,
            "suspicious-pair-shares-device",
        ),
        (
            "repeated_fold_to_partner_wins",
            "pair.same-network",
            "must_fire",
            6,
            "suspicious-pair-shares-network",
        ),
        (
            "repeated_fold_to_partner_wins",
            "pair.repeated-fold-to-partner-wins",
            "must_fire",
            2,
            "five-hand-three-directional-sixty-percent-window",
        ),
        (
            "suspicious_same_device",
            "pair.same-device",
            "must_fire",
            3,
            "suspicious-pair-shares-device",
        ),
        (
            "suspicious_same_device",
            "pair.same-network",
            "must_fire",
            3,
            "same-device-implies-shared-network-in-fixture",
        ),
        (
            "suspicious_same_network",
            "pair.same-network",
            "must_fire",
            3,
            "suspicious-pair-shares-network",
        ),
        (
            "suspicious_same_network",
            "pair.same-device",
            "must_not_fire",
            0,
            "devices-are-distinct",
        ),
        (
            "innocent_household",
            "pair.same-device",
            "must_fire",
            2,
            "soft-evidence-with-negative-label",
        ),
        (
            "innocent_household",
            "pair.same-network",
            "must_fire",
            2,
            "soft-evidence-with-negative-label",
        ),
        (
            "legitimate_multitabler",
            "pair.same-device",
            "must_not_fire",
            0,
            "activity-alone-is-not-device-evidence",
        ),
        (
            "legitimate_multitabler",
            "pair.same-network",
            "must_not_fire",
            0,
            "activity-alone-is-not-network-evidence",
        ),
        (
            "legitimate_multitabler",
            "pair.repeated-fold-to-partner-wins",
            "must_not_fire",
            0,
            "two-hands-cannot-meet-five-hand-window",
        ),
    )
    output: list[dict[str, Any]] = []
    for family, rule_id, expectation, exact, reason in requirements:
        case = case_by_family[family]
        observed, hand_ids = count(case, rule_id)
        if observed != exact:
            raise RuntimeError(
                f"acceptance oracle changed for {family}/{rule_id}: "
                f"expected={exact} observed={observed}"
            )
        output.append(
            {
                "schema_version": ALERT_ACCEPTANCE_SCHEMA_VERSION,
                "case_id": case.case_id,
                "pair_key": case.pair_key,
                "rule_id": rule_id,
                "rule_version": 1,
                "expectation": expectation,
                "minimum_firings": exact,
                "maximum_firings": exact,
                "qualifying_hand_ids": hand_ids,
                "reason_code": reason,
            }
        )
    return output


def _score_expectations(
    *,
    profile: AlertAcceptanceProfile,
    feature_events: list[PairFeatureEvent],
    scorer: PairOnnxScorer,
    review_policy_path: Path,
) -> list[dict[str, Any]]:
    review_policy = load_review_policy(review_policy_path)
    frame = pd.DataFrame(
        [flatten_pair_feature(event, profile.dataset_split) for event in feature_events]
    )
    pair_scores = scorer.score_pairs(frame)
    score_by_pair = {
        (str(row.hand_id), str(row.pair_key)): row
        for row in pair_scores.itertuples(index=False)
    }
    events_by_hand: dict[str, list[PairFeatureEvent]] = {}
    for event in feature_events:
        events_by_hand.setdefault(event.payload.hand_id, []).append(event)

    alert_hands: list[str] = []
    resolved: list[tuple[str, list[PairFeatureEvent], Any]] = []
    for hand_id, events in sorted(
        events_by_hand.items(),
        key=lambda item: (
            item[1][0].payload.played_at,
            item[0],
        ),
    ):
        highest = max(
            (
                score_by_pair[(hand_id, event.payload.pair_key)]
                for event in sorted(
                    events,
                    key=lambda value: value.payload.pair_key,
                )
            ),
            key=lambda row: float(row.calibrated_probability),
        )
        if bool(highest.alert):
            alert_hands.append(hand_id)
        resolved.append((hand_id, events, highest))
    if len(alert_hands) < profile.minimum_model_alerts:
        raise RuntimeError(
            f"acceptance model alerts={len(alert_hands)}; "
            f"required={profile.minimum_model_alerts}"
        )
    selected = set(alert_hands[: profile.minimum_model_alerts])

    output: list[dict[str, Any]] = []
    for hand_id, events, highest in resolved:
        ordered = sorted(events, key=lambda event: event.payload.pair_key)
        rule_evidence: list[RuleEvidenceEvent] = []
        for event in ordered:
            rule_evidence.extend(
                RuleEvidenceEvent.model_validate(raw)
                for raw in event.upstream_rule_evidence
            )
            rule_evidence.extend(
                evaluate_pair_rules(event, emitted_at=event.emitted_at)
            )
        evidence_ids = [str(event.event_id) for event in rule_evidence]
        score_id = _score_id(scorer.contract["run_id"], ordered)
        risk_score_event_id = _go_stable_uuid(
            RISK_SCORE_COMPUTED,
            score_id,
        )
        decision_id = stable_review_decision_id(
            tenant_id=profile.tenant_id,
            product_id=profile.product_id,
            dataset_id=profile.dataset_id,
            dataset_split=profile.dataset_split,
            policy_id=review_policy.policy_id,
            policy_version=review_policy.policy_version,
            risk_score_event_id=risk_score_event_id,
        )
        expected_alert = bool(highest.alert)
        alert_id = (
            _go_stable_uuid(
                RISK_ALERT_CREATED,
                score_id,
                str(decision_id),
            )
            if expected_alert
            else None
        )
        output.append(
            {
                "schema_version": ALERT_ACCEPTANCE_SCHEMA_VERSION,
                "dataset_id": profile.dataset_id,
                "hand_id": hand_id,
                "model_name": scorer.contract["model_name"],
                "model_run_id": scorer.contract["run_id"],
                "decision_threshold": float(scorer.policy["threshold"]),
                "hand_risk_probability": float(highest.calibrated_probability),
                "probability_tolerance": 1e-6,
                "highest_risk_pair": str(highest.pair_key),
                "expected_alert": expected_alert,
                "selected_demo_alert": hand_id in selected,
                "score_id": score_id,
                "risk_score_event_id": str(risk_score_event_id),
                "review_decision_event_id": str(decision_id),
                "risk_alert_event_id": (
                    str(alert_id) if alert_id is not None else None
                ),
                "review_policy_id": review_policy.policy_id,
                "review_policy_version": review_policy.policy_version,
                "expected_policy_outcome": (
                    "review_recommended" if expected_alert else "no_review"
                ),
                "expected_rule_evidence_event_ids": evidence_ids,
                "expected_sink_rows": {
                    "risk_scores": 1,
                    "review_decisions": 1,
                    "risk_alerts": int(expected_alert),
                    "rule_evidence": len(evidence_ids),
                },
                "expected_admin_row_id": (
                    str(alert_id) if alert_id is not None else None
                ),
                "expected_admin_visible": expected_alert,
            }
        )
    return output


def _benchmark_hand_ids(root: Path) -> tuple[set[str], str]:
    manifest_path = root.resolve() / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"missing D5 benchmark manifest: {manifest_path}")
    assignment_paths = sorted(root.glob("**/hand_assignments.jsonl"))
    if not assignment_paths:
        raise ValueError("D5 benchmark contains no hand assignments")
    hand_ids = {
        str(row["hand_id"]) for path in assignment_paths for row in _iter_jsonl(path)
    }
    return hand_ids, _sha256(manifest_path)


def _write_jsonl(path: Path, values: Iterable[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as stream:
        for value in values:
            stream.write(_json_line(value))


def build_alert_acceptance_pack(
    config: AlertAcceptanceBuildConfig,
) -> dict[str, Any]:
    output_dir = config.output_dir.resolve()
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"alert-acceptance output is not empty: {output_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)
    start_at = config.start_at.astimezone(timezone.utc)
    profile = config.profile

    scorer = PairOnnxScorer(config.model_dir)
    hands, cases, contexts = _generate_hands_and_cases(profile, start_at)
    context_events = _context_events(profile, contexts)
    feature_events, evidence_events = _pair_features_and_evidence(
        hands,
        context_events,
    )
    evidence_expectations = _evidence_expectations(cases, evidence_events)
    score_expectations = _score_expectations(
        profile=profile,
        feature_events=feature_events,
        scorer=scorer,
        review_policy_path=config.review_policy_path,
    )

    acceptance_hand_ids = {str(hand.payload["hand_id"]) for hand in hands}
    benchmark_hand_ids, benchmark_manifest_sha256 = _benchmark_hand_ids(
        config.benchmark_dir
    )
    overlap = acceptance_hand_ids & benchmark_hand_ids
    if overlap:
        raise RuntimeError(
            f"alert-acceptance hand leaked into D5 benchmark: {min(overlap)}"
        )

    labels: list[PairHandLabel] = []
    for case in cases:
        player_a, player_b, _ = _canonical_pair(*case.members)
        for hand_id in case.hand_ids:
            labels.append(
                PairHandLabel(
                    example_id=_stable_uuid(
                        profile.dataset_id,
                        profile.dataset_split,
                        "acceptance-pair-label",
                        hand_id,
                        case.pair_key,
                    ),
                    dataset_id=profile.dataset_id,
                    dataset_split=profile.dataset_split,
                    hand_id=hand_id,
                    pair_key=case.pair_key,
                    player_a=player_a,
                    player_b=player_b,
                    is_collusive=case.is_collusive,
                    collusion_pair_id=(case.case_id if case.is_collusive else None),
                    label_available_at=case.label_available_at,
                )
            )

    paths = {
        "config": output_dir / "config.json",
        "hands": output_dir / "events" / "hands.jsonl",
        "contexts": output_dir / "snapshots" / "users.jsonl",
        "pair_features": output_dir / "expected" / "pair_features.jsonl",
        "cases": output_dir / "private_labels" / "cases.jsonl",
        "pair_labels": output_dir / "private_labels" / "pair_labels.jsonl",
        "rule_evidence": output_dir / "private_oracle" / "rule_evidence_events.jsonl",
        "evidence_expectations": output_dir
        / "private_oracle"
        / "evidence_expectations.jsonl",
        "score_expectations": output_dir
        / "private_oracle"
        / "score_expectations.jsonl",
    }
    paths["config"].parent.mkdir(parents=True, exist_ok=True)
    paths["config"].write_text(
        json.dumps(profile.to_dict(), indent=2, sort_keys=True) + "\n"
    )
    _write_jsonl(paths["hands"], hands)
    _write_jsonl(
        paths["contexts"],
        (contexts[player_id] for player_id in sorted(contexts)),
    )
    _write_jsonl(paths["pair_features"], feature_events)
    _write_jsonl(
        paths["cases"],
        (case.to_dict(profile) for case in cases),
    )
    _write_jsonl(paths["pair_labels"], labels)
    _write_jsonl(paths["rule_evidence"], evidence_events)
    _write_jsonl(paths["evidence_expectations"], evidence_expectations)
    _write_jsonl(paths["score_expectations"], score_expectations)

    model_dir = config.model_dir.resolve()
    bindings = {
        "model_artifact_manifest_sha256": _sha256(model_dir / "artifact_manifest.json"),
        "scoring_contract_sha256": _sha256(model_dir / "scoring_contract.json"),
        "decision_policy_sha256": _sha256(model_dir / "decision_policy.json"),
        "review_policy_sha256": _sha256(config.review_policy_path.resolve()),
        "pair_rules_sha256": _sha256(
            Path("schemas/rules/pair-rules-v1.json").resolve()
        ),
        "stateful_pair_rules_sha256": _sha256(
            Path("schemas/rules/stateful-pair-rules-v1.json").resolve()
        ),
        "benchmark_manifest_sha256": benchmark_manifest_sha256,
    }
    binding_path = output_dir / "bindings.json"
    binding_path.write_text(json.dumps(bindings, indent=2, sort_keys=True) + "\n")
    paths["bindings"] = binding_path
    artifacts = {
        str(path.relative_to(output_dir)): _sha256(path) for path in paths.values()
    }
    alert_count = sum(bool(row["expected_alert"]) for row in score_expectations)
    selected_count = sum(bool(row["selected_demo_alert"]) for row in score_expectations)
    manifest = {
        "schema_version": ALERT_ACCEPTANCE_SCHEMA_VERSION,
        "product_type": "alert_acceptance",
        "dataset_id": profile.dataset_id,
        "dataset_split": profile.dataset_split,
        "profile_id": profile.profile_id,
        "training_allowed": False,
        "allowed_uses": ["end_to_end_acceptance", "demo_replay"],
        "forbidden_uses": [
            "model_training",
            "model_validation",
            "model_testing",
            "calibration",
            "threshold_selection",
            "model_promotion",
        ],
        "private_oracle_after_scoring_only": True,
        "benchmark_hand_overlap": 0,
        "counts": {
            "hands": len(hands),
            "players": len(contexts),
            "pair_features": len(feature_events),
            "cases": len(cases),
            "target_pair_labels": len(labels),
            "rule_evidence_events": len(evidence_events),
            "evidence_expectations": len(evidence_expectations),
            "score_expectations": len(score_expectations),
            "expected_model_alerts": alert_count,
            "selected_demo_alerts": selected_count,
        },
        "bindings": bindings,
        "runtime_variant_fields": [
            "emitted_at",
            "scored_at",
            "service_build_version",
        ],
        "replay_status": {
            "offline_python_feature_oracle": "passed",
            "offline_onnx_score_oracle": "passed",
            "java_flink": "not_run",
            "go_risk": "not_run",
            "snowflake_sinks": "not_run",
            "admin": "not_run",
        },
        "artifacts": dict(sorted(artifacts.items())),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def verify_alert_acceptance_pack(
    root: Path,
    *,
    model_dir: Path,
    benchmark_dir: Path,
    review_policy_path: Path = Path("schemas/policies/review-policy-v1.json"),
) -> dict[str, Any]:
    root = root.resolve()
    manifest = json.loads((root / "manifest.json").read_text())
    if (
        manifest.get("product_type") != "alert_acceptance"
        or manifest.get("training_allowed") is not False
    ):
        raise ValueError("dataset is not a sealed alert-acceptance product")
    for relative, expected in manifest["artifacts"].items():
        path = root / relative
        if not path.is_file() or _sha256(path) != expected:
            raise ValueError(f"alert-acceptance artifact hash mismatch: {relative}")

    current_bindings = {
        "model_artifact_manifest_sha256": _sha256(
            model_dir.resolve() / "artifact_manifest.json"
        ),
        "scoring_contract_sha256": _sha256(
            model_dir.resolve() / "scoring_contract.json"
        ),
        "decision_policy_sha256": _sha256(model_dir.resolve() / "decision_policy.json"),
        "review_policy_sha256": _sha256(review_policy_path.resolve()),
        "pair_rules_sha256": _sha256(
            Path("schemas/rules/pair-rules-v1.json").resolve()
        ),
        "stateful_pair_rules_sha256": _sha256(
            Path("schemas/rules/stateful-pair-rules-v1.json").resolve()
        ),
    }
    stored_bindings = json.loads((root / "bindings.json").read_text())
    if manifest.get("bindings") != stored_bindings:
        raise ValueError("manifest and binding artifact disagree")
    for key, value in current_bindings.items():
        if stored_bindings.get(key) != value:
            raise ValueError(f"alert-acceptance binding changed: {key}")
    benchmark_ids, benchmark_sha256 = _benchmark_hand_ids(benchmark_dir)
    if stored_bindings["benchmark_manifest_sha256"] != benchmark_sha256:
        raise ValueError("D5 benchmark binding changed")

    hands = [
        validate_event(row) for row in _iter_jsonl(root / "events" / "hands.jsonl")
    ]
    for hand in hands:
        assert_inference_safe(hand.model_dump(mode="json"))
        if int(hand.payload["num_players"]) != 6:
            raise ValueError("Go acceptance replay requires six-player hands")
    acceptance_ids = {str(hand.payload["hand_id"]) for hand in hands}
    if acceptance_ids & benchmark_ids:
        raise ValueError("alert-acceptance IDs appear in a D5 benchmark")

    features = [
        PairFeatureEvent.model_validate(row)
        for row in _iter_jsonl(root / "expected" / "pair_features.jsonl")
    ]
    for feature in features:
        assert_inference_safe(feature.model_dump(mode="json"))
    feature_counts = Counter(event.payload.hand_id for event in features)
    if set(feature_counts) != acceptance_ids or set(feature_counts.values()) != {15}:
        raise ValueError("alert-acceptance features are not complete 15-pair hands")

    profile = AlertAcceptanceProfile.from_json(root / "config.json")
    recomputed_scores = _score_expectations(
        profile=profile,
        feature_events=features,
        scorer=PairOnnxScorer(model_dir),
        review_policy_path=review_policy_path,
    )
    stored_scores = list(
        _iter_jsonl(root / "private_oracle" / "score_expectations.jsonl")
    )
    if recomputed_scores != stored_scores:
        raise ValueError("alert-acceptance score oracle changed")
    cases = list(_iter_jsonl(root / "private_labels" / "cases.jsonl"))
    rule_events = [
        RuleEvidenceEvent.model_validate(row)
        for row in _iter_jsonl(root / "private_oracle" / "rule_evidence_events.jsonl")
    ]
    expected_rule_ids = {
        event_id
        for score in stored_scores
        for event_id in score["expected_rule_evidence_event_ids"]
    }
    stored_rule_ids = {str(event.event_id) for event in rule_events}
    if expected_rule_ids != stored_rule_ids:
        raise ValueError("score and rule-evidence oracles disagree")
    reconstructed_cases = [
        _Case(
            case_id=str(case["case_id"]),
            case_kind=str(case["case_kind"]),
            scenario_family=str(case["scenario_family"]),
            members=tuple(case["members"]),
            pair_key=str(case["pair_key"]),
            hand_ids=tuple(case["hand_ids"]),
            is_collusive=bool(case["is_collusive"]),
            required_context_relationship=str(case["required_context_relationship"]),
            label_available_at=datetime.fromisoformat(
                str(case["label_available_at"]).replace("Z", "+00:00")
            ),
        )
        for case in cases
    ]
    recomputed_evidence = _evidence_expectations(
        reconstructed_cases,
        rule_events,
    )
    stored_evidence = list(
        _iter_jsonl(root / "private_oracle" / "evidence_expectations.jsonl")
    )
    if recomputed_evidence != stored_evidence:
        raise ValueError("alert-acceptance evidence oracle changed")

    expected_counts = {
        "hands": len(hands),
        "players": sum(1 for _ in _iter_jsonl(root / "snapshots" / "users.jsonl")),
        "pair_features": len(features),
        "cases": len(cases),
        "target_pair_labels": sum(
            1 for _ in _iter_jsonl(root / "private_labels" / "pair_labels.jsonl")
        ),
        "rule_evidence_events": len(rule_events),
        "evidence_expectations": len(stored_evidence),
        "score_expectations": len(stored_scores),
        "expected_model_alerts": sum(
            bool(row["expected_alert"]) for row in stored_scores
        ),
        "selected_demo_alerts": sum(
            bool(row["selected_demo_alert"]) for row in stored_scores
        ),
    }
    if manifest["counts"] != expected_counts:
        raise ValueError("alert-acceptance manifest counts changed")
    return {
        "status": "passed",
        **expected_counts,
        "benchmark_hand_overlap": 0,
        "training_allowed": False,
    }
