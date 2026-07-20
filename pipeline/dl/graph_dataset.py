"""Prior-only heterogeneous temporal graph exports for Phase 11."""

from __future__ import annotations

import hashlib
import json
import math
import shutil
from collections import defaultdict, deque
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from itertools import groupby
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .history_dataset import (
    event_alignment_sha256,
    sha256_file,
    write_deterministic_npz,
)


GRAPH_SCHEMA_VERSION = 1
GRAPH_BENCHMARKS = ("cold_start", "new_relationship")
GRAPH_SPLITS = ("train", "validation", "test")
RESOURCE_TYPES = ("device", "network", "session", "table", "account_link")
ROOT_USER_FEATURES = (
    "context_missing",
    "skill_rating",
    "log_account_age_days",
    "bankroll_ordinal",
    "preferred_stake_ordinal",
    "kyc_ordinal",
    "account_active",
    "country_hash_fraction",
    "timezone_hash_fraction",
    "log_hands_seen",
    "fold_rate",
    "raise_rate",
    "saw_flop_rate",
    "log_unique_coplayer_degree",
)
USER_EDGE_FEATURES = ("log_hands_together", "log_minutes_since_last_hand")
RESOURCE_NODE_FEATURES = (
    "log_user_degree",
    "log_event_count",
    "log_minutes_since_edge",
    "shared_with_other_endpoint",
    "relation_strength",
)
PAIR_GRAPH_FEATURES = (
    "log_hands_together",
    "log_minutes_since_last_hand",
    "coplayer_neighbor_jaccard",
    "same_device",
    "same_network",
    "same_session",
    "log_common_prior_tables",
    "direct_account_link",
    "log_shared_resource_degree",
)


@dataclass(frozen=True)
class GraphDatasetConfig:
    source_dir: Path = Path("data/datasets/context-full-v2")
    pair_dataset_dir: Path = Path("data/datasets/pair-full-v2")
    output_dir: Path = Path("data/datasets/pair-graph-full-v2")
    benchmarks: tuple[str, ...] = GRAPH_BENCHMARKS
    max_user_neighbors: int = 8
    max_resource_neighbors: int = 4
    overwrite: bool = False

    def __post_init__(self) -> None:
        if not self.benchmarks or any(value not in GRAPH_BENCHMARKS for value in self.benchmarks):
            raise ValueError(f"graph benchmarks must be selected from {GRAPH_BENCHMARKS}")
        if len(set(self.benchmarks)) != len(self.benchmarks):
            raise ValueError("graph benchmarks must be unique")
        if self.max_user_neighbors < 1 or self.max_resource_neighbors < 1:
            raise ValueError("graph neighbor limits must be positive")


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open() as stream:
        for line in stream:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def _timestamp_ns(value: object) -> int:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")
    return int(timestamp.value)


def _pair_players(pair_key: object) -> tuple[str, str]:
    values = str(pair_key).split(":")
    if len(values) != 2 or not values[0] or not values[1] or values[0] >= values[1]:
        raise ValueError(f"invalid canonical pair key: {pair_key!r}")
    return values[0], values[1]


def _pair_key(left: str, right: str) -> str:
    return f"{left}:{right}" if left < right else f"{right}:{left}"


def _hash_fraction(value: object) -> float:
    digest = hashlib.sha256(str(value or "missing").encode("utf-8")).digest()
    return int.from_bytes(digest[:4], "big") / float(2**32 - 1)


def _log_minutes(current_ns: int, previous_ns: int | None) -> float:
    if previous_ns is None:
        return 0.0
    if previous_ns >= current_ns:
        raise ValueError("graph edge timestamp must be strictly earlier than the example")
    return math.log1p((current_ns - previous_ns) / 60_000_000_000)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


class TemporalGraphState:
    def __init__(self, max_recent_tables: int = 16) -> None:
        self.context: dict[str, tuple[int, dict[str, Any]]] = {}
        self.sessions: dict[str, tuple[int, dict[str, Any]]] = {}
        self.resource_users: dict[str, dict[str, set[str]]] = {
            "device": defaultdict(set),
            "network": defaultdict(set),
            "session": defaultdict(set),
        }
        self.resource_events: dict[str, dict[str, int]] = {
            "device": defaultdict(int),
            "network": defaultdict(int),
            "session": defaultdict(int),
        }
        self.resource_last_ns: dict[str, dict[str, int]] = {
            "device": {},
            "network": {},
            "session": {},
        }
        self.account_links: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
        self.user_stats: dict[str, dict[str, float]] = defaultdict(
            lambda: {
                "hands": 0.0,
                "actions": 0.0,
                "folds": 0.0,
                "raises": 0.0,
                "saw_flop": 0.0,
            }
        )
        self.coplayers: dict[str, dict[str, tuple[int, int]]] = defaultdict(dict)
        self.pair_state: dict[str, tuple[int, int]] = {}
        self.user_tables: dict[str, deque[tuple[int, str]]] = defaultdict(
            lambda: deque(maxlen=max_recent_tables)
        )
        self.table_users: dict[str, set[str]] = defaultdict(set)
        self.table_events: dict[str, int] = defaultdict(int)
        self.table_last_ns: dict[str, int] = {}

    def apply_context(self, timestamp_ns: int, payload: Mapping[str, Any]) -> None:
        user_id = str(payload["user_id"])
        previous = self.context.get(user_id)
        if previous is not None:
            old = previous[1]
            self.resource_users["device"][str(old["device_id"])].discard(user_id)
            self.resource_users["network"][str(old["network_cluster_id"])].discard(user_id)
        value = dict(payload)
        self.context[user_id] = (timestamp_ns, value)
        for relation, field in (("device", "device_id"), ("network", "network_cluster_id")):
            resource_id = str(value[field])
            self.resource_users[relation][resource_id].add(user_id)
            self.resource_events[relation][resource_id] += 1
            self.resource_last_ns[relation][resource_id] = timestamp_ns

    def apply_session(self, timestamp_ns: int, payload: Mapping[str, Any]) -> None:
        user_id = str(payload["user_id"])
        previous = self.sessions.get(user_id)
        if previous is not None:
            self.resource_users["session"][str(previous[1]["session_id"])].discard(user_id)
        value = dict(payload)
        self.sessions[user_id] = (timestamp_ns, value)
        session_id = str(value["session_id"])
        self.resource_users["session"][session_id].add(user_id)
        self.resource_events["session"][session_id] += 1
        self.resource_last_ns["session"][session_id] = timestamp_ns

    def apply_account_link(self, timestamp_ns: int, payload: Mapping[str, Any]) -> None:
        value = dict(payload)
        left = str(value["user_id"])
        right = str(value["related_user_id"])
        self.account_links[left].append((timestamp_ns, {**value, "other_user_id": right}))
        self.account_links[right].append((timestamp_ns, {**value, "other_user_id": left}))

    def user_features(self, user_id: str, current_ns: int) -> np.ndarray:
        context_entry = self.context.get(user_id)
        if context_entry is None:
            context_values = [1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
        else:
            _, context = context_entry
            created_ns = _timestamp_ns(context["account_created_at"])
            account_days = max((current_ns - created_ns) / 86_400_000_000_000, 0.0)
            context_values = [
                0.0,
                float(context.get("skill_rating", 0.0) or 0.0),
                math.log1p(account_days),
                {"low": 0.0, "medium": 0.5, "high": 1.0}.get(
                    str(context.get("bankroll_bucket", "")).lower(), 0.0
                ),
                {"micro": 0.0, "low": 0.33, "medium": 0.66, "high": 1.0}.get(
                    str(context.get("preferred_stake_bucket", "")).lower(), 0.0
                ),
                {"none": 0.0, "pending": 0.33, "basic": 0.66, "full": 1.0}.get(
                    str(context.get("kyc_level", "")).lower(), 0.0
                ),
                float(str(context.get("account_status", "")).lower() == "active"),
                _hash_fraction(context.get("country_bucket")),
                _hash_fraction(context.get("timezone")),
            ]
        stats = self.user_stats[user_id]
        actions = max(stats["actions"], 1.0)
        hands = max(stats["hands"], 1.0)
        dynamic = [
            math.log1p(stats["hands"]),
            stats["folds"] / actions,
            stats["raises"] / actions,
            stats["saw_flop"] / hands,
            math.log1p(len(self.coplayers[user_id])),
        ]
        output = np.asarray(context_values + dynamic, dtype=np.float32)
        if len(output) != len(ROOT_USER_FEATURES) or not np.isfinite(output).all():
            raise ValueError(f"invalid graph user features for {user_id}")
        return output

    def _resource_feature(
        self,
        relation: str,
        resource_id: str,
        user_id: str,
        other_user_id: str,
        current_ns: int,
        *,
        strength: float = 1.0,
        event_ns: int | None = None,
    ) -> np.ndarray:
        if relation == "table":
            users = self.table_users[resource_id]
            events = self.table_events[resource_id]
            last_ns = self.table_last_ns.get(resource_id)
        else:
            users = self.resource_users[relation][resource_id]
            events = self.resource_events[relation][resource_id]
            last_ns = self.resource_last_ns[relation].get(resource_id)
        edge_ns = event_ns if event_ns is not None else last_ns
        return np.asarray(
            [
                math.log1p(len(users)),
                math.log1p(events),
                _log_minutes(current_ns, edge_ns),
                float(other_user_id in users),
                strength,
            ],
            dtype=np.float32,
        )

    def endpoint_graph(
        self,
        user_id: str,
        other_user_id: str,
        current_ns: int,
        *,
        max_user_neighbors: int,
        max_resource_neighbors: int,
    ) -> tuple[
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        np.ndarray,
        int,
    ]:
        root = self.user_features(user_id, current_ns)
        user_neighbors = np.zeros(
            (max_user_neighbors, len(ROOT_USER_FEATURES)), dtype=np.float16
        )
        user_edges = np.zeros(
            (max_user_neighbors, len(USER_EDGE_FEATURES)), dtype=np.float16
        )
        user_mask = np.zeros(max_user_neighbors, dtype=np.uint8)
        latest_edge = -1
        ordered_neighbors = sorted(
            self.coplayers[user_id].items(), key=lambda value: (value[1][1], value[0]), reverse=True
        )[:max_user_neighbors]
        for index, (neighbor_id, (count, last_ns)) in enumerate(ordered_neighbors):
            if last_ns >= current_ns:
                raise ValueError("co-player edge is not strictly prior")
            user_neighbors[index] = self.user_features(neighbor_id, current_ns)
            user_edges[index] = np.asarray(
                [math.log1p(count), _log_minutes(current_ns, last_ns)], dtype=np.float16
            )
            user_mask[index] = 1
            latest_edge = max(latest_edge, last_ns)

        resources = np.zeros(
            (
                len(RESOURCE_TYPES),
                max_resource_neighbors,
                len(RESOURCE_NODE_FEATURES),
            ),
            dtype=np.float16,
        )
        resource_mask = np.zeros(
            (len(RESOURCE_TYPES), max_resource_neighbors), dtype=np.uint8
        )
        context_entry = self.context.get(user_id)
        if context_entry is not None:
            context_ns, context = context_entry
            for relation_index, (relation, field) in enumerate(
                (("device", "device_id"), ("network", "network_cluster_id"))
            ):
                resource_id = str(context[field])
                resources[relation_index, 0] = self._resource_feature(
                    relation, resource_id, user_id, other_user_id, current_ns, event_ns=context_ns
                )
                resource_mask[relation_index, 0] = 1
                latest_edge = max(latest_edge, context_ns)
        session_entry = self.sessions.get(user_id)
        if session_entry is not None:
            session_ns, session = session_entry
            session_id = str(session["session_id"])
            resources[2, 0] = self._resource_feature(
                "session", session_id, user_id, other_user_id, current_ns, event_ns=session_ns
            )
            resource_mask[2, 0] = 1
            latest_edge = max(latest_edge, session_ns)
        seen_tables: set[str] = set()
        table_values: list[tuple[int, str]] = []
        for timestamp_ns, table_id in reversed(self.user_tables[user_id]):
            if table_id not in seen_tables:
                seen_tables.add(table_id)
                table_values.append((timestamp_ns, table_id))
            if len(table_values) >= max_resource_neighbors:
                break
        for index, (timestamp_ns, table_id) in enumerate(table_values):
            resources[3, index] = self._resource_feature(
                "table", table_id, user_id, other_user_id, current_ns, event_ns=timestamp_ns
            )
            resource_mask[3, index] = 1
            latest_edge = max(latest_edge, timestamp_ns)
        confidence = {"low": 0.33, "medium": 0.66, "high": 1.0}
        links = sorted(self.account_links[user_id], key=lambda value: value[0], reverse=True)[
            :max_resource_neighbors
        ]
        for index, (timestamp_ns, link) in enumerate(links):
            if timestamp_ns >= current_ns:
                raise ValueError("account-link edge is not strictly prior")
            resources[4, index] = np.asarray(
                [
                    math.log1p(2),
                    math.log1p(1),
                    _log_minutes(current_ns, timestamp_ns),
                    float(str(link["other_user_id"]) == other_user_id),
                    confidence.get(str(link.get("confidence_bucket", "")).lower(), 0.0),
                ],
                dtype=np.float16,
            )
            resource_mask[4, index] = 1
            latest_edge = max(latest_edge, timestamp_ns)
        return root, user_neighbors, user_edges, user_mask, resources, resource_mask, latest_edge

    def pair_features(self, left: str, right: str, current_ns: int) -> np.ndarray:
        state = self.pair_state.get(_pair_key(left, right))
        count, last_ns = state if state is not None else (0, None)
        left_neighbors = set(self.coplayers[left]) - {right}
        right_neighbors = set(self.coplayers[right]) - {left}
        union = left_neighbors | right_neighbors
        jaccard = len(left_neighbors & right_neighbors) / max(len(union), 1)
        left_context = self.context.get(left, (0, {}))[1]
        right_context = self.context.get(right, (0, {}))[1]
        left_session = self.sessions.get(left, (0, {}))[1]
        right_session = self.sessions.get(right, (0, {}))[1]
        same_device = bool(left_context) and left_context.get("device_id") == right_context.get("device_id")
        same_network = bool(left_context) and left_context.get("network_cluster_id") == right_context.get("network_cluster_id")
        same_session = bool(left_session) and left_session.get("session_id") == right_session.get("session_id")
        left_tables = {table_id for _, table_id in self.user_tables[left]}
        right_tables = {table_id for _, table_id in self.user_tables[right]}
        direct_link = any(
            str(link["other_user_id"]) == right for _, link in self.account_links[left]
        )
        shared_degree = 0
        if same_device:
            shared_degree += len(self.resource_users["device"][str(left_context["device_id"])])
        if same_network:
            shared_degree += len(
                self.resource_users["network"][str(left_context["network_cluster_id"])]
            )
        output = np.asarray(
            [
                math.log1p(count),
                _log_minutes(current_ns, last_ns),
                jaccard,
                float(same_device),
                float(same_network),
                float(same_session),
                math.log1p(len(left_tables & right_tables)),
                float(direct_link),
                math.log1p(shared_degree),
            ],
            dtype=np.float32,
        )
        return output

    def update_hand(self, timestamp_ns: int, payload: Mapping[str, Any]) -> None:
        players = [str(player["player_id"]) for player in payload["players"]]
        table_id = str(payload["table_id"])
        action_by_user: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for action in payload["actions"]:
            action_by_user[str(action["player_id"])].append(action)
        for user_id in players:
            stats = self.user_stats[user_id]
            actions = action_by_user[user_id]
            streets = {str(action["street"]).lower() for action in actions}
            stats["hands"] += 1
            stats["actions"] += len(actions)
            stats["folds"] += sum(str(action["action_type"]).lower() == "fold" for action in actions)
            stats["raises"] += sum(str(action["action_type"]).lower() == "raise" for action in actions)
            stats["saw_flop"] += float("flop" in streets)
            self.user_tables[user_id].append((timestamp_ns, table_id))
            self.table_users[table_id].add(user_id)
        self.table_events[table_id] += 1
        self.table_last_ns[table_id] = timestamp_ns
        for left_index, left in enumerate(sorted(players)):
            for right in sorted(players)[left_index + 1 :]:
                key = _pair_key(left, right)
                count = self.pair_state.get(key, (0, 0))[0] + 1
                self.pair_state[key] = (count, timestamp_ns)
                self.coplayers[left][right] = (count, timestamp_ns)
                self.coplayers[right][left] = (count, timestamp_ns)


def _empty_split_arrays(
    rows: int, max_user_neighbors: int, max_resource_neighbors: int
) -> dict[str, np.ndarray]:
    return {
        "event_ids": np.empty(rows, dtype="<U36"),
        "labels": np.empty(rows, dtype=np.int8),
        "example_played_ns": np.empty(rows, dtype=np.int64),
        "graph_last_edge_ns": np.full(rows, -1, dtype=np.int64),
        "root_features": np.zeros((rows, 2, len(ROOT_USER_FEATURES)), dtype=np.float16),
        "user_neighbor_features": np.zeros(
            (rows, 2, max_user_neighbors, len(ROOT_USER_FEATURES)), dtype=np.float16
        ),
        "user_edge_features": np.zeros(
            (rows, 2, max_user_neighbors, len(USER_EDGE_FEATURES)), dtype=np.float16
        ),
        "user_neighbor_masks": np.zeros((rows, 2, max_user_neighbors), dtype=np.uint8),
        "resource_features": np.zeros(
            (
                rows,
                2,
                len(RESOURCE_TYPES),
                max_resource_neighbors,
                len(RESOURCE_NODE_FEATURES),
            ),
            dtype=np.float16,
        ),
        "resource_masks": np.zeros(
            (rows, 2, len(RESOURCE_TYPES), max_resource_neighbors), dtype=np.uint8
        ),
        "pair_graph_features": np.zeros((rows, len(PAIR_GRAPH_FEATURES)), dtype=np.float16),
    }


def build_source_graph_arrays(
    frames: Mapping[str, pd.DataFrame],
    hand_events: Sequence[Mapping[str, Any]],
    context_events: Sequence[Mapping[str, Any]],
    session_events: Sequence[Mapping[str, Any]],
    account_link_events: Sequence[Mapping[str, Any]],
    *,
    max_user_neighbors: int,
    max_resource_neighbors: int,
) -> tuple[dict[str, dict[str, np.ndarray]], dict[str, dict[str, Any]]]:
    prepared = {split: frame.reset_index(drop=True).copy() for split, frame in frames.items()}
    arrays = {
        split: _empty_split_arrays(len(frame), max_user_neighbors, max_resource_neighbors)
        for split, frame in prepared.items()
    }
    targets_by_hand: dict[str, tuple[str, np.ndarray]] = {}
    for split, frame in prepared.items():
        for hand_id, indices in frame.groupby("hand_id", sort=False).indices.items():
            key = str(hand_id)
            if key in targets_by_hand:
                raise ValueError(f"hand {key} appears in more than one benchmark split")
            targets_by_hand[key] = (split, np.asarray(indices, dtype=np.int64))
    hand_records = []
    for event in hand_events:
        payload = event.get("payload", event)
        hand_records.append((_timestamp_ns(payload["played_at"]), str(payload["hand_id"]), payload))
    hand_records.sort(key=lambda value: (value[0], value[1]))
    if not set(targets_by_hand).issubset({record[1] for record in hand_records}):
        raise ValueError("graph target hands are missing from the source stream")

    side_events: list[tuple[int, str, Mapping[str, Any]]] = []
    for kind, events, time_field in (
        ("context", context_events, "effective_at"),
        ("session", session_events, "started_at"),
        ("account", account_link_events, "effective_at"),
    ):
        for event in events:
            payload = event.get("payload", event)
            side_events.append((_timestamp_ns(payload[time_field]), kind, payload))
    side_events.sort(key=lambda value: (value[0], value[1]))
    side_index = 0
    state = TemporalGraphState()
    processed_targets: set[str] = set()
    for played_ns, timestamp_group in groupby(hand_records, key=lambda value: value[0]):
        while side_index < len(side_events) and side_events[side_index][0] < played_ns:
            timestamp_ns, kind, payload = side_events[side_index]
            if kind == "context":
                state.apply_context(timestamp_ns, payload)
            elif kind == "session":
                state.apply_session(timestamp_ns, payload)
            else:
                state.apply_account_link(timestamp_ns, payload)
            side_index += 1
        group_records = list(timestamp_group)
        for _, hand_id, payload in group_records:
            target = targets_by_hand.get(hand_id)
            if target is None:
                continue
            split, indices = target
            frame = prepared[split]
            output = arrays[split]
            if len(indices) != 15:
                raise ValueError(f"expected 15 graph targets for {hand_id}, found {len(indices)}")
            players = {str(player["player_id"]) for player in payload["players"]}
            for row_index in indices:
                row = frame.iloc[int(row_index)]
                left, right = _pair_players(row["pair_key"])
                if left not in players or right not in players:
                    raise ValueError(f"graph pair is not present in {hand_id}")
                left_values = state.endpoint_graph(
                    left,
                    right,
                    played_ns,
                    max_user_neighbors=max_user_neighbors,
                    max_resource_neighbors=max_resource_neighbors,
                )
                right_values = state.endpoint_graph(
                    right,
                    left,
                    played_ns,
                    max_user_neighbors=max_user_neighbors,
                    max_resource_neighbors=max_resource_neighbors,
                )
                for endpoint, values in enumerate((left_values, right_values)):
                    root, neighbors, edges, neighbor_mask, resources, resource_mask, latest = values
                    output["root_features"][row_index, endpoint] = root
                    output["user_neighbor_features"][row_index, endpoint] = neighbors
                    output["user_edge_features"][row_index, endpoint] = edges
                    output["user_neighbor_masks"][row_index, endpoint] = neighbor_mask
                    output["resource_features"][row_index, endpoint] = resources
                    output["resource_masks"][row_index, endpoint] = resource_mask
                    output["graph_last_edge_ns"][row_index] = max(
                        output["graph_last_edge_ns"][row_index], latest
                    )
                output["pair_graph_features"][row_index] = state.pair_features(
                    left, right, played_ns
                )
                output["event_ids"][row_index] = str(row["event_id"])
                output["labels"][row_index] = int(row["target"])
                output["example_played_ns"][row_index] = played_ns
            processed_targets.add(hand_id)
        # Hand-derived edges are committed only after every hand at this event
        # time has been snapshotted.
        for timestamp_ns, _, payload in group_records:
            state.update_hand(timestamp_ns, payload)
    if processed_targets != set(targets_by_hand):
        raise ValueError("not every target hand received a graph snapshot")
    audits: dict[str, dict[str, Any]] = {}
    for split, output in arrays.items():
        if np.any(output["graph_last_edge_ns"] >= output["example_played_ns"]):
            raise ValueError(f"{split} graph contains a current or future edge")
        audits[split] = {
            "rows": len(output["labels"]),
            "hands": int(prepared[split]["hand_id"].nunique()),
            "positive_rows": int(output["labels"].sum()),
            "user_neighbor_edges": int(output["user_neighbor_masks"].sum()),
            "resource_edges": int(output["resource_masks"].sum()),
            "event_alignment_sha256": event_alignment_sha256(output["event_ids"]),
            "strictly_prior_edge_check": True,
            "equal_timestamp_isolation": True,
        }
    return arrays, audits


def build_graph_dataset(config: GraphDatasetConfig) -> dict[str, Any]:
    source_dir = config.source_dir.resolve()
    pair_dir = config.pair_dataset_dir.resolve()
    output_dir = config.output_dir.resolve()
    source_manifest_path = source_dir / "manifest.json"
    pair_manifest_path = pair_dir / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text())
    pair_manifest = json.loads(pair_manifest_path.read_text())
    if source_manifest["dataset_id"] != pair_manifest["dataset_id"]:
        raise ValueError("graph source and pair dataset IDs disagree")
    if pair_manifest["challenge_labels_public"]:
        raise ValueError("challenge labels cannot enter the graph dataset")
    if pair_manifest["feature_definition_version"] != "pair-features-v1":
        raise ValueError("Phase 11 requires pair-features-v1")
    if output_dir.exists() and any(output_dir.iterdir()):
        if not config.overwrite:
            raise FileExistsError(f"output directory is not empty: {output_dir}")
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    schema = {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "phase": 11,
        "benchmarks": list(config.benchmarks),
        "root_user_features": list(ROOT_USER_FEATURES),
        "user_edge_features": list(USER_EDGE_FEATURES),
        "resource_types": list(RESOURCE_TYPES),
        "resource_node_features": list(RESOURCE_NODE_FEATURES),
        "pair_graph_features": list(PAIR_GRAPH_FEATURES),
        "max_user_neighbors": config.max_user_neighbors,
        "max_resource_neighbors": config.max_resource_neighbors,
        "history_semantics": "strictly_before_example_played_at",
        "raw_id_embedding_count": 0,
        "inductive_node_initialization": "feature_only",
        "challenge_labels_public": False,
    }
    _write_json(output_dir / "schema.json", schema)
    artifacts = {"schema.json": sha256_file(output_dir / "schema.json")}
    benchmark_audits: dict[str, Any] = {}
    source_cache: dict[str, dict[str, list[dict[str, Any]]]] = {}

    def load_source(split: str) -> dict[str, list[dict[str, Any]]]:
        if split not in source_cache:
            values = {}
            for name in ("hands", "user_context", "sessions", "account_links"):
                relative = f"{split}/events/{name}.jsonl"
                path = source_dir / relative
                if sha256_file(path) != source_manifest["artifacts"][relative]:
                    raise ValueError(f"source graph hash mismatch: {relative}")
                values[name] = _read_jsonl(path)
            source_cache[split] = values
        return source_cache[split]

    for benchmark in config.benchmarks:
        frames: dict[str, pd.DataFrame] = {}
        for split in GRAPH_SPLITS:
            relative = f"dgx/{benchmark}/{split}.parquet"
            path = pair_dir / relative
            if sha256_file(path) != pair_manifest["artifacts"][relative]:
                raise ValueError(f"pair graph hash mismatch: {relative}")
            frame = pd.read_parquet(path)
            if set(frame["benchmark_split"].astype(str)) != {split}:
                raise ValueError(f"{relative} contains another benchmark split")
            frames[split] = frame
        benchmark_audits[benchmark] = {"splits": {}}
        if benchmark == "cold_start":
            for split in GRAPH_SPLITS:
                source = load_source(split)
                split_arrays, audits = build_source_graph_arrays(
                    {split: frames[split]},
                    source["hands"],
                    source["user_context"],
                    source["sessions"],
                    source["account_links"],
                    max_user_neighbors=config.max_user_neighbors,
                    max_resource_neighbors=config.max_resource_neighbors,
                )
                relative = f"benchmarks/{benchmark}/{split}.npz"
                write_deterministic_npz(output_dir / relative, split_arrays[split])
                artifacts[relative] = sha256_file(output_dir / relative)
                benchmark_audits[benchmark]["splits"][split] = audits[split]
        else:
            source_split = pair_manifest["benchmarks"][benchmark]["source_split"]
            source = load_source(source_split)
            split_arrays, audits = build_source_graph_arrays(
                frames,
                source["hands"],
                source["user_context"],
                source["sessions"],
                source["account_links"],
                max_user_neighbors=config.max_user_neighbors,
                max_resource_neighbors=config.max_resource_neighbors,
            )
            for split in GRAPH_SPLITS:
                relative = f"benchmarks/{benchmark}/{split}.npz"
                write_deterministic_npz(output_dir / relative, split_arrays[split])
                artifacts[relative] = sha256_file(output_dir / relative)
                benchmark_audits[benchmark]["splits"][split] = audits[split]
        print(
            f"[pair-graph-dataset] benchmark={benchmark} "
            + " ".join(
                f"{split}={benchmark_audits[benchmark]['splits'][split]['rows']}"
                for split in GRAPH_SPLITS
            ),
            flush=True,
        )
    manifest = {
        "schema_version": GRAPH_SCHEMA_VERSION,
        "phase": 11,
        "dataset_id": source_manifest["dataset_id"],
        "feature_definition_version": pair_manifest["feature_definition_version"],
        "source_world_manifest_sha256": sha256_file(source_manifest_path),
        "source_pair_manifest_sha256": sha256_file(pair_manifest_path),
        "benchmarks": benchmark_audits,
        "challenge_artifacts_read": False,
        "challenge_labels_public": False,
        "raw_id_embedding_count": 0,
        "artifacts": artifacts,
    }
    _write_json(output_dir / "manifest.json", manifest)
    return manifest


def load_graph_split(path: Path) -> dict[str, np.ndarray]:
    with np.load(path, allow_pickle=False) as bundle:
        return {name: bundle[name] for name in bundle.files}
