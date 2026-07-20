"""Deterministic scheduling and replay of frozen multi-stream worlds."""

from __future__ import annotations

import heapq
import json
import random
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Iterator, Literal, Protocol, Sequence

from pipeline.events import event_partition_key, validate_event
from pipeline.events.contracts import TOPIC_BY_EVENT_TYPE


ReplayMode = Literal["replay", "accelerated", "realtime", "chaos"]

_EVENT_FILES = (
    "user_context.jsonl",
    "sessions.jsonl",
    "account_links.jsonl",
    "hands.jsonl",
)
_EVENT_TYPE_PRIORITY = {
    "poker.user-context.updated": 0,
    "poker.session.started": 1,
    "poker.account-link.updated": 2,
    "poker.hand.completed": 3,
}


@dataclass(frozen=True)
class ReplayEvent:
    envelope: dict
    split: str
    source_path: str
    source_line: int
    occurred_at: datetime
    canonical_topic: str
    partition_key: str

    @property
    def event_id(self) -> str:
        return str(self.envelope["event_id"])

    @property
    def event_type(self) -> str:
        return str(self.envelope["event_type"])


@dataclass(frozen=True)
class ReplayConfig:
    mode: ReplayMode = "replay"
    splits: tuple[str, ...] = ("train", "validation", "test", "challenge")
    max_events: int | None = None
    rate: float = 0.0
    speed: float = 3_600.0
    chaos_seed: int = 91_001
    duplicate_rate: float = 0.01
    late_rate: float = 0.02
    reorder_window: int = 25
    publish_batch_size: int = 500
    ack_timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.mode not in ("replay", "accelerated", "realtime", "chaos"):
            raise ValueError(f"unsupported replay mode: {self.mode}")
        if not self.splits:
            raise ValueError("at least one dataset split is required")
        if self.max_events is not None and self.max_events < 1:
            raise ValueError("max_events must be positive")
        if self.rate < 0:
            raise ValueError("rate must not be negative")
        if self.mode == "accelerated" and self.rate <= 0:
            raise ValueError("accelerated mode requires a positive rate")
        if self.speed <= 0:
            raise ValueError("speed must be positive")
        for name, value in (("duplicate_rate", self.duplicate_rate), ("late_rate", self.late_rate)):
            if not 0 <= value <= 1:
                raise ValueError(f"{name} must be in [0, 1]")
        if self.reorder_window < 1:
            raise ValueError("reorder_window must be positive")
        if self.publish_batch_size < 1:
            raise ValueError("publish_batch_size must be positive")
        if self.ack_timeout_seconds <= 0:
            raise ValueError("ack_timeout_seconds must be positive")


@dataclass(frozen=True)
class PendingPublish:
    event_id: str
    topic: str
    key: str
    handle: object | None


@dataclass(frozen=True)
class PublishAck:
    event_id: str
    topic: str
    key: str
    partition: int | None
    offset: int | None


class EventPublisher(Protocol):
    def publish(self, event: ReplayEvent) -> PendingPublish: ...

    def acknowledge(self, pending: PendingPublish, timeout_seconds: float) -> PublishAck: ...

    def flush(self) -> None: ...

    def close(self) -> None: ...


class DryRunPublisher:
    """Validate routing and counts without connecting to Kafka."""

    def publish(self, event: ReplayEvent) -> PendingPublish:
        return PendingPublish(
            event_id=event.event_id,
            topic=event.canonical_topic,
            key=event.partition_key,
            handle=None,
        )

    def acknowledge(self, pending: PendingPublish, timeout_seconds: float) -> PublishAck:
        del timeout_seconds
        return PublishAck(
            event_id=pending.event_id,
            topic=pending.topic,
            key=pending.key,
            partition=None,
            offset=None,
        )

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


@dataclass
class ReplayReport:
    dataset_id: str
    mode: ReplayMode
    splits: list[str]
    source_events: int
    attempted: int
    acknowledged: int
    duplicate_attempts: int
    attempted_by_topic: dict[str, int]
    acknowledged_by_topic: dict[str, int]
    first_occurred_at: str | None
    last_occurred_at: str | None
    elapsed_seconds: float

    def to_dict(self) -> dict:
        return {
            "dataset_id": self.dataset_id,
            "mode": self.mode,
            "splits": self.splits,
            "source_events": self.source_events,
            "attempted": self.attempted,
            "acknowledged": self.acknowledged,
            "duplicate_attempts": self.duplicate_attempts,
            "attempted_by_topic": dict(sorted(self.attempted_by_topic.items())),
            "acknowledged_by_topic": dict(sorted(self.acknowledged_by_topic.items())),
            "first_occurred_at": self.first_occurred_at,
            "last_occurred_at": self.last_occurred_at,
            "elapsed_seconds": round(self.elapsed_seconds, 6),
        }


def _event_sort_key(event: ReplayEvent, split_order: dict[str, int]) -> tuple:
    return (
        event.occurred_at,
        split_order[event.split],
        _EVENT_TYPE_PRIORITY[event.event_type],
        event.event_id,
    )


def _iter_event_file(path: Path, split: str, expected_dataset_id: str) -> Iterator[ReplayEvent]:
    with path.open() as stream:
        for line_number, line in enumerate(stream, start=1):
            if not line.strip():
                continue
            raw = json.loads(line)
            envelope = validate_event(raw)
            if envelope.dataset_id != expected_dataset_id:
                raise ValueError(
                    f"{path}:{line_number} dataset_id={envelope.dataset_id!r} "
                    f"does not match manifest {expected_dataset_id!r}"
                )
            if envelope.dataset_split != split:
                raise ValueError(
                    f"{path}:{line_number} split={envelope.dataset_split!r} "
                    f"does not match directory {split!r}"
                )
            yield ReplayEvent(
                envelope=envelope.model_dump(mode="json"),
                split=split,
                source_path=str(path),
                source_line=line_number,
                occurred_at=envelope.occurred_at,
                canonical_topic=TOPIC_BY_EVENT_TYPE[envelope.event_type],
                partition_key=event_partition_key(envelope),
            )


def load_world_manifest(dataset_dir: Path) -> dict:
    manifest_path = dataset_dir / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"missing world manifest: {manifest_path}")
    return json.loads(manifest_path.read_text())


def iter_world_events(
    dataset_dir: Path,
    splits: Sequence[str] = ("train", "validation", "test", "challenge"),
    max_events: int | None = None,
) -> Iterator[ReplayEvent]:
    """Merge all selected public streams into deterministic event-time order."""
    manifest = load_world_manifest(dataset_dir)
    dataset_id = str(manifest["dataset_id"])
    available_splits = set(manifest["splits"])
    selected = tuple(splits)
    unknown = set(selected) - available_splits
    if unknown:
        raise ValueError(f"unknown dataset splits: {sorted(unknown)}")
    split_order = {split: index for index, split in enumerate(selected)}
    streams: list[Iterator[ReplayEvent]] = []
    for split in selected:
        for filename in _EVENT_FILES:
            path = dataset_dir / split / "events" / filename
            if not path.is_file():
                raise FileNotFoundError(f"missing event stream: {path}")
            streams.append(_iter_event_file(path, split, dataset_id))

    merged = heapq.merge(
        *streams,
        key=lambda event: _event_sort_key(event, split_order),
    )
    for index, event in enumerate(merged):
        if max_events is not None and index >= max_events:
            break
        yield event


def iter_delivery_events(
    events: Iterable[ReplayEvent],
    config: ReplayConfig,
) -> Iterator[ReplayEvent]:
    """Apply a deterministic delivery schedule without mutating events."""
    if config.mode != "chaos":
        yield from events
        return

    rng = random.Random(config.chaos_seed)
    delayed: list[ReplayEvent] = []
    for event in events:
        if rng.random() < config.late_rate:
            delayed.append(event)
        else:
            yield event
        if rng.random() < config.duplicate_rate:
            yield event
        if len(delayed) >= config.reorder_window:
            yield delayed.pop(rng.randrange(len(delayed)))
    while delayed:
        yield delayed.pop(rng.randrange(len(delayed)))


class EventPacer:
    def __init__(
        self,
        config: ReplayConfig,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.config = config
        self._monotonic = monotonic
        self._sleep = sleep
        self._started: float | None = None
        self._first_event_time: datetime | None = None

    def wait(self, index: int, event: ReplayEvent) -> None:
        if self._started is None:
            self._started = self._monotonic()
            self._first_event_time = event.occurred_at
            return

        target_elapsed = 0.0
        if self.config.mode == "realtime":
            assert self._first_event_time is not None
            target_elapsed = (
                event.occurred_at - self._first_event_time
            ).total_seconds() / self.config.speed
        elif self.config.rate > 0:
            target_elapsed = index / self.config.rate
        else:
            return
        delay = self._started + target_elapsed - self._monotonic()
        if delay > 0:
            self._sleep(delay)


def _drain_pending(
    publisher: EventPublisher,
    pending: list[PendingPublish],
    report_counts: dict[str, int],
    timeout_seconds: float,
) -> int:
    acknowledged = 0
    for item in pending:
        ack = publisher.acknowledge(item, timeout_seconds)
        report_counts[ack.topic] = report_counts.get(ack.topic, 0) + 1
        acknowledged += 1
    pending.clear()
    return acknowledged


def replay_world(
    dataset_dir: Path,
    publisher: EventPublisher,
    config: ReplayConfig | None = None,
    *,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> ReplayReport:
    """Validate, schedule, publish, acknowledge, and summarize one replay."""
    cfg = config or ReplayConfig()
    manifest = load_world_manifest(dataset_dir)
    source = iter_world_events(dataset_dir, cfg.splits, cfg.max_events)
    delivery = iter_delivery_events(source, cfg)
    pacer = EventPacer(cfg, monotonic=monotonic, sleep=sleep)
    started = monotonic()
    attempted_by_topic: dict[str, int] = {}
    acknowledged_by_topic: dict[str, int] = {}
    source_event_ids: set[str] = set()
    pending: list[PendingPublish] = []
    attempted = 0
    acknowledged = 0
    first_occurred_at: datetime | None = None
    last_occurred_at: datetime | None = None

    try:
        for index, event in enumerate(delivery):
            pacer.wait(index, event)
            if first_occurred_at is None or event.occurred_at < first_occurred_at:
                first_occurred_at = event.occurred_at
            if last_occurred_at is None or event.occurred_at > last_occurred_at:
                last_occurred_at = event.occurred_at
            source_event_ids.add(event.event_id)
            item = publisher.publish(event)
            pending.append(item)
            attempted += 1
            attempted_by_topic[item.topic] = attempted_by_topic.get(item.topic, 0) + 1
            if len(pending) >= cfg.publish_batch_size:
                acknowledged += _drain_pending(
                    publisher,
                    pending,
                    acknowledged_by_topic,
                    cfg.ack_timeout_seconds,
                )
        acknowledged += _drain_pending(
            publisher,
            pending,
            acknowledged_by_topic,
            cfg.ack_timeout_seconds,
        )
        publisher.flush()
    finally:
        publisher.close()

    return ReplayReport(
        dataset_id=str(manifest["dataset_id"]),
        mode=cfg.mode,
        splits=list(cfg.splits),
        source_events=len(source_event_ids),
        attempted=attempted,
        acknowledged=acknowledged,
        duplicate_attempts=attempted - len(source_event_ids),
        attempted_by_topic=attempted_by_topic,
        acknowledged_by_topic=acknowledged_by_topic,
        first_occurred_at=first_occurred_at.isoformat() if first_occurred_at else None,
        last_occurred_at=last_occurred_at.isoformat() if last_occurred_at else None,
        elapsed_seconds=max(0.0, monotonic() - started),
    )
