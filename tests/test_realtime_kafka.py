from __future__ import annotations

from types import SimpleNamespace

import pytest

from pipeline.realtime import RealTimeProcessor


class _FakeConsumer:
    def __init__(self, responses, events: list[str] | None = None) -> None:
        self.responses = list(responses)
        self.events = events if events is not None else []
        self.commit_calls = 0
        self.closed = False

    def poll(self, *, timeout_ms: int, max_records: int):
        del timeout_ms
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        records = [SimpleNamespace(value=value) for value in response[:max_records]]
        remainder = response[max_records:]
        if remainder:
            self.responses.insert(0, remainder)
        return {"partition-0": records} if records else {}

    def commit(self) -> None:
        self.events.append("commit")
        self.commit_calls += 1

    def close(self) -> None:
        self.closed = True


class _RecordingProcessor(RealTimeProcessor):
    def __init__(self, events: list[str] | None = None, *, fail: bool = False) -> None:
        super().__init__(persist_history=False, persist_alerts=False)
        self.events = events if events is not None else []
        self.fail = fail
        self.batches: list[list[dict]] = []

    def _flush(self, buffer: list[dict]) -> int:
        self.events.append("flush")
        self.batches.append(list(buffer))
        if self.fail:
            raise RuntimeError("persistence failed")
        return len(buffer)


def _install_consumer(monkeypatch, consumer: _FakeConsumer):
    import kafka

    captured = {}

    def factory(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return consumer

    monkeypatch.setattr(kafka, "KafkaConsumer", factory)
    return captured


def test_run_kafka_commits_only_after_full_batch_succeeds(monkeypatch):
    events: list[str] = []
    consumer = _FakeConsumer(
        [[{"hand_id": "h1"}, {"hand_id": "h2"}, {"hand_id": "h3"}]],
        events,
    )
    captured = _install_consumer(monkeypatch, consumer)
    processor = _RecordingProcessor(events)

    total = processor.run_kafka(
        batch_size=3,
        max_messages=3,
        poll_timeout_ms=1,
    )

    assert total == 3
    assert [len(batch) for batch in processor.batches] == [3]
    assert events == ["flush", "commit"]
    assert consumer.commit_calls == 1
    assert consumer.closed
    assert captured["kwargs"]["enable_auto_commit"] is False


def test_run_kafka_flushes_partial_batch_on_interval(monkeypatch):
    consumer = _FakeConsumer(
        [[{"hand_id": "h1"}], KeyboardInterrupt()],
    )
    _install_consumer(monkeypatch, consumer)
    processor = _RecordingProcessor()

    total = processor.run_kafka(
        batch_size=25,
        flush_interval_seconds=0,
        poll_timeout_ms=1,
    )

    assert total == 1
    assert [len(batch) for batch in processor.batches] == [1]
    assert consumer.commit_calls == 1
    assert consumer.closed


def test_run_kafka_does_not_commit_failed_batch(monkeypatch, capsys):
    consumer = _FakeConsumer(
        [[{"hand_id": "h1"}, {"hand_id": "h2"}]],
    )
    _install_consumer(monkeypatch, consumer)
    processor = _RecordingProcessor(fail=True)

    with pytest.raises(RuntimeError, match="persistence failed"):
        processor.run_kafka(
            batch_size=2,
            max_messages=2,
            poll_timeout_ms=1,
        )

    assert consumer.commit_calls == 0
    assert consumer.closed
    assert "Kafka offsets were not committed" in capsys.readouterr().err


def test_run_kafka_rejects_invalid_batch_settings():
    processor = _RecordingProcessor()

    with pytest.raises(ValueError, match="batch_size"):
        processor.run_kafka(batch_size=0)
    with pytest.raises(ValueError, match="flush_interval_seconds"):
        processor.run_kafka(flush_interval_seconds=-1)
