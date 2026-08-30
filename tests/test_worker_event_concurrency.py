import asyncio
from typing import Any
from uuid import UUID, uuid4

import pytest

from ex_agent.config import Settings
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.worker import WorkflowWorker


class FakeLockRedis:
    def __init__(self) -> None:
        self.locks: dict[str, str] = {}
        self.acknowledged: list[str] = []

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool,
        ex: int,
    ) -> bool:
        del nx, ex
        if key in self.locks:
            return False
        self.locks[key] = value
        return True

    async def eval(
        self,
        script: str,
        key_count: int,
        key: str,
        value: str,
        *args: Any,
    ) -> int:
        del key_count, args
        if "del" in script and self.locks.get(key) == value:
            del self.locks[key]
            return 1
        return int(self.locks.get(key) == value)

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        del stream, group
        self.acknowledged.append(message_id)
        return 1


def _event_fields(execution_id: UUID, sequence: int = 1) -> dict[str, str]:
    return {
        "event_id": str(uuid4()),
        "event_type": "execution.started",
        "schema_version": "1.0",
        "execution_id": str(execution_id),
        "event_sequence": str(sequence),
        "payload": "{}",
        "occurred_at": "2026-08-28T00:00:00Z",
    }


def _worker(redis: FakeLockRedis) -> Any:
    worker: Any = WorkflowWorker.__new__(WorkflowWorker)
    worker._settings = Settings(worker_metrics_enabled=False)
    worker._redis = redis
    return worker


@pytest.mark.asyncio
async def test_different_executions_are_processed_concurrently() -> None:
    worker = _worker(FakeLockRedis())
    active = 0
    maximum_active = 0
    both_started = asyncio.Event()
    release = asyncio.Event()

    async def process(
        stream: str,
        group: str,
        message_id: str,
        event: ExecutorEvent,
    ) -> None:
        del stream, group, message_id, event
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        if active == 2:
            both_started.set()
        try:
            await release.wait()
        finally:
            active -= 1

    worker._process_executor_event = process
    tasks = [
        asyncio.create_task(
            worker._handle_executor_event(
                f"consumer-{index}",
                "executor.events",
                "group",
                f"{index}-0",
                _event_fields(uuid4()),
            )
        )
        for index in range(2)
    ]
    await asyncio.wait_for(both_started.wait(), timeout=1)
    release.set()
    await asyncio.gather(*tasks)

    assert maximum_active == 2


@pytest.mark.asyncio
async def test_same_execution_is_serialized_by_lock() -> None:
    worker = _worker(FakeLockRedis())
    execution_id = uuid4()
    started = asyncio.Event()
    release = asyncio.Event()
    calls = 0

    async def process(
        stream: str,
        group: str,
        message_id: str,
        event: ExecutorEvent,
    ) -> None:
        del stream, group, message_id, event
        nonlocal calls
        calls += 1
        started.set()
        await release.wait()

    worker._process_executor_event = process
    first = asyncio.create_task(
        worker._handle_executor_event(
            "consumer-1",
            "executor.events",
            "group",
            "1-0",
            _event_fields(execution_id, 1),
        )
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    await worker._handle_executor_event(
        "consumer-2",
        "executor.events",
        "group",
        "2-0",
        _event_fields(execution_id, 2),
    )
    release.set()
    await first

    assert calls == 1


@pytest.mark.asyncio
async def test_failed_executor_event_stays_pending_and_releases_lock() -> None:
    redis = FakeLockRedis()
    worker = _worker(redis)

    async def process(
        stream: str,
        group: str,
        message_id: str,
        event: ExecutorEvent,
    ) -> None:
        del stream, group, message_id, event
        raise RuntimeError("temporary database outage")

    worker._process_executor_event = process
    await worker._handle_executor_event(
        "consumer-1",
        "executor.events",
        "group",
        "1-0",
        _event_fields(uuid4()),
    )

    assert redis.acknowledged == []
    assert redis.locks == {}


def test_transport_retry_is_exponential_and_bounded() -> None:
    worker = _worker(FakeLockRedis())

    assert worker._next_retry_delay(0.5) == 1
    assert worker._next_retry_delay(20) == 30
