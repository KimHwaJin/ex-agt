import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from ex_agent.config import Settings
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.worker import WorkflowWorker

pytestmark = pytest.mark.skipif(
    not {"TEST_DATABASE_URL", "TEST_REDIS_URL"}.issubset(os.environ),
    reason="Compose database and Redis are not configured",
)


def _settings(suffix: UUID) -> Settings:
    database_url = os.environ["TEST_DATABASE_URL"]
    return Settings(
        agent_database_url=database_url,
        agent_checkpoint_database_url=database_url.replace(
            "postgresql+psycopg://",
            "postgresql://",
        ),
        agent_redis_url=os.environ["TEST_REDIS_URL"],
        agent_command_stream=f"test-worker-commands-{suffix}",
        agent_command_consumer_group=f"test-command-group-{suffix}",
        executor_event_stream=f"test-executor-events-{suffix}",
        executor_event_consumer_group=f"test-executor-group-{suffix}",
        worker_metrics_enabled=False,
    )


async def _create_bound_task(
    worker: WorkflowWorker,
    execution_id: UUID,
) -> UUID:
    task_id = uuid4()
    await worker._repository.create_task(
        task_id=task_id,
        input_message_id=uuid4(),
        user_id="integration-user",
        project_id="integration-project",
        session_id=f"event-integration-{task_id}",
        content="Executor event integration test",
        idempotency_key=f"create-{task_id}",
    )
    await worker._repository.bind_execution(
        task_id=task_id,
        execution_id=execution_id,
        operation_id=uuid4(),
        execution_version=1,
        next_step_sequence=1,
    )
    return task_id


def _event(
    execution_id: UUID,
    sequence: int,
    event_type: str,
) -> ExecutorEvent:
    return ExecutorEvent.model_validate(
        {
            "event_id": uuid4(),
            "event_type": event_type,
            "schema_version": "1.0",
            "execution_id": execution_id,
            "event_sequence": sequence,
            "payload": {"sequence": sequence},
            "occurred_at": "2026-08-28T00:00:00Z",
        }
    )


def _fields(event: ExecutorEvent) -> dict[str, str]:
    return {
        "event_id": str(event.event_id),
        "event_type": event.event_type,
        "schema_version": event.schema_version,
        "execution_id": str(event.execution_id),
        "event_sequence": str(event.event_sequence),
        "payload": json.dumps(event.payload),
        "occurred_at": event.occurred_at,
    }


async def _read_one(
    worker: WorkflowWorker,
    consumer: str,
) -> tuple[str, dict[str, str]]:
    settings = worker._settings
    response = await worker._redis.xreadgroup(
        settings.executor_event_consumer_group,
        consumer,
        {settings.executor_event_stream: ">"},
        count=1,
        block=1000,
    )
    if not response:
        raise AssertionError("Executor event was not delivered")
    return response[0][1][0]


async def _publish(
    worker: WorkflowWorker,
    event: ExecutorEvent,
) -> None:
    await worker._redis.xadd(
        worker._settings.executor_event_stream,
        _fields(event),
    )


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_real_stream_processes_different_executions_concurrently() -> (
    None
):
    worker = WorkflowWorker(_settings(uuid4()))
    settings = worker._settings
    active = 0
    maximum_active = 0
    both_started = asyncio.Event()
    release = asyncio.Event()
    original = worker._persist_executor_event

    async def observed_persist(
        stream: str,
        task_id: UUID,
        event: ExecutorEvent,
    ) -> None:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        if active == 2:
            both_started.set()
        try:
            await release.wait()
            await original(stream, task_id, event)
        finally:
            active -= 1

    cast(Any, worker)._persist_executor_event = observed_persist
    try:
        await worker._ensure_group(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        execution_ids = [uuid4(), uuid4()]
        task_ids = [
            await _create_bound_task(worker, execution_id)
            for execution_id in execution_ids
        ]
        for execution_id in execution_ids:
            await _publish(
                worker,
                _event(execution_id, 1, "execution.started"),
            )
        entries = [
            await _read_one(worker, f"integration-consumer-{index}")
            for index in range(2)
        ]
        handlers = [
            asyncio.create_task(
                worker._handle_executor_event(
                    f"integration-consumer-{index}",
                    settings.executor_event_stream,
                    settings.executor_event_consumer_group,
                    message_id,
                    fields,
                )
            )
            for index, (message_id, fields) in enumerate(entries)
        ]
        await asyncio.wait_for(both_started.wait(), timeout=2)
        release.set()
        await asyncio.gather(*handlers)

        bindings = [
            await worker._repository.binding_for_task(task_id)
            for task_id in task_ids
        ]
        pending = await worker._redis.xpending(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        assert maximum_active == 2
        assert [item.last_event_sequence for item in bindings] == [1, 1]
        assert pending["pending"] == 0
    finally:
        release.set()
        await worker._redis.delete(
            settings.executor_event_stream,
            settings.agent_command_stream,
        )
        await worker.close()


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_real_stream_recovers_reverse_order_and_deduplicates() -> None:
    worker = WorkflowWorker(_settings(uuid4()))
    settings = worker._settings
    execution_id = uuid4()
    first = _event(execution_id, 1, "execution.started")
    second = _event(execution_id, 2, "execution.step_started")
    history_calls: list[tuple[UUID, int, int]] = []

    async def events_after(
        requested_execution_id: UUID,
        *,
        after_sequence: int,
        limit: int,
    ) -> list[ExecutorEvent]:
        history_calls.append((requested_execution_id, after_sequence, limit))
        return [first]

    executor = cast(Any, worker._executor)
    executor.events_after = cast(
        Callable[..., Awaitable[list[ExecutorEvent]]],
        events_after,
    )
    try:
        await worker._ensure_group(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        task_id = await _create_bound_task(worker, execution_id)

        await _publish(worker, second)
        message_id, fields = await _read_one(worker, "reverse-consumer")
        await worker._handle_executor_event(
            "reverse-consumer",
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
            message_id,
            fields,
        )

        await _publish(worker, first)
        message_id, fields = await _read_one(worker, "duplicate-consumer")
        await worker._handle_executor_event(
            "duplicate-consumer",
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
            message_id,
            fields,
        )

        binding = await worker._repository.binding_for_task(task_id)
        events = await worker._repository.events_after(task_id, 0)
        executor_events = [
            event
            for event in events
            if event.event_type.startswith("execution.")
            or event.event_type == "executor.boundary_received"
        ]
        pending = await worker._redis.xpending(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        assert history_calls == [(execution_id, 0, 2)]
        assert binding.last_event_sequence == 2
        assert [event.event_type for event in executor_events] == [
            "execution.started",
            "execution.step_started",
        ]
        assert [
            event.payload["event_sequence"] for event in executor_events
        ] == [1, 2]
        assert pending["pending"] == 0
    finally:
        await worker._redis.delete(
            settings.executor_event_stream,
            settings.agent_command_stream,
        )
        await worker.close()


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_stale_event_catches_up_to_latest_executor_history() -> None:
    worker = WorkflowWorker(_settings(uuid4()))
    settings = worker._settings
    execution_id = uuid4()
    first = _event(execution_id, 1, "execution.started")
    second = _event(execution_id, 2, "execution.operation_completed")
    terminal = _event(execution_id, 3, "execution.completed")
    history_calls: list[tuple[UUID, int, int]] = []

    async def events_after(
        requested_execution_id: UUID,
        *,
        after_sequence: int,
        limit: int,
    ) -> list[ExecutorEvent]:
        history_calls.append((requested_execution_id, after_sequence, limit))
        return [first, second, terminal]

    executor = cast(Any, worker._executor)
    executor.events_after = cast(
        Callable[..., Awaitable[list[ExecutorEvent]]],
        events_after,
    )
    try:
        await worker._ensure_group(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        task_id = await _create_bound_task(worker, execution_id)
        await _publish(worker, first)
        message_id, fields = await _read_one(worker, "stale-consumer")

        await worker._handle_executor_event(
            "stale-consumer",
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
            message_id,
            fields,
            catch_up=True,
        )

        binding = await worker._repository.binding_for_task(task_id)
        events = await worker._repository.events_after(task_id, 0)
        executor_events = [
            event
            for event in events
            if event.event_type.startswith("execution.")
            or event.event_type == "executor.boundary_received"
        ]
        assert history_calls == [(execution_id, 0, 500)]
        assert binding.last_event_sequence == 3
        assert [
            event.payload["event_sequence"] for event in executor_events
        ] == [1, 2, 3]
    finally:
        await worker._redis.delete(
            settings.executor_event_stream,
            settings.agent_command_stream,
        )
        await worker.close()


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_event_arriving_before_binding_is_not_acknowledged() -> None:
    worker = WorkflowWorker(_settings(uuid4()))
    settings = worker._settings
    execution_id = uuid4()
    event = _event(execution_id, 1, "execution.started")

    async def events_after(
        requested_execution_id: UUID,
        *,
        after_sequence: int,
        limit: int,
    ) -> list[ExecutorEvent]:
        assert requested_execution_id == execution_id
        assert after_sequence == 0
        assert limit == 500
        return [event]

    executor = cast(Any, worker._executor)
    executor.events_after = cast(
        Callable[..., Awaitable[list[ExecutorEvent]]],
        events_after,
    )
    try:
        await worker._ensure_group(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        await _publish(worker, event)
        message_id, fields = await _read_one(
            worker,
            "binding-race-consumer",
        )

        await worker._handle_executor_event(
            "binding-race-consumer",
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
            message_id,
            fields,
        )

        pending = await worker._redis.xpending(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        assert pending["pending"] == 1

        task_id = await _create_bound_task(worker, execution_id)
        await worker._handle_executor_event(
            "binding-race-consumer",
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
            message_id,
            fields,
            catch_up=True,
        )

        pending = await worker._redis.xpending(
            settings.executor_event_stream,
            settings.executor_event_consumer_group,
        )
        binding = await worker._repository.binding_for_task(task_id)
        assert pending["pending"] == 0
        assert binding.last_event_sequence == 1
    finally:
        await worker._redis.delete(
            settings.executor_event_stream,
            settings.agent_command_stream,
        )
        await worker.close()
