from __future__ import annotations

import asyncio

import pytest

from agent.runtime.config import build_worker_settings
from agent.runtime.lifecycle import AgentRuntime, recovery_lifespan
from ex_agent.config import Settings


class Recovery:
    def __init__(self, *, return_early: bool = False) -> None:
        self.return_early = return_early
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()

    async def run(self, stop: asyncio.Event) -> None:
        self.started.set()
        if not self.return_early:
            await stop.wait()
        self.stopped.set()


class Worker:
    def __init__(self) -> None:
        self.stop = asyncio.Event()
        self.started = asyncio.Event()
        self.cancelled = False

    async def run(self) -> None:
        self.started.set()
        try:
            await self.stop.wait()
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def request_stop(self) -> None:
        self.stop.set()


def test_agent_settings_map_to_explicit_worker_topology(tmp_path):
    settings = Settings(
        agent_database_url="postgresql+psycopg://db/agent",
        agent_checkpoint_database_url="postgresql://db/checkpoints",
        agent_redis_url="redis://cache/3",
        agent_command_stream="agent-work",
        agent_command_consumer_group="agent-dispatch",
        executor_event_stream="executor-source",
        executor_event_consumer_group="agent-ingress",
        executor_worker_namespace="tenant-safe-worker",
        worker_command_concurrency=3,
        worker_executor_event_concurrency=7,
        checkpoint_pool_max_size=9,
        agent_skill_root=tmp_path,
    )

    result = build_worker_settings(settings)

    assert result.database_url == "postgresql://db/checkpoints"
    assert result.redis_url == "redis://cache/3"
    assert result.namespace == "tenant-safe-worker"
    assert result.command_stream == "agent-work"
    assert result.command_group == "agent-dispatch"
    assert result.executor_event_stream == "executor-source"
    assert result.event_group == "agent-ingress"
    assert result.ingress_workers == 7
    assert result.dispatch_workers == 3
    assert result.pool_size == 9


async def test_runtime_supervises_worker_and_both_recovery_loops():
    request, failure, worker = Recovery(), Recovery(), Worker()
    runtime = AgentRuntime(request, failure)

    task = asyncio.create_task(runtime.run_worker(worker))
    await asyncio.gather(
        request.started.wait(), failure.started.wait(), worker.started.wait()
    )
    assert await runtime.ready()

    runtime.request_stop()
    await task

    assert request.stopped.is_set()
    assert failure.stopped.is_set()
    assert worker.stop.is_set()
    assert not await runtime.ready()


async def test_runtime_supervises_product_event_relay():
    request, failure, delivery = Recovery(), Recovery(), Recovery()
    runtime = AgentRuntime(request, failure, delivery)

    task = asyncio.create_task(runtime.run_recovery())
    await asyncio.gather(
        request.started.wait(),
        failure.started.wait(),
        delivery.started.wait(),
    )
    assert await runtime.ready()

    runtime.request_stop()
    await task

    assert delivery.stopped.is_set()


async def test_api_recovery_lifespan_starts_and_joins_both_loops():
    request, failure = Recovery(), Recovery()
    runtime = AgentRuntime(request, failure)

    async with recovery_lifespan(runtime, shutdown_timeout_seconds=1):
        assert await runtime.ready()

    assert request.stopped.is_set()
    assert failure.stopped.is_set()
    assert not runtime.running


async def test_unexpected_recovery_exit_stops_worker():
    runtime = AgentRuntime(Recovery(return_early=True), Recovery())
    worker = Worker()

    with pytest.raises(ExceptionGroup) as raised:
        await runtime.run_worker(worker)

    assert any(
        "request recovery" in str(error) for error in raised.value.exceptions
    )
    assert worker.stop.is_set()
    assert worker.cancelled
    assert not await runtime.ready()


async def test_runtime_cannot_restart_after_stop():
    runtime = AgentRuntime(Recovery(), Recovery())
    runtime.request_stop()

    with pytest.raises(RuntimeError, match="cannot be restarted"):
        await runtime.run_recovery()


async def test_runtime_readiness_requires_all_supervised_tasks_alive():
    runtime = AgentRuntime(Recovery(), Recovery())
    runtime._running = True
    runtime._tasks = {
        "done": asyncio.create_task(asyncio.sleep(0)),
    }
    await runtime._tasks["done"]

    assert not await runtime.ready()
