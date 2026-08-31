"""Supervise durable recovery loops with an optional Worker consumer."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any


class AgentRuntime:
    """One lifecycle for request recovery, failure recovery and Worker."""

    def __init__(
        self,
        request_recovery: Any,
        failure_recovery: Any,
    ) -> None:
        self.request_recovery = request_recovery
        self.failure_recovery = failure_recovery
        self._stop = asyncio.Event()
        self._started = asyncio.Event()
        self._stopped = asyncio.Event()
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._worker: Any | None = None
        self._running = False

    @property
    def running(self) -> bool:
        return self._running

    def request_stop(self) -> None:
        self._stop.set()
        if self._worker is not None:
            self._worker.request_stop()

    async def ready(self) -> bool:
        return (
            self._running
            and not self._stop.is_set()
            and bool(self._tasks)
            and all(not task.done() for task in self._tasks.values())
        )

    async def wait_started(self) -> None:
        await self._started.wait()

    async def run_recovery(self) -> None:
        """Run both loops for an API host that invokes the graph directly."""

        await self._run(None)

    async def run_worker(self, worker: Any) -> None:
        """Run both loops and the supplied reusable Worker as one unit."""

        self._worker = worker
        try:
            await self._run(worker)
        finally:
            worker.request_stop()

    async def stop(self, timeout_seconds: float) -> None:
        """Stop cooperatively, then cancel only loops exceeding the grace."""

        self.request_stop()
        if not self._running:
            return
        try:
            await asyncio.wait_for(
                self._stopped.wait(), timeout=timeout_seconds
            )
        except TimeoutError:
            for task in self._tasks.values():
                task.cancel()
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            await self._stopped.wait()

    async def _run(self, worker: Any | None) -> None:
        if self._running:
            raise RuntimeError("Agent runtime is already running")
        if self._stop.is_set():
            raise RuntimeError("Stopped Agent runtime cannot be restarted")
        self._running = True
        self._stopped.clear()
        try:
            async with asyncio.TaskGroup() as group:
                self._tasks = {
                    "request-recovery": group.create_task(
                        self._supervise(
                            "request recovery",
                            self.request_recovery.run(self._stop),
                        )
                    ),
                    "failure-recovery": group.create_task(
                        self._supervise(
                            "failure recovery",
                            self.failure_recovery.run(self._stop),
                        )
                    ),
                }
                if worker is not None:
                    self._tasks["worker"] = group.create_task(
                        self._run_worker(worker)
                    )
                self._started.set()
        finally:
            self._stop.set()
            if worker is not None:
                worker.request_stop()
            self._running = False
            self._stopped.set()

    async def _supervise(self, name: str, operation) -> None:
        await operation
        if not self._stop.is_set():
            raise RuntimeError(f"{name} stopped unexpectedly")

    async def _run_worker(self, worker: Any) -> None:
        await worker.run()
        if not self._stop.is_set():
            raise RuntimeError("Worker stopped unexpectedly")


@asynccontextmanager
async def recovery_lifespan(
    runtime: AgentRuntime,
    *,
    shutdown_timeout_seconds: float,
) -> AsyncIterator[None]:
    """Run recoveries in an API lifespan and join them on shutdown."""

    task = asyncio.create_task(runtime.run_recovery())
    await runtime.wait_started()
    if task.done():
        await task
    try:
        yield
    finally:
        runtime.request_stop()
        await runtime.stop(shutdown_timeout_seconds)
        await task
