from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from ex_agent.worker_main import _run_until_stopped


class FakeWorker:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.shutdown_calls = 0

    async def run(self) -> None:
        self.started.set()
        await self.finished.wait()

    async def shutdown(self) -> None:
        self.shutdown_calls += 1
        self.finished.set()


@pytest.mark.asyncio
async def test_stop_event_drains_worker_before_entrypoint_returns() -> None:
    worker = FakeWorker()
    stop_requested = asyncio.Event()
    serving = asyncio.create_task(
        _run_until_stopped(cast(Any, worker), stop_requested)
    )

    await asyncio.wait_for(worker.started.wait(), timeout=0.1)
    stop_requested.set()
    await asyncio.wait_for(serving, timeout=0.1)

    assert worker.shutdown_calls == 1


@pytest.mark.asyncio
async def test_natural_worker_completion_does_not_request_shutdown() -> None:
    worker = FakeWorker()
    stop_requested = asyncio.Event()
    serving = asyncio.create_task(
        _run_until_stopped(cast(Any, worker), stop_requested)
    )

    await asyncio.wait_for(worker.started.wait(), timeout=0.1)
    worker.finished.set()
    await asyncio.wait_for(serving, timeout=0.1)

    assert worker.shutdown_calls == 0
