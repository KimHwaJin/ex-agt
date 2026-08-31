import asyncio

import pytest

from executor_worker import DeferEvent
from executor_worker.guard import LeaseLostError, SessionGuard


@pytest.mark.redis
@pytest.mark.postgres
async def test_shared_guard_excludes_same_session_allows_other(worker):
    other = SessionGuard(
        worker.redis, worker.settings.namespace, renew_seconds=1
    )
    async with worker.guard.hold("session"):
        with pytest.raises(DeferEvent):
            async with other.hold("session"):
                raise AssertionError("Concurrent invocation")
        async with other.hold("another-session"):
            pass
    async with other.hold("session"):
        pass


@pytest.mark.redis
@pytest.mark.postgres
async def test_lock_loss_cancels_work_without_releasing_new_owner(worker):
    key = worker.guard.key("session")
    with pytest.raises(LeaseLostError):
        async with worker.guard.hold("session"):
            await worker.redis.set(key, "different-owner", ex=60)
            await asyncio.wait_for(asyncio.Event().wait(), 3)
    assert await worker.redis.get(key) == "different-owner"


@pytest.mark.redis
@pytest.mark.postgres
async def test_api_guard_external_cancellation_propagates(worker):
    entered = asyncio.Event()

    async def request():
        async with worker.guard.hold("session"):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(request())
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    async with worker.guard.hold("session"):
        pass
