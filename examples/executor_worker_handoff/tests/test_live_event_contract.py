"""Opt-in checks of actual Executor REST/Redis event interoperability.

Requires an isolated TEST_DATABASE_URL, a real TEST_REDIS_URL,
TEST_EXECUTOR_URL, and TEST_EXECUTION_ID of a completed test execution.
Does not submit executions or modify the source Redis Stream.
"""

import os
from uuid import uuid4

import httpx
import pytest

from worker import ExecutorEvent
from worker.ingress import EventRouter
from worker.redis_streams import next_stream_id


@pytest.fixture
async def live_event(worker):
    base_url = os.getenv("TEST_EXECUTOR_URL")
    execution_id = os.getenv("TEST_EXECUTION_ID")
    if not base_url or not execution_id:
        pytest.skip("Requires explicit actual Executor config")  # ty: ignore
    async with httpx.AsyncClient(
        base_url=base_url.rstrip("/") + "/",
    ) as http:
        response = await http.get(
            f"executions/{execution_id}/events", params={"limit": 500}
        )
        response.raise_for_status()
        page = response.json()
        assert not page["has_more"], "Use a small completed test execution"
        item = page["items"][-1]
        assert item["event_sequence"] > 1
        start = "-"
        stream = os.getenv("TEST_EXECUTOR_STREAM", "executor.events")
        while entries := await worker.redis.xrange(
            stream, min=start, count=500
        ):
            for _, fields in entries:
                if fields.get("event_id") == item["event_id"]:
                    return base_url, fields, item
            next_id = next_stream_id(entries[-1][0])
            if next_id is None:
                break
            start = next_id
    raise AssertionError("Actual event no longer exists in Redis")


async def test_live_redis_then_rest_is_same_event(worker, live_event):
    _, fields, item = live_event
    await worker.store.ingest(ExecutorEvent.from_redis(fields))
    # REST audit/delivery metadata and equivalent UTC spellings must not
    # turn the same Executor event into a conflicting identity.
    await worker.store.ingest(ExecutorEvent.model_validate(item))


async def test_live_rest_fills_gap_before_redis_tail(worker, live_event):
    base_url, fields, _ = live_event
    tail = ExecutorEvent.from_redis(fields)
    await worker.bindings.register(
        execution_id=tail.execution_id,
        session_id=f"live-contract-{uuid4()}",
        task_id=str(uuid4()),
    )
    # Inject a missing prefix into this isolated Inbox, not the shared
    # Stream: only the actual Redis terminal event has arrived so far.
    await worker.store.ingest(tail)
    async with httpx.AsyncClient(
        base_url=base_url.rstrip("/") + "/",
    ) as http:
        router = EventRouter(worker.store, http, {tail.event_type})
        for _ in range(2):
            await router.once()
    async with worker.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT last_sequence,last_error FROM ew_bindings "
            "WHERE namespace=%s AND execution_id=%s",
            (worker.settings.namespace, tail.execution_id),
        )
        assert await cur.fetchone() == (tail.event_sequence, None)
