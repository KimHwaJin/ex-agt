from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis
from redis.exceptions import ConnectionError, ResponseError

from worker.consumer import (
    AckDecision,
    HandlerResult,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
)
from worker.redis_streams import (
    claim_pending_page,
    group_progress,
    next_stream_id,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("0-0", "0-1"),
        ("9007199254740993-0", "9007199254740993-1"),
        ("10-18446744073709551615", "11-0"),
        ("18446744073709551615-18446744073709551615", None),
    ],
)
def test_inclusive_cursor_is_exact(value, expected):
    assert next_stream_id(value) == expected


@pytest.mark.parametrize("value", ["bad", "1-2-3", "18446744073709551616-0"])
def test_invalid_cursor_is_rejected(value):
    with pytest.raises(ValueError):
        next_stream_id(value)


@pytest.fixture
async def transport():
    if "TEST_REDIS_URL" not in os.environ:
        pytest.skip("Requires isolated Redis")  # ty: ignore
    redis = Redis.from_url(os.environ["TEST_REDIS_URL"], decode_responses=True)
    stream = f"compat-{uuid4()}"
    await redis.xgroup_create(stream, "g", id="0", mkstream=True)
    try:
        yield redis, stream
    finally:
        await redis.delete(stream)
        await redis.aclose()


async def claim(redis, stream, *, cursor="0-0", consumer="new", count=2):
    return await claim_pending_page(
        redis,
        stream,
        "g",
        consumer,
        min_idle_milliseconds=30000,
        cursor=cursor,
        count=count,
    )


async def seed_pending(redis, stream, count):
    ids = [
        await redis.xadd(stream, {"value": str(i)}, id=f"1-{i}")
        for i in range(count)
    ]
    await redis.xreadgroup("g", "old", {stream: ">"}, count=count)
    return ids


async def make_stale(redis, stream, ids):
    await redis.xclaim(stream, "g", "old", 0, ids, idle=60000, justid=True)


@pytest.mark.redis
async def test_page_advances_past_fresh_entries_without_claiming_them(
    transport,
):
    redis, stream = transport
    ids = await seed_pending(redis, stream, 5)
    await make_stale(redis, stream, ids[2:])
    cursor, messages = await claim(redis, stream)
    assert cursor == "1-2" and messages == []
    cursor, messages = await claim(redis, stream, cursor=cursor)
    assert cursor == "1-4"
    assert messages == [(ids[i], {"value": str(i)}) for i in (2, 3)]
    cursor, messages = await claim(redis, stream, cursor=cursor)
    assert cursor == "0-0"
    assert messages == [(ids[4], {"value": "4"})]
    pending = await redis.xpending_range(stream, "g", "-", "+", 10)
    assert [row["consumer"] for row in pending] == ["old"] * 2 + ["new"] * 3


@pytest.mark.redis
async def test_two_reclaimers_cannot_claim_same_fresh_lease(transport):
    redis, stream = transport
    ids = await seed_pending(redis, stream, 3)
    await make_stale(redis, stream, ids)
    pages = await asyncio.gather(
        claim(redis, stream, consumer="a", count=3),
        claim(redis, stream, consumer="b", count=3),
    )
    assert sorted(mid for _, rows in pages for mid, _ in rows) == ids


@pytest.mark.redis
async def test_missing_body_is_cleaned_but_live_pending_is_preserved(
    transport,
):
    redis, stream = transport
    ids = await seed_pending(redis, stream, 3)
    await make_stale(redis, stream, ids[:2])
    await redis.xdel(stream, ids[0])
    _, messages = await claim(redis, stream, count=3)
    assert messages == [(ids[1], {"value": "1"})]
    pending = await redis.xpending_range(stream, "g", "-", "+", 10)
    assert [row["message_id"] for row in pending] == ids[1:]


@pytest.mark.redis
async def test_heartbeat_refresh_prevents_reclaim(transport):
    redis, stream = transport
    ids = await seed_pending(redis, stream, 1)
    await make_stale(redis, stream, ids)
    await redis.xclaim(stream, "g", "old", 0, ids, justid=True)
    assert (await claim(redis, stream))[1] == []


@pytest.mark.redis
async def test_progress_never_reports_unknown_lag_as_zero(transport):
    redis, stream = transport
    await redis.xadd(stream, {"value": "unread"})
    before = await group_progress(redis, stream, "g")
    assert before.pending == 0 and before.has_unread
    assert before.lag is None or before.lag == 1
    rows = await redis.xreadgroup("g", "old", {stream: ">"}, count=1)
    after = await group_progress(redis, stream, "g")
    assert after.pending == 1 and not after.has_unread
    assert after.lag is None or after.lag == 0
    await redis.xack(stream, "g", rows[0][1][0][0])


class RecordingHandler:
    def __init__(self):
        self.messages = []

    def lock_key(self, message):
        return None

    async def handle(self, message):
        self.messages.append(message)
        return HandlerResult(AckDecision.ACK)


def consumer(redis, stream, handler):
    return RedisStreamConsumer(
        redis,
        RedisStreamConsumerConfig(
            stream=stream,
            group="g",
            consumer_prefix="compat",
            block_milliseconds=10,
            claim_batch_size=1,
        ),
        lambda _: handler,
        retry_initial_seconds=0.01,
        retry_max_seconds=0.05,
    )


async def wait_until(check):
    for _ in range(500):
        if check():
            return
        await asyncio.sleep(0.01)
    raise AssertionError("Condition did not become true")


@pytest.mark.redis
async def test_recovery_pages_do_not_starve_new_messages(transport):
    redis, stream = transport
    ids = await seed_pending(redis, stream, 10)
    await make_stale(redis, stream, ids)
    fresh = await redis.xadd(stream, {"value": "new"})
    handler = RecordingHandler()
    runtime = consumer(redis, stream, handler)
    run = asyncio.create_task(runtime.run())
    try:
        await wait_until(lambda: len(handler.messages) == 11)
        assert handler.messages[0].reclaimed
        assert handler.messages[1].message_id == fresh
        assert not handler.messages[1].reclaimed
        assert runtime.is_healthy
    finally:
        await runtime.shutdown()
        await run
    assert not runtime.is_healthy


@pytest.mark.redis
async def test_real_acl_error_fails_fast_instead_of_staying_ready(transport):
    admin, stream = transport
    username = f"compat-{uuid4()}"
    await admin.execute_command(
        "ACL", "SETUSER", username, "on", ">test-only", "~*", "+@all", "-eval"
    )
    restricted = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        username=username,
        password="test-only",
        decode_responses=True,
    )
    runtime = consumer(restricted, stream, RecordingHandler())
    try:
        with pytest.raises(ResponseError):
            await asyncio.wait_for(runtime.run(), 5)
        assert not runtime.is_running and not runtime.is_healthy
    finally:
        await restricted.aclose()
        await admin.execute_command("ACL", "DELUSER", username)


@pytest.mark.redis
async def test_transient_connection_error_marks_unhealthy_then_recovers(
    transport, monkeypatch
):
    redis, stream = transport
    handler = RecordingHandler()
    runtime = consumer(redis, stream, handler)
    run = asyncio.create_task(runtime.run())
    failing = False
    original = redis.eval

    async def sometimes_fails(*args, **kwargs):
        if failing:
            raise ConnectionError("injected transport outage")
        return await original(*args, **kwargs)

    monkeypatch.setattr(redis, "eval", sometimes_fails)
    try:
        await wait_until(lambda: runtime.is_healthy)
        failing = True
        await wait_until(lambda: not runtime.is_healthy)
        assert runtime.is_running
        failing = False
        await redis.xadd(stream, {"value": "after reconnect"})
        await wait_until(lambda: len(handler.messages) == 1)
        await wait_until(lambda: runtime.is_healthy)
    finally:
        await runtime.shutdown()
        await run


@pytest.mark.postgres
@pytest.mark.redis
async def test_runtime_metrics_preserve_unknown_lag(worker):
    for item in worker.consumers:
        await item.initialize()
    await worker.redis.xadd(
        worker.settings.executor_event_stream, {"value": "unread"}
    )
    await worker._metrics()
    metric = worker.telemetry.stream
    assert metric.labels("ingress", "has_unread")._value.get() == 1
    known = metric.labels("ingress", "lag_known")._value.get()
    assert metric.labels("ingress", "lag")._value.get() == (1 if known else -1)
