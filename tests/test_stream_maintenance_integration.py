from __future__ import annotations

import os
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from ex_agent.transport import SafeStreamTrimmer

pytestmark = pytest.mark.skipif(
    "TEST_REDIS_URL" not in os.environ,
    reason="Compose Redis is not configured",
)


async def _read_ids(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    count: int,
) -> list[str]:
    response: Any = await redis.xreadgroup(
        group,
        consumer,
        {stream: ">"},
        count=count,
    )
    if not response:
        return []
    return [message_id for message_id, _fields in response[0][1]]


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_trim_preserves_slowest_group_and_pending() -> None:
    stream = f"test-safe-trim-{uuid4()}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    trimmer = SafeStreamTrimmer(
        redis,
        retention_seconds=3,
        minimum_retained_entries=1,
    )
    now = datetime.fromtimestamp(10, tz=UTC)
    try:
        for milliseconds in range(1000, 7000, 1000):
            await redis.xadd(
                stream,
                {"value": str(milliseconds)},
                id=f"{milliseconds}-0",
            )
        await redis.xgroup_create(stream, "fast", id="0-0")
        await redis.xgroup_create(stream, "slow", id="0-0")

        fast_ids = await _read_ids(redis, stream, "fast", "fast-0", 10)
        slow_ids = await _read_ids(redis, stream, "slow", "slow-0", 4)
        await redis.xack(stream, "fast", *fast_ids)
        await redis.xack(stream, "slow", *slow_ids[:3])

        plan = await trimmer.plan(stream, now=now)
        first = await trimmer.trim(stream, now=now)

        assert plan.trim_before_id == "4000-0"
        assert first.trim_before_id == "4000-0"
        assert first.removed_entries == 3
        assert [row[0] for row in await redis.xrange(stream)] == [
            "4000-0",
            "5000-0",
            "6000-0",
        ]
        pending: Any = await redis.xpending(stream, "slow")
        assert pending["pending"] == 1
        assert pending["min"] == "4000-0"

        await redis.xack(stream, "slow", slow_ids[-1])
        remaining_ids = await _read_ids(
            redis,
            stream,
            "slow",
            "slow-0",
            10,
        )
        await redis.xack(stream, "slow", *remaining_ids)
        second = await trimmer.trim(stream, now=now)

        assert second.trim_before_id == "6000-0"
        assert second.removed_entries == 2
        assert [row[0] for row in await redis.xrange(stream)] == ["6000-0"]
    finally:
        await redis.delete(stream)
        await redis.aclose()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_trim_is_blocked_by_new_group_at_zero() -> None:
    stream = f"test-safe-trim-new-group-{uuid4()}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    trimmer = SafeStreamTrimmer(
        redis,
        retention_seconds=1,
        minimum_retained_entries=0,
    )
    try:
        await redis.xadd(stream, {"value": "one"}, id="1000-0")
        await redis.xadd(stream, {"value": "two"}, id="2000-0")
        await redis.xgroup_create(stream, "new-reader", id="0-0")

        result = await trimmer.trim(
            stream,
            now=datetime.fromtimestamp(10, tz=UTC),
        )

        assert result.trim_before_id == "0-0"
        assert result.removed_entries == 0
        assert await redis.xlen(stream) == 2
    finally:
        await redis.delete(stream)
        await redis.aclose()
