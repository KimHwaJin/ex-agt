from __future__ import annotations

import asyncio
import json
import os
from typing import Any
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from ex_agent.transport import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamMessage,
)

pytestmark = pytest.mark.skipif(
    "TEST_REDIS_URL" not in os.environ,
    reason="Compose Redis is not configured",
)


class InvalidHandler:
    def lock_key(self, message: StreamMessage) -> None:
        del message
        raise PermanentMessageError("invalid test envelope")

    async def handle(self, message: StreamMessage) -> HandlerResult:
        del message
        return HandlerResult(AckDecision.ACK)


class BlockingHandler:
    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    def lock_key(self, message: StreamMessage) -> str:
        return message.fields["lock_key"]

    async def handle(self, message: StreamMessage) -> HandlerResult:
        del message
        self.started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return HandlerResult(AckDecision.ACK)


class RecordingHandler:
    def __init__(self) -> None:
        self.messages: list[StreamMessage] = []

    def lock_key(self, message: StreamMessage) -> str:
        return message.fields["lock_key"]

    async def handle(self, message: StreamMessage) -> HandlerResult:
        self.messages.append(message)
        return HandlerResult(AckDecision.ACK)


class RetryHandler:
    def lock_key(self, message: StreamMessage) -> None:
        del message
        return None

    async def handle(self, message: StreamMessage) -> HandlerResult:
        del message
        return HandlerResult(
            AckDecision.RETRY,
            outcome="dependency_unavailable",
            reason="dependency unavailable",
        )


async def _wait_pending_count(
    redis: Redis,
    stream: str,
    group: str,
    expected: int,
) -> None:
    for _ in range(40):
        pending: Any = await redis.xpending(stream, group)
        if pending["pending"] == expected:
            return
        await asyncio.sleep(0.05)
    raise AssertionError(f"Redis pending count did not reach {expected}")


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_dlq_write_and_source_ack_are_atomic() -> None:
    suffix = uuid4()
    stream = f"test-consumer-source-{suffix}"
    group = f"test-consumer-group-{suffix}"
    dead_letter_stream = f"test-consumer-dlq-{suffix}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    handler = InvalidHandler()
    consumer = RedisStreamConsumer(
        redis,
        RedisStreamConsumerConfig(
            stream=stream,
            group=group,
            consumer_prefix="integration-consumer",
            dead_letter_stream=dead_letter_stream,
        ),
        lambda _: handler,
    )
    try:
        await consumer.ensure_group()
        message_id = await redis.xadd(stream, {"bad": "payload"})
        delivered: Any = await redis.xreadgroup(
            group,
            "integration-consumer-0",
            {stream: ">"},
            count=1,
        )
        assert delivered[0][1][0][0] == message_id

        await consumer.process_message(
            "integration-consumer-0",
            handler,
            StreamMessage(message_id, {"bad": "payload"}),
        )

        pending: Any = await redis.xpending(stream, group)
        dead_letters: Any = await redis.xrange(dead_letter_stream)
        assert pending["pending"] == 0
        assert len(dead_letters) == 1
        fields = dead_letters[0][1]
        assert fields["schema_version"] == "1"
        assert len(fields["failure_id"]) == 64
        assert fields["consumer"] == "integration-consumer-0"
        assert fields["error_type"] == "PermanentMessageError"
        assert fields["reclaimed"] == "false"
        assert fields["dead_lettered_at"]
        assert fields["source_message_id"] == message_id
        assert fields["reason"] == "invalid test envelope"
        assert fields["retry_attempts"] == "0"
        assert json.loads(fields["fields"]) == {"bad": "payload"}
    finally:
        await redis.delete(stream, dead_letter_stream)
        await redis.aclose()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_retry_limit_moves_poison_message_to_dlq() -> None:
    suffix = uuid4()
    stream = f"test-consumer-retry-source-{suffix}"
    group = f"test-consumer-retry-group-{suffix}"
    dead_letter_stream = f"test-consumer-retry-dlq-{suffix}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    handler = RetryHandler()
    consumer = RedisStreamConsumer(
        redis,
        RedisStreamConsumerConfig(
            stream=stream,
            group=group,
            consumer_prefix="integration-consumer",
            dead_letter_stream=dead_letter_stream,
            max_retry_attempts=3,
        ),
        lambda _: handler,
    )
    try:
        await consumer.ensure_group()
        message_id = await redis.xadd(stream, {"job": "poison"})
        delivered: Any = await redis.xreadgroup(
            group,
            "integration-consumer-0",
            {stream: ">"},
            count=1,
        )
        assert delivered[0][1][0][0] == message_id

        for attempt in range(3):
            await consumer.process_message(
                "integration-consumer-0",
                handler,
                StreamMessage(
                    message_id,
                    {"job": "poison"},
                    reclaimed=attempt > 0,
                ),
            )

        pending: Any = await redis.xpending(stream, group)
        dead_letters: Any = await redis.xrange(dead_letter_stream)
        assert pending["pending"] == 0
        assert len(dead_letters) == 1
        assert dead_letters[0][1]["retry_attempts"] == "3"
        retry_key = consumer._retry_state_key(message_id)
        assert await redis.exists(retry_key) == 0
    finally:
        await redis.delete(stream, dead_letter_stream)
        await redis.aclose()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_renews_lock_and_stream_lease_in_one_pipeline() -> (
    None
):
    suffix = uuid4()
    stream = f"test-consumer-renew-source-{suffix}"
    group = f"test-consumer-renew-group-{suffix}"
    lock_key = f"test-consumer-lock-{suffix}"
    consumer_name = "integration-consumer-0"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    handler = InvalidHandler()
    consumer = RedisStreamConsumer(
        redis,
        RedisStreamConsumerConfig(
            stream=stream,
            group=group,
            consumer_prefix="integration-consumer",
        ),
        lambda _: handler,
    )
    try:
        await consumer.ensure_group()
        message_id = await redis.xadd(stream, {"job": "one"})
        delivered: Any = await redis.xreadgroup(
            group,
            consumer_name,
            {stream: ">"},
            count=1,
        )
        assert delivered[0][1][0][0] == message_id
        lock_lease = await consumer._acquire_lock(lock_key)

        assert lock_lease is not None
        await consumer._renew_lease_once(
            consumer_name,
            message_id,
            lock_lease=lock_lease,
        )

        pending: Any = await redis.xpending_range(
            stream,
            group,
            min="-",
            max="+",
            count=1,
        )
        assert await redis.ttl(lock_key) > 0
        assert pending[0]["message_id"] == message_id
        assert pending[0]["consumer"] == consumer_name
    finally:
        await redis.delete(stream, lock_key)
        await redis.aclose()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_shutdown_message_is_reclaimed_by_another_runtime() -> None:
    suffix = uuid4()
    stream = f"test-consumer-reclaim-source-{suffix}"
    group = f"test-consumer-reclaim-group-{suffix}"
    dead_letter_stream = f"test-consumer-reclaim-dlq-{suffix}"
    lock_key = f"test-consumer-reclaim-lock-{suffix}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    first_handler = BlockingHandler()
    second_handler = RecordingHandler()

    def config(prefix: str) -> RedisStreamConsumerConfig:
        return RedisStreamConsumerConfig(
            stream=stream,
            group=group,
            consumer_prefix=prefix,
            block_milliseconds=100,
            claim_idle_milliseconds=1100,
            dead_letter_stream=dead_letter_stream,
            lock_ttl_seconds=2,
            lock_renew_interval_seconds=1,
        )

    first = RedisStreamConsumer(
        redis,
        config("worker-a"),
        lambda _: first_handler,
    )
    second = RedisStreamConsumer(
        redis,
        config("worker-b"),
        lambda _: second_handler,
    )
    first_run: asyncio.Task[None] | None = None
    second_run: asyncio.Task[None] | None = None
    try:
        await first.initialize()
        message_id = await redis.xadd(
            stream,
            {"job": "recover", "lock_key": lock_key},
        )
        first_run = asyncio.create_task(first.run())
        await asyncio.wait_for(first_handler.started.wait(), timeout=2)

        pending: Any = await redis.xpending_range(
            stream,
            group,
            min="-",
            max="+",
            count=1,
        )
        assert pending[0]["message_id"] == message_id
        assert pending[0]["consumer"] == "worker-a-0"
        assert await redis.exists(lock_key) == 1

        await first.shutdown(grace_period_seconds=0)
        await first_run

        assert first_handler.cancelled.is_set() is True
        assert await redis.exists(lock_key) == 0
        await _wait_pending_count(redis, stream, group, 1)

        claimed: Any = await redis.xclaim(
            stream,
            group,
            "orphaned-worker",
            min_idle_time=0,
            message_ids=[message_id],
            idle=5000,
            justid=True,
        )
        assert claimed == [message_id]

        second_run = asyncio.create_task(second.run())
        await _wait_pending_count(redis, stream, group, 0)
        await second.shutdown(grace_period_seconds=1)
        await second_run

        assert len(second_handler.messages) == 1
        assert second_handler.messages[0].message_id == message_id
        assert second_handler.messages[0].reclaimed is True
    finally:
        if first.is_running:
            await first.shutdown(grace_period_seconds=0)
        if second.is_running:
            await second.shutdown(grace_period_seconds=0)
        for task in (first_run, second_run):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first_run, second_run) if task is not None),
            return_exceptions=True,
        )
        await redis.delete(stream, dead_letter_stream, lock_key)
        await redis.aclose()
