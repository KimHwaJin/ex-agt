from __future__ import annotations

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
        assert fields["source_message_id"] == message_id
        assert fields["reason"] == "invalid test envelope"
        assert json.loads(fields["fields"]) == {"bad": "payload"}
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
