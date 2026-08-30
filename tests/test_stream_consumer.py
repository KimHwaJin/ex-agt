from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from ex_agent.transport.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamMessage,
    _autoclaim_page,
)


class FakePipeline:
    def __init__(self, redis: FakeRedis, *, transaction: bool) -> None:
        self._redis = redis
        self._transaction = transaction
        self.commands: list[tuple[str, tuple[Any, ...]]] = []

    def xadd(self, stream: str, fields: dict[str, str]) -> None:
        self.commands.append(("xadd", (stream, fields)))

    def xack(self, stream: str, group: str, message_id: str) -> None:
        self.commands.append(("xack", (stream, group, message_id)))

    def eval(
        self,
        script: str,
        key_count: int,
        key: str,
        value: str,
        *args: str,
    ) -> None:
        self.commands.append(("eval", (script, key_count, key, value, *args)))

    def xclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        message_ids: list[str],
        justid: bool,
    ) -> None:
        self.commands.append(
            (
                "xclaim",
                (
                    stream,
                    group,
                    consumer,
                    min_idle_time,
                    message_ids,
                    justid,
                ),
            )
        )

    def xgroup_delconsumer(
        self,
        stream: str,
        group: str,
        name: str,
    ) -> None:
        self.commands.append(("xgroup_delconsumer", (stream, group, name)))

    async def execute(self) -> list[Any]:
        self._redis.pipeline_modes.append(self._transaction)
        if self._transaction:
            self._redis.transactions.append(self.commands)
            return [1] * len(self.commands)
        results: list[Any] = []
        for name, args in self.commands:
            if name == "eval":
                results.append(await self._redis.eval(*args))
            elif name == "xclaim":
                claimed = await self._redis.xclaim(
                    args[0],
                    args[1],
                    args[2],
                    min_idle_time=args[3],
                    message_ids=args[4],
                    justid=args[5],
                )
                results.append(claimed)
            elif name == "xgroup_delconsumer":
                results.append(await self._redis.xgroup_delconsumer(*args))
        return results


class FakeRedis:
    def __init__(self) -> None:
        self.locks: dict[str, str] = {}
        self.acknowledged: list[str] = []
        self.transactions: list[list[tuple[str, tuple[Any, ...]]]] = []
        self.pipeline_modes: list[bool] = []
        self.consumers: list[dict[str, Any]] = []
        self.deleted_consumers: list[str] = []
        self.autoclaim_response: Any = ["0-0", [], []]
        self.autoclaim_calls: list[dict[str, Any]] = []
        self.xclaim_calls: list[dict[str, Any]] = []

    async def set(
        self,
        key: str,
        value: str,
        *,
        nx: bool,
        ex: int,
    ) -> bool:
        del nx, ex
        if key in self.locks:
            return False
        self.locks[key] = value
        return True

    async def eval(
        self,
        script: str,
        key_count: int,
        key: str,
        value: str,
        *args: str,
    ) -> int:
        del key_count, args
        if "del" in script and self.locks.get(key) == value:
            del self.locks[key]
            return 1
        return int(self.locks.get(key) == value)

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        del stream, group
        self.acknowledged.append(message_id)
        return 1

    def pipeline(self, *, transaction: bool) -> FakePipeline:
        return FakePipeline(self, transaction=transaction)

    async def xinfo_consumers(
        self,
        stream: str,
        group: str,
    ) -> list[dict[str, Any]]:
        del stream, group
        return self.consumers

    async def xgroup_delconsumer(
        self,
        stream: str,
        group: str,
        name: str,
    ) -> int:
        del stream, group
        self.deleted_consumers.append(name)
        return 1

    async def xautoclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        start_id: str,
        count: int,
    ) -> Any:
        self.autoclaim_calls.append(
            {
                "stream": stream,
                "group": group,
                "consumer": consumer,
                "min_idle_time": min_idle_time,
                "start_id": start_id,
                "count": count,
            }
        )
        return self.autoclaim_response

    async def xclaim(
        self,
        stream: str,
        group: str,
        consumer: str,
        *,
        min_idle_time: int,
        message_ids: list[str],
        justid: bool,
    ) -> list[str]:
        self.xclaim_calls.append(
            {
                "stream": stream,
                "group": group,
                "consumer": consumer,
                "min_idle_time": min_idle_time,
                "message_ids": message_ids,
                "justid": justid,
            }
        )
        return message_ids


class DelayedAckRedis(FakeRedis):
    def __init__(self) -> None:
        super().__init__()
        self.ack_started = asyncio.Event()
        self.ack_allowed = asyncio.Event()

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.ack_started.set()
        await self.ack_allowed.wait()
        return await super().xack(stream, group, message_id)


class Handler:
    def __init__(
        self,
        result: HandlerResult,
        *,
        lock_key: str | None = "lock:one",
        permanent_error: str | None = None,
        handle_error: str | None = None,
    ) -> None:
        self._result = result
        self._lock_key = lock_key
        self._permanent_error = permanent_error
        self._handle_error = handle_error
        self.calls = 0

    def lock_key(self, message: StreamMessage) -> str | None:
        del message
        if self._permanent_error is not None:
            raise PermanentMessageError(self._permanent_error)
        return self._lock_key

    async def handle(self, message: StreamMessage) -> HandlerResult:
        del message
        self.calls += 1
        if self._handle_error is not None:
            raise PermanentMessageError(self._handle_error)
        return self._result


def _consumer(
    redis: FakeRedis,
    handler: Handler,
    **overrides: Any,
) -> RedisStreamConsumer:
    config = RedisStreamConsumerConfig(
        stream="source",
        group="group",
        consumer_prefix="instance-command",
        dead_letter_stream="source.dlq",
        **overrides,
    )
    return RedisStreamConsumer(
        cast(Any, redis),
        config,
        lambda _: handler,
    )


@pytest.mark.asyncio
async def test_acknowledges_only_after_handler_success() -> None:
    redis = FakeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))

    await _consumer(redis, handler).process_message(
        "consumer-0",
        handler,
        StreamMessage("1-0", {"value": "one"}),
    )

    assert handler.calls == 1
    assert redis.acknowledged == ["1-0"]
    assert redis.locks == {}


@pytest.mark.asyncio
async def test_retry_decision_leaves_message_pending() -> None:
    redis = FakeRedis()
    handler = Handler(HandlerResult(AckDecision.RETRY, outcome="deferred"))

    await _consumer(redis, handler).process_message(
        "consumer-0",
        handler,
        StreamMessage("1-0", {}),
    )

    assert redis.acknowledged == []
    assert redis.transactions == []


@pytest.mark.asyncio
async def test_permanent_message_error_moves_to_dlq_and_acks_atomically() -> (
    None
):
    redis = FakeRedis()
    handler = Handler(
        HandlerResult(AckDecision.ACK),
        permanent_error="invalid envelope",
    )

    await _consumer(redis, handler).process_message(
        "consumer-0",
        handler,
        StreamMessage("9-0", {"bad": "payload"}),
    )

    assert handler.calls == 0
    assert len(redis.transactions) == 1
    commands = redis.transactions[0]
    assert [name for name, _ in commands] == ["xadd", "xack"]
    assert commands[0][1][0] == "source.dlq"
    assert commands[1][1] == ("source", "group", "9-0")


@pytest.mark.asyncio
async def test_handler_permanent_error_is_finalized_while_lock_is_held() -> (
    None
):
    redis = FakeRedis()
    handler = Handler(
        HandlerResult(AckDecision.ACK),
        handle_error="unsupported job",
    )

    await _consumer(redis, handler).process_message(
        "consumer-0",
        handler,
        StreamMessage("10-0", {"job": "unsupported"}),
    )

    assert handler.calls == 1
    assert len(redis.transactions) == 1
    assert redis.transactions[0][0][1][1]["reason"] == "unsupported job"
    assert redis.locks == {}


@pytest.mark.asyncio
async def test_lock_contention_does_not_process_or_ack() -> None:
    redis = FakeRedis()
    redis.locks["lock:one"] = "other-owner"
    handler = Handler(HandlerResult(AckDecision.ACK))

    await _consumer(redis, handler).process_message(
        "consumer-0",
        handler,
        StreamMessage("1-0", {}),
    )

    assert handler.calls == 0
    assert redis.acknowledged == []
    assert redis.locks == {"lock:one": "other-owner"}


@pytest.mark.asyncio
async def test_cleanup_deletes_only_idle_empty_foreign_consumers() -> None:
    redis = FakeRedis()
    redis.consumers = [
        {"name": b"old-empty", "pending": 0, "idle": 100_000},
        {"name": "old-pending", "pending": 1, "idle": 100_000},
        {"name": "old-active", "pending": 0, "idle": 50},
        {
            "name": "instance-command-0",
            "pending": 0,
            "idle": 100_000,
        },
    ]
    handler = Handler(HandlerResult(AckDecision.ACK))

    await _consumer(
        redis,
        handler,
        consumer_gc_idle_milliseconds=60_000,
    ).cleanup_consumers()

    assert redis.deleted_consumers == ["old-empty"]
    assert redis.pipeline_modes == [False]


@pytest.mark.asyncio
async def test_autoclaim_uses_supplied_cursor_and_configured_batch() -> None:
    redis = FakeRedis()
    redis.autoclaim_response = [b"42-0", [], []]
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler, claim_batch_size=25)

    cursor, entries = await consumer._claim_stale(
        "instance-command-0",
        "10-0",
    )

    assert cursor == "42-0"
    assert entries == []
    assert redis.autoclaim_calls[0]["start_id"] == "10-0"
    assert redis.autoclaim_calls[0]["count"] == 25


@pytest.mark.asyncio
async def test_lock_and_stream_lease_renew_in_one_pipeline() -> None:
    redis = FakeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler)
    lock_lease = await consumer._acquire_lock("lock:one")

    assert lock_lease is not None
    await consumer._renew_lease_once(
        "instance-command-0",
        "11-0",
        lock_lease=lock_lease,
    )

    assert redis.pipeline_modes == [False]
    assert redis.xclaim_calls == [
        {
            "stream": "source",
            "group": "group",
            "consumer": "instance-command-0",
            "min_idle_time": 0,
            "message_ids": ["11-0"],
            "justid": True,
        }
    ]


@pytest.mark.asyncio
async def test_stream_lease_covers_ack_finalization() -> None:
    redis = DelayedAckRedis()
    handler = Handler(
        HandlerResult(AckDecision.ACK),
        lock_key=None,
    )
    consumer = _consumer(redis, handler)
    heartbeat_during_ack = asyncio.Event()

    async def observe_lease_during_ack(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        await redis.ack_started.wait()
        heartbeat_during_ack.set()
        await asyncio.Event().wait()

    cast(Any, consumer)._renew_lease = observe_lease_during_ack
    processing = asyncio.create_task(
        consumer.process_message(
            "consumer-0",
            handler,
            StreamMessage("12-0", {}),
        )
    )
    try:
        await asyncio.wait_for(redis.ack_started.wait(), timeout=0.1)
        await asyncio.wait_for(heartbeat_during_ack.wait(), timeout=0.1)
        redis.ack_allowed.set()
        await asyncio.wait_for(processing, timeout=0.1)
    finally:
        redis.ack_allowed.set()
        if not processing.done():
            processing.cancel()
        await asyncio.gather(processing, return_exceptions=True)

    assert redis.acknowledged == ["12-0"]


def test_autoclaim_page_rejects_invalid_shape() -> None:
    with pytest.raises(TypeError, match="invalid response"):
        _autoclaim_page([])


def test_consumer_config_rejects_unsafe_lease_timing() -> None:
    with pytest.raises(ValueError, match="claim idle"):
        RedisStreamConsumerConfig(
            stream="source",
            group="group",
            consumer_prefix="consumer",
            claim_idle_milliseconds=10_000,
            lock_renew_interval_seconds=10,
        )
