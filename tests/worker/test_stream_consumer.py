from __future__ import annotations

import asyncio
from typing import Any, cast

import pytest

from worker.consumer import (
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

    def delete(self, key: str) -> None:
        self.commands.append(("delete", (key,)))

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
            for name, args in self.commands:
                if name == "delete":
                    await self._redis.delete(*args)
                elif name == "xack":
                    await self._redis.xack(*args)
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
        self.retry_attempts: dict[str, int] = {}

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
        del key_count
        if "incr" in script:
            del value, args
            attempts = self.retry_attempts.get(key, 0) + 1
            self.retry_attempts[key] = attempts
            return attempts
        del args
        if "del" in script and self.locks.get(key) == value:
            del self.locks[key]
            return 1
        return int(self.locks.get(key) == value)

    async def delete(self, key: str) -> int:
        existed = key in self.retry_attempts
        self.retry_attempts.pop(key, None)
        return int(existed)

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


class RuntimeRedis(FakeRedis):
    def __init__(
        self,
        messages: list[tuple[str, dict[str, str]]] | None = None,
    ) -> None:
        super().__init__()
        self.messages = messages or []
        self.group_create_calls = 0
        self.read_started = asyncio.Event()

    async def xgroup_create(
        self,
        stream: str,
        group: str,
        *,
        id: str,
        mkstream: bool,
    ) -> bool:
        del stream, group, id, mkstream
        self.group_create_calls += 1
        return True

    async def xreadgroup(
        self,
        group: str,
        consumer: str,
        streams: dict[str, str],
        *,
        count: int,
        block: int,
    ) -> list[tuple[str, list[tuple[str, dict[str, str]]]]]:
        del group, consumer, count, block
        self.read_started.set()
        if self.messages:
            return [(next(iter(streams)), [self.messages.pop(0)])]
        await asyncio.Event().wait()
        return []


class Handler:
    def __init__(
        self,
        result: HandlerResult,
        *,
        lock_key: str | None = "lock:one",
        permanent_error: str | None = None,
        handle_error: str | None = None,
        transient_error: str | None = None,
    ) -> None:
        self._result = result
        self._lock_key = lock_key
        self._permanent_error = permanent_error
        self._handle_error = handle_error
        self._transient_error = transient_error
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
        if self._transient_error is not None:
            raise RuntimeError(self._transient_error)
        return self._result


class BlockingHandler:
    def __init__(self, *, lock_key: str | None) -> None:
        self._lock_key = lock_key
        self.started = asyncio.Event()
        self.allowed = asyncio.Event()
        self.cancelled = asyncio.Event()

    def lock_key(self, message: StreamMessage) -> str | None:
        del message
        return self._lock_key

    async def handle(self, message: StreamMessage) -> HandlerResult:
        del message
        self.started.set()
        try:
            await self.allowed.wait()
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return HandlerResult(AckDecision.ACK)


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
    assert list(redis.retry_attempts.values()) == [1]


@pytest.mark.asyncio
async def test_retry_limit_moves_message_to_dlq() -> None:
    redis = FakeRedis()
    handler = Handler(
        HandlerResult(
            AckDecision.RETRY,
            outcome="dependency_unavailable",
            reason="dependency unavailable",
        )
    )
    consumer = _consumer(redis, handler, max_retry_attempts=2)
    message = StreamMessage("2-0", {"job": "one"})

    await consumer.process_message("consumer-0", handler, message)
    await consumer.process_message(
        "consumer-0",
        handler,
        StreamMessage(message.message_id, message.fields, reclaimed=True),
    )

    assert handler.calls == 2
    assert redis.acknowledged == ["2-0"]
    assert redis.retry_attempts == {}
    commands = redis.transactions[-1]
    assert [name for name, _ in commands] == ["delete", "xadd", "xack"]
    assert commands[1][1][1]["retry_attempts"] == "2"


@pytest.mark.asyncio
async def test_retryable_handler_exception_uses_retry_budget() -> None:
    redis = FakeRedis()
    handler = Handler(
        HandlerResult(AckDecision.ACK),
        transient_error="dependency timeout",
    )
    consumer = _consumer(redis, handler, max_retry_attempts=1)

    await consumer.process_message(
        "consumer-0",
        handler,
        StreamMessage("3-0", {"job": "one"}),
    )

    assert handler.calls == 1
    assert redis.acknowledged == ["3-0"]
    commands = redis.transactions[-1]
    assert commands[1][1][1]["reason"] == ("RuntimeError: dependency timeout")


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
    assert [name for name, _ in commands] == ["delete", "xadd", "xack"]
    assert commands[1][1][0] == "source.dlq"
    assert commands[2][1] == ("source", "group", "9-0")


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
    assert redis.transactions[0][1][1][1]["reason"] == "unsupported job"
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
    assert redis.retry_attempts == {}


@pytest.mark.asyncio
async def test_reclaimed_success_clears_retry_state_with_ack() -> None:
    redis = FakeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler)
    retry_key = consumer._retry_state_key("4-0")
    redis.retry_attempts[retry_key] = 1

    await consumer.process_message(
        "consumer-0",
        handler,
        StreamMessage("4-0", {"job": "recovered"}, reclaimed=True),
    )

    assert redis.acknowledged == ["4-0"]
    assert redis.retry_attempts == {}
    assert [name for name, _ in redis.transactions[-1]] == [
        "delete",
        "xack",
    ]


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


@pytest.mark.asyncio
async def test_initialize_runs_group_setup_only_once() -> None:
    redis = RuntimeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler)

    await consumer.initialize()
    await consumer.initialize()

    assert redis.group_create_calls == 1


@pytest.mark.asyncio
async def test_shutdown_cancels_blocked_reader_after_grace_period() -> None:
    redis = RuntimeRedis()
    handler = Handler(
        HandlerResult(AckDecision.ACK),
        lock_key=None,
    )
    consumer = _consumer(redis, handler)
    running = asyncio.create_task(consumer.run())

    await asyncio.wait_for(redis.read_started.wait(), timeout=0.1)
    assert consumer.is_running is True
    await consumer.shutdown(grace_period_seconds=0)
    await asyncio.wait_for(running, timeout=0.1)

    assert consumer.is_running is False


@pytest.mark.asyncio
async def test_stop_requested_before_run_is_not_lost() -> None:
    redis = RuntimeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler)

    consumer.request_stop()
    await consumer.run()

    assert redis.group_create_calls == 0
    assert redis.read_started.is_set() is False
    assert consumer.is_running is False


@pytest.mark.asyncio
async def test_external_run_cancellation_remains_observable() -> None:
    redis = RuntimeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))
    consumer = _consumer(redis, handler)
    running = asyncio.create_task(consumer.run())

    await asyncio.wait_for(redis.read_started.wait(), timeout=0.1)
    running.cancel()

    with pytest.raises(asyncio.CancelledError):
        await running
    assert consumer.is_running is False


@pytest.mark.asyncio
async def test_shutdown_drains_active_handler_within_grace_period() -> None:
    redis = RuntimeRedis([("20-0", {"job": "one"})])
    handler = BlockingHandler(lock_key=None)
    consumer = RedisStreamConsumer(
        cast(Any, redis),
        RedisStreamConsumerConfig(
            stream="source",
            group="group",
            consumer_prefix="instance-command",
            dead_letter_stream="source.dlq",
        ),
        lambda _: handler,
    )
    running = asyncio.create_task(consumer.run())

    await asyncio.wait_for(handler.started.wait(), timeout=0.1)
    stopping = asyncio.create_task(consumer.shutdown(grace_period_seconds=1))
    await asyncio.sleep(0)
    assert stopping.done() is False
    handler.allowed.set()
    await asyncio.wait_for(stopping, timeout=0.1)
    await asyncio.wait_for(running, timeout=0.1)

    assert redis.acknowledged == ["20-0"]
    assert handler.cancelled.is_set() is False


@pytest.mark.asyncio
async def test_shutdown_timeout_leaves_message_pending_for_recovery() -> None:
    redis = RuntimeRedis([("21-0", {"job": "slow"})])
    handler = BlockingHandler(lock_key="lock:slow")
    consumer = RedisStreamConsumer(
        cast(Any, redis),
        RedisStreamConsumerConfig(
            stream="source",
            group="group",
            consumer_prefix="instance-command",
            dead_letter_stream="source.dlq",
        ),
        lambda _: handler,
    )
    running = asyncio.create_task(consumer.run())

    await asyncio.wait_for(handler.started.wait(), timeout=0.1)
    await consumer.shutdown(grace_period_seconds=0)
    await asyncio.wait_for(running, timeout=0.1)

    assert handler.cancelled.is_set() is True
    assert redis.acknowledged == []
    assert redis.locks == {}


@pytest.mark.asyncio
async def test_shutdown_rejects_negative_grace_period() -> None:
    redis = RuntimeRedis()
    handler = Handler(HandlerResult(AckDecision.ACK))

    with pytest.raises(ValueError, match="cannot be negative"):
        await _consumer(redis, handler).shutdown(-1)


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


def test_consumer_config_rejects_retry_state_expiring_too_early() -> None:
    with pytest.raises(ValueError, match="retry state TTL"):
        RedisStreamConsumerConfig(
            stream="source",
            group="group",
            consumer_prefix="consumer",
            max_retry_attempts=5,
            retry_state_ttl_seconds=150,
        )
