from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from time import perf_counter
from typing import Any, Protocol, cast
from uuid import uuid4

from redis.asyncio import Redis
from redis.exceptions import ResponseError

logger = logging.getLogger(__name__)

_RENEW_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""


class AckDecision(StrEnum):
    ACK = "ACK"
    RETRY = "RETRY"
    DEAD_LETTER = "DEAD_LETTER"


@dataclass(frozen=True)
class HandlerResult:
    decision: AckDecision
    outcome: str = "succeeded"
    reason: str | None = None


@dataclass(frozen=True)
class StreamMessage:
    message_id: str
    fields: dict[str, str]
    reclaimed: bool = False


@dataclass(frozen=True)
class _LockLease:
    key: str
    value: str


@dataclass(frozen=True)
class RedisStreamConsumerConfig:
    stream: str
    group: str
    consumer_prefix: str
    concurrency: int = 1
    block_milliseconds: int = 5000
    claim_idle_milliseconds: int = 30000
    claim_batch_size: int = 10
    group_start_id: str = "0"
    dead_letter_stream: str | None = None
    lock_ttl_seconds: int = 60
    lock_renew_interval_seconds: int = 10
    consumer_gc_idle_milliseconds: int | None = None

    def __post_init__(self) -> None:
        if self.concurrency < 1:
            raise ValueError("concurrency must be positive")
        if self.block_milliseconds < 1:
            raise ValueError("block_milliseconds must be positive")
        if self.claim_idle_milliseconds < 1:
            raise ValueError("claim_idle_milliseconds must be positive")
        if self.claim_batch_size < 1:
            raise ValueError("claim_batch_size must be positive")
        if self.lock_ttl_seconds < 1:
            raise ValueError("lock_ttl_seconds must be positive")
        if self.lock_renew_interval_seconds < 1:
            raise ValueError("lock_renew_interval_seconds must be positive")
        if self.lock_renew_interval_seconds >= self.lock_ttl_seconds:
            raise ValueError("lock renewal must be faster than lock expiry")
        if (
            self.lock_renew_interval_seconds * 1000
            >= self.claim_idle_milliseconds
        ):
            raise ValueError("lease renewal must be faster than claim idle")


class PermanentMessageError(ValueError):
    """A malformed or unsupported message that should not be retried."""


class StreamLeaseLostError(RuntimeError):
    pass


class StreamMessageHandler(Protocol):
    def lock_key(self, message: StreamMessage) -> str | None: ...

    async def handle(self, message: StreamMessage) -> HandlerResult: ...


class ConsumerObserver(Protocol):
    def operation_started(self) -> None: ...

    def lock_contended(self) -> None: ...

    def operation_finished(self, outcome: str, duration: float) -> None: ...

    def transport_retry(self) -> None: ...

    def dead_lettered(self) -> None: ...


class NullConsumerObserver:
    def operation_started(self) -> None:
        return

    def lock_contended(self) -> None:
        return

    def operation_finished(self, outcome: str, duration: float) -> None:
        return

    def transport_retry(self) -> None:
        return

    def dead_lettered(self) -> None:
        return


HandlerFactory = Callable[[int], StreamMessageHandler]


class RedisStreamConsumer:
    """Reusable at-least-once Redis Stream consumer runtime."""

    def __init__(
        self,
        redis: Redis,
        config: RedisStreamConsumerConfig,
        handler_factory: HandlerFactory,
        *,
        observer: ConsumerObserver | None = None,
        retry_initial_seconds: float = 0.5,
        retry_max_seconds: float = 30,
    ) -> None:
        self._redis = redis
        self._config = config
        self._handler_factory = handler_factory
        self._observer = observer or NullConsumerObserver()
        self._retry_initial_seconds = retry_initial_seconds
        self._retry_max_seconds = retry_max_seconds

    async def run(self) -> None:
        await self.ensure_group()
        await self.cleanup_consumers()
        await asyncio.gather(
            *(
                self._consume_slot(slot_index)
                for slot_index in range(self._config.concurrency)
            )
        )

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._config.stream,
                self._config.group,
                id=self._config.group_start_id,
                mkstream=True,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def cleanup_consumers(self) -> None:
        minimum_idle = self._config.consumer_gc_idle_milliseconds
        if minimum_idle is None:
            return
        consumers: Any = await self._redis.xinfo_consumers(
            self._config.stream,
            self._config.group,
        )
        current_consumers = {
            f"{self._config.consumer_prefix}-{slot_index}"
            for slot_index in range(self._config.concurrency)
        }
        stale_consumers: list[str] = []
        for consumer in consumers:
            name = _text(consumer["name"])
            if name in current_consumers:
                continue
            if int(consumer.get("pending", 0)) != 0:
                continue
            idle = int(consumer.get("idle", 0))
            if idle < minimum_idle:
                continue
            stale_consumers.append(name)
        if not stale_consumers:
            return
        pipeline = self._redis.pipeline(transaction=False)
        for name in stale_consumers:
            pipeline.xgroup_delconsumer(
                self._config.stream,
                self._config.group,
                name,
            )
        await pipeline.execute()

    async def process_message(
        self,
        consumer: str,
        handler: StreamMessageHandler,
        message: StreamMessage,
    ) -> None:
        started_at = perf_counter()
        outcome = "succeeded"
        lock_lease: _LockLease | None = None
        self._notify(self._observer.operation_started)
        try:
            try:
                lock_key = handler.lock_key(message)
            except PermanentMessageError as error:
                outcome = "dead_lettered"
                await self._dead_letter(message, str(error))
                return
            if lock_key is not None:
                lock_lease = await self._acquire_lock(lock_key)
                if lock_lease is None:
                    outcome = "contended"
                    self._notify(self._observer.lock_contended)
                    return
            try:
                result = await self._run_with_lease(
                    consumer,
                    handler,
                    message,
                    lock_lease=lock_lease,
                )
            except Exception:
                outcome = "failed"
                logger.exception(
                    "Redis Stream message handler failed",
                    extra={
                        "stream": self._config.stream,
                        "group": self._config.group,
                        "message_id": message.message_id,
                    },
                )
                return
            outcome = result.outcome
        finally:
            try:
                if lock_lease is not None:
                    await self._release_lock(lock_lease)
            finally:
                self._notify(
                    self._observer.operation_finished,
                    outcome,
                    perf_counter() - started_at,
                )

    async def _consume_slot(self, slot_index: int) -> None:
        consumer = f"{self._config.consumer_prefix}-{slot_index}"
        handler = self._handler_factory(slot_index)
        retry_delay = self._retry_initial_seconds
        claim_cursor = "0-0"
        while True:
            try:
                claim_cursor, claimed = await self._claim_stale(
                    consumer,
                    claim_cursor,
                )
                if claimed:
                    for message_id, fields in claimed:
                        await self.process_message(
                            consumer,
                            handler,
                            StreamMessage(
                                message_id=message_id,
                                fields=fields,
                                reclaimed=True,
                            ),
                        )
                    retry_delay = self._retry_initial_seconds
                    continue
                if claim_cursor != "0-0":
                    continue
                messages = await self._redis.xreadgroup(
                    self._config.group,
                    consumer,
                    {self._config.stream: ">"},
                    count=1,
                    block=self._config.block_milliseconds,
                )
                for _, entries in messages:
                    for message_id, fields in entries:
                        await self.process_message(
                            consumer,
                            handler,
                            StreamMessage(message_id, fields),
                        )
                retry_delay = self._retry_initial_seconds
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Redis Stream consumer iteration failed",
                    extra={
                        "stream": self._config.stream,
                        "group": self._config.group,
                        "consumer": consumer,
                    },
                )
                self._notify(self._observer.transport_retry)
                await asyncio.sleep(retry_delay)
                retry_delay = min(
                    retry_delay * 2,
                    self._retry_max_seconds,
                )

    async def _claim_stale(
        self,
        consumer: str,
        cursor: str,
    ) -> tuple[str, list[tuple[str, dict[str, str]]]]:
        response: Any = await self._redis.xautoclaim(
            self._config.stream,
            self._config.group,
            consumer,
            min_idle_time=self._config.claim_idle_milliseconds,
            start_id=cursor,
            count=self._config.claim_batch_size,
        )
        return _autoclaim_page(response)

    async def _run_with_lease(
        self,
        consumer: str,
        handler: StreamMessageHandler,
        message: StreamMessage,
        *,
        lock_lease: _LockLease | None,
    ) -> HandlerResult:
        heartbeat = asyncio.create_task(
            self._renew_lease(
                consumer,
                message.message_id,
                lock_lease=lock_lease,
            )
        )
        handler_task = asyncio.create_task(
            self._handle_and_finalize(handler, message)
        )
        try:
            done, _ = await asyncio.wait(
                {heartbeat, handler_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if handler_task in done:
                return await handler_task
            error = heartbeat.exception()
            handler_task.cancel()
            await asyncio.gather(handler_task, return_exceptions=True)
            if error is not None:
                raise error
            raise StreamLeaseLostError(message.message_id)
        finally:
            for task in (heartbeat, handler_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(
                heartbeat,
                handler_task,
                return_exceptions=True,
            )

    async def _renew_lease(
        self,
        consumer: str,
        message_id: str,
        *,
        lock_lease: _LockLease | None,
    ) -> None:
        while True:
            await asyncio.sleep(self._config.lock_renew_interval_seconds)
            await self._renew_lease_once(
                consumer,
                message_id,
                lock_lease=lock_lease,
            )

    async def _renew_lease_once(
        self,
        consumer: str,
        message_id: str,
        *,
        lock_lease: _LockLease | None,
    ) -> None:
        if lock_lease is not None:
            pipeline = self._redis.pipeline(transaction=False)
            pipeline.eval(
                _RENEW_LOCK_SCRIPT,
                1,
                lock_lease.key,
                lock_lease.value,
                str(self._config.lock_ttl_seconds),
            )
            pipeline.xclaim(
                self._config.stream,
                self._config.group,
                consumer,
                min_idle_time=0,
                message_ids=[message_id],
                justid=True,
            )
            renewed, claimed = await pipeline.execute()
            if renewed != 1:
                raise StreamLeaseLostError(lock_lease.key)
        else:
            claimed = await self._redis.xclaim(
                self._config.stream,
                self._config.group,
                consumer,
                min_idle_time=0,
                message_ids=[message_id],
                justid=True,
            )
        if not claimed:
            raise StreamLeaseLostError(message_id)

    async def _handle_and_finalize(
        self,
        handler: StreamMessageHandler,
        message: StreamMessage,
    ) -> HandlerResult:
        try:
            result = await handler.handle(message)
        except PermanentMessageError as error:
            await self._dead_letter(message, str(error))
            return HandlerResult(
                AckDecision.DEAD_LETTER,
                outcome="dead_lettered",
                reason=str(error),
            )
        if result.decision is AckDecision.ACK:
            await self._ack(message.message_id)
        elif result.decision is AckDecision.DEAD_LETTER:
            await self._dead_letter(
                message,
                result.reason or "handler rejected message",
            )
        return result

    async def _acquire_lock(self, lock_key: str) -> _LockLease | None:
        lease = _LockLease(lock_key, str(uuid4()))
        acquired = await self._redis.set(
            lease.key,
            lease.value,
            nx=True,
            ex=self._config.lock_ttl_seconds,
        )
        return lease if acquired else None

    async def _release_lock(self, lease: _LockLease) -> None:
        release = self._redis.eval(
            _RELEASE_LOCK_SCRIPT,
            1,
            lease.key,
            lease.value,
        )
        await cast(Awaitable[Any], release)

    async def _ack(self, message_id: str) -> None:
        await self._redis.xack(
            self._config.stream,
            self._config.group,
            message_id,
        )

    async def _dead_letter(
        self,
        message: StreamMessage,
        reason: str,
    ) -> None:
        dead_letter_stream = self._config.dead_letter_stream
        if dead_letter_stream is None:
            raise RuntimeError("dead-letter stream is not configured")
        pipeline = self._redis.pipeline(transaction=True)
        pipeline.xadd(
            dead_letter_stream,
            {
                "source_stream": self._config.stream,
                "source_group": self._config.group,
                "source_message_id": message.message_id,
                "reason": reason,
                "fields": json.dumps(
                    message.fields,
                    ensure_ascii=False,
                    sort_keys=True,
                ),
            },
        )
        pipeline.xack(
            self._config.stream,
            self._config.group,
            message.message_id,
        )
        await pipeline.execute()
        self._notify(self._observer.dead_lettered)

    def _notify(self, callback: Callable[..., None], *args: Any) -> None:
        try:
            callback(*args)
        except Exception:
            logger.exception(
                "Redis Stream consumer observer failed",
                extra={
                    "stream": self._config.stream,
                    "group": self._config.group,
                },
            )


def _autoclaim_page(
    response: Any,
) -> tuple[str, list[tuple[str, dict[str, str]]]]:
    if not isinstance(response, (list, tuple)) or len(response) < 2:
        raise TypeError("Redis XAUTOCLAIM returned an invalid response")
    cursor = _text(response[0])
    entries = response[1]
    if not isinstance(entries, list):
        raise TypeError("Redis XAUTOCLAIM entries must be a list")
    return cursor, entries


def _text(value: Any) -> str:
    if isinstance(value, bytes):
        return value.decode()
    return str(value)
