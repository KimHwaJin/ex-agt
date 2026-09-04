from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Mapping
from contextlib import AsyncExitStack, suppress
from typing import Self

import httpx
from prometheus_client import generate_latest
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from worker.config import Settings
from worker.consumer import (
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
)
from worker.contracts import EventHandler
from worker.dispatcher import Dispatcher
from worker.guard import SessionGuard
from worker.ingress import EventRouter, Ingress
from worker.outbox import Outbox
from worker.store import Store
from worker.telemetry import Telemetry

logger = logging.getLogger(__name__)


class ExecutorWorker:
    def __init__(
        self,
        settings: Settings,
        handlers: Mapping[str, EventHandler],
    ) -> None:
        self.settings = settings
        self.handlers = dict(handlers)
        self.pool = AsyncConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=settings.pool_size,
            open=False,
        )
        self.redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_connect_timeout=settings.request_timeout_seconds,
            socket_timeout=max(10, settings.request_timeout_seconds),
        )
        self.http = httpx.AsyncClient(
            base_url=settings.executor_base_url.rstrip("/") + "/",
            timeout=settings.request_timeout_seconds,
        )
        self.store = Store(self.pool, settings.namespace)
        self.bindings = self.store
        self.guard = SessionGuard(
            self.redis,
            settings.namespace,
            ttl=settings.lease_ttl_seconds,
            renew_seconds=settings.lease_renew_seconds,
        )
        self.telemetry = Telemetry()
        self.router = EventRouter(
            self.store,
            self.http,
            set(handlers),
            batch_size=settings.batch_size,
            concurrency=settings.ingress_workers,
        )
        self.outbox = Outbox(
            self.store,
            self.redis,
            settings.command_stream,
            batch_size=settings.batch_size,
            lease_seconds=settings.publish_lease_seconds,
        )
        self.ingress = Ingress(self.store)
        self.dispatcher = Dispatcher(
            self.store,
            self.guard,
            self.handlers,
            max_attempts=settings.max_handler_attempts,
        )
        self.consumers = [
            self._consumer(
                "ingress",
                settings.executor_event_stream,
                settings.event_group,
                lambda _: self.ingress,
                settings.ingress_workers,
            ),
            self._consumer(
                "dispatch",
                settings.command_stream,
                settings.command_group,
                lambda _: self.dispatcher,
                settings.dispatch_workers,
            ),
        ]
        self._readiness_checks: dict[str, Callable[[], Awaitable[bool]]] = {}
        self._stop = asyncio.Event()
        self._running = False
        self._stack = AsyncExitStack()

    def _consumer(self, kind, stream, group, factory, concurrency):
        settings = self.settings
        return RedisStreamConsumer(
            self.redis,
            RedisStreamConsumerConfig(
                stream=stream,
                group=group,
                consumer_prefix=f"{settings.instance_id}-{kind}",
                concurrency=concurrency,
                block_milliseconds=1000,
                claim_idle_milliseconds=settings.claim_idle_milliseconds,
                claim_batch_size=settings.batch_size,
                dead_letter_stream=f"{settings.namespace}:{kind}:dlq",
                lock_ttl_seconds=settings.lease_ttl_seconds,
                lock_renew_interval_seconds=settings.lease_renew_seconds,
                consumer_gc_idle_milliseconds=86400000,
                retry_state_ttl_seconds=604800,
                retry_key_prefix=f"{settings.namespace}:transport-retries",
            ),
            factory,
            observer=self.telemetry.observer(kind),
        )

    async def __aenter__(self) -> Self:
        try:
            self._stack.push_async_callback(self.redis.aclose)
            self._stack.push_async_callback(self.http.aclose)
            self._stack.push_async_callback(self.pool.close)
            await self.pool.open(wait=True)
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(self, *args) -> None:
        await self._stack.aclose()

    def request_stop(self) -> None:
        self._stop.set()
        for consumer in self.consumers:
            consumer.request_stop()

    def add_readiness_check(
        self,
        name: str,
        check: Callable[[], Awaitable[bool]],
    ) -> None:
        if not name.strip() or not callable(check):
            raise ValueError("Readiness check must have a name and callable")
        if self._running:
            raise RuntimeError("Readiness checks must be added before run")
        if name in self._readiness_checks:
            raise ValueError(f"Duplicate readiness check: {name}")
        self._readiness_checks[name] = check

    async def run(self) -> None:
        if self._running:
            raise RuntimeError("Worker already running")
        if self._stop.is_set():
            return
        self._running = True
        server = None
        tasks: list[asyncio.Task] = []
        stopper = asyncio.create_task(self._stop.wait())
        try:
            if self.settings.health_port:
                server = await asyncio.start_server(
                    self._health,
                    "0.0.0.0",
                    self.settings.health_port,
                    limit=8192,
                )
            tasks = [asyncio.create_task(c.run()) for c in self.consumers]
            tasks += [
                asyncio.create_task(self._loop(self.router.once)),
                asyncio.create_task(self._loop(self.outbox.once)),
                asyncio.create_task(self._loop(self._metrics, interval=10)),
            ]
            done, _ = await asyncio.wait(
                [*tasks, stopper],
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in done:
                if task is not stopper:
                    await task
                    if not self._stop.is_set():
                        raise RuntimeError("Worker loop stopped unexpectedly")
        finally:
            self.request_stop()
            await asyncio.gather(
                *(
                    c.shutdown(self.settings.shutdown_seconds)
                    for c in self.consumers
                )
            )
            for task in [*tasks, stopper]:
                task.cancel()
            await asyncio.gather(*tasks, stopper, return_exceptions=True)
            if server is not None:
                server.close()
                await server.wait_closed()
            self._running = False

    async def _loop(
        self,
        operation: Callable[[], Awaitable[int]],
        *,
        interval: float | None = None,
    ) -> None:
        delay = interval or self.settings.poll_seconds
        while not self._stop.is_set():
            try:
                count = await operation()
                delay = interval or (
                    self.settings.poll_seconds
                    if count
                    else min(delay * 2, self.settings.idle_poll_seconds)
                )
            except Exception:
                logger.exception("Worker maintenance iteration failed")
                delay = min(max(delay * 2, 0.5), 30)
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), delay)

    async def _metrics(self) -> int:
        counts = await self.store.counts()
        self.telemetry.backlog.clear()
        for state, count in counts.items():
            self.telemetry.backlog.labels(state).set(count)
        for kind, stream, group in (
            (
                "ingress",
                self.settings.executor_event_stream,
                self.settings.event_group,
            ),
            (
                "dispatch",
                self.settings.command_stream,
                self.settings.command_group,
            ),
        ):
            for entry in await self.redis.xinfo_groups(stream):
                if entry["name"] == group:
                    for metric in ("pending", "lag"):
                        self.telemetry.stream.labels(kind, metric).set(
                            entry.get(metric) or 0,
                        )
        return 0

    async def ready(self) -> bool:
        if self._stop.is_set() or not all(
            c.is_running for c in self.consumers
        ):
            return False
        try:
            async with asyncio.timeout(2):
                await self.redis.ping()
                async with self.pool.connection() as conn:
                    await conn.execute("SELECT 1 FROM ew_bindings LIMIT 0")
                if self._readiness_checks:
                    results = await asyncio.gather(
                        *(
                            check()
                            for check in self._readiness_checks.values()
                        ),
                        return_exceptions=True,
                    )
                    if not all(result is True for result in results):
                        return False
            return True
        except Exception:
            return False

    async def _health(self, reader, writer) -> None:
        try:
            async with asyncio.timeout(3):
                request = await reader.readuntil(b"\r\n\r\n")
                path = request.split(b" ", 2)[1]
                status, body = "200 OK", b"ok"
                if path == b"/health/ready":
                    if not await self.ready():
                        status, body = "503 Unavailable", b"not ready"
                elif path == b"/metrics":
                    body = generate_latest(self.telemetry.registry)
                elif path != b"/health/live":
                    status, body = "404 Not Found", b"not found"
                writer.write(
                    f"HTTP/1.1 {status}\r\nConnection: close\r\n"
                    f"Content-Length: {len(body)}\r\n"
                    "Content-Type: text/plain; charset=utf-8\r\n\r\n".encode()
                    + body
                )
                await writer.drain()
        except (
            TimeoutError,
            ValueError,
            IndexError,
            asyncio.IncompleteReadError,
            asyncio.LimitOverrunError,
        ):
            pass
        finally:
            writer.close()
            await writer.wait_closed()
