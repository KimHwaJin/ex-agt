"""Lightweight API-side access to Worker bindings and session leases."""

from __future__ import annotations

from contextlib import AsyncExitStack
from typing import Self

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from worker.config import Settings
from worker.guard import SessionGuard
from worker.store import Store


class ApiWorkerBridge:
    """Share Worker durability primitives without starting consumers."""

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
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
        self.store = Store(self.pool, settings.namespace)
        self.bindings = self.store
        self.guard = SessionGuard(
            self.redis,
            settings.namespace,
            ttl=settings.lease_ttl_seconds,
            renew_seconds=settings.lease_renew_seconds,
        )
        self._stack = AsyncExitStack()

    async def __aenter__(self) -> Self:
        try:
            self._stack.push_async_callback(self.redis.aclose)
            self._stack.push_async_callback(self.pool.close)
            await self.pool.open(wait=True)
        except BaseException:
            await self._stack.aclose()
            raise
        return self

    async def __aexit__(self, *args) -> None:
        await self._stack.aclose()


__all__ = ["ApiWorkerBridge"]
