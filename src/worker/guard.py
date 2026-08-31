from __future__ import annotations

import asyncio
import hashlib
from collections.abc import AsyncIterator, Awaitable
from contextlib import asynccontextmanager, suppress
from typing import Any, cast
from uuid import uuid4

from redis.asyncio import Redis

from worker.consumer import (
    _RELEASE_LOCK_SCRIPT,
    _RENEW_LOCK_SCRIPT,
)
from worker.contracts import DeferEvent


class LeaseLostError(RuntimeError):
    pass


class SessionGuard:
    """Short API/Worker invocation exclusion; not the long chat lock."""

    def __init__(
        self,
        redis: Redis,
        namespace: str,
        *,
        ttl: int = 60,
        renew_seconds: int = 10,
    ) -> None:
        if not 0 < renew_seconds * 2 < ttl:
            raise ValueError("Renewal must precede lock expiry")
        self.redis = redis
        self.namespace = namespace
        self.ttl = ttl
        self.renew_seconds = renew_seconds

    def key(self, session_id: str) -> str:
        digest = hashlib.sha256(session_id.encode()).hexdigest()
        return f"{self.namespace}:session-lock:{digest}"

    @asynccontextmanager
    async def hold(self, session_id: str) -> AsyncIterator[None]:
        key, token = self.key(session_id), str(uuid4())
        acquired = await self.redis.set(key, token, nx=True, ex=self.ttl)
        if not acquired:
            raise DeferEvent("Session is being invoked by another process")
        owner = asyncio.current_task()
        assert owner is not None
        lost = asyncio.Event()

        async def renew() -> None:
            try:
                while True:
                    await asyncio.sleep(self.renew_seconds)
                    async with asyncio.timeout(self.renew_seconds):
                        pending = self.redis.eval(
                            _RENEW_LOCK_SCRIPT,
                            1,
                            key,
                            token,
                            str(self.ttl),
                        )
                        result = await cast(Awaitable[Any], pending)
                    if result != 1:
                        raise LeaseLostError(key)
            except Exception:
                lost.set()
                owner.cancel()

        heartbeat = asyncio.create_task(renew())
        try:
            yield
            if lost.is_set():
                raise LeaseLostError(key)
        except asyncio.CancelledError:
            if lost.is_set():
                raise LeaseLostError(key) from None
            raise
        finally:
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)
            with suppress(Exception):
                async with asyncio.timeout(self.renew_seconds):
                    pending = self.redis.eval(
                        _RELEASE_LOCK_SCRIPT,
                        1,
                        key,
                        token,
                    )
                    await cast(Awaitable[Any], pending)
