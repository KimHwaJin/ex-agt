from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from threading import Lock
from time import perf_counter, time
from typing import Any

from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine


@dataclass(frozen=True)
class DependencyStatus:
    ready: bool
    latency_seconds: float
    error: str | None = None

    def payload(self) -> dict[str, bool | float | str | None]:
        return {
            "ready": self.ready,
            "latency_seconds": self.latency_seconds,
            "error": self.error,
        }


@dataclass(frozen=True)
class ReadinessResult:
    checks: dict[str, DependencyStatus]
    checked_at_epoch_seconds: float

    @property
    def ready(self) -> bool:
        return all(check.ready for check in self.checks.values())

    def payload(
        self,
        *,
        stale_after_seconds: float | None = None,
    ) -> dict[str, Any]:
        age_seconds = max(0.0, time() - self.checked_at_epoch_seconds)
        stale = (
            stale_after_seconds is not None
            and age_seconds > stale_after_seconds
        )
        return {
            "status": "ready" if self.ready and not stale else "unready",
            "ready": self.ready and not stale,
            "stale": stale,
            "age_seconds": age_seconds,
            "checked_at_epoch_seconds": self.checked_at_epoch_seconds,
            "checks": {
                name: check.payload() for name, check in self.checks.items()
            },
        }

    @classmethod
    def starting(cls) -> ReadinessResult:
        return cls.unavailable("starting")

    @classmethod
    def stopping(cls) -> ReadinessResult:
        return cls.unavailable("stopping")

    @classmethod
    def unavailable(cls, reason: str) -> ReadinessResult:
        checks = {
            name: DependencyStatus(
                ready=False,
                latency_seconds=0,
                error=reason,
            )
            for name in ("postgres", "redis")
        }
        return cls(checks=checks, checked_at_epoch_seconds=time())


class ReadinessState:
    def __init__(self) -> None:
        self._lock = Lock()
        self._result = ReadinessResult.starting()

    def update(self, result: ReadinessResult) -> None:
        with self._lock:
            self._result = result

    def payload(self, stale_after_seconds: float) -> dict[str, Any]:
        with self._lock:
            result = self._result
        return result.payload(stale_after_seconds=stale_after_seconds)


async def probe_dependencies(
    engine: AsyncEngine,
    redis: Redis,
    *,
    timeout_seconds: float,
) -> ReadinessResult:
    async def postgres_probe() -> None:
        async with engine.connect() as connection:
            await connection.execute(text("SELECT 1"))

    async def redis_probe() -> None:
        if not await redis.ping():
            raise ConnectionError("Redis PING did not return PONG")

    postgres, redis_status = await asyncio.gather(
        _probe(postgres_probe, timeout_seconds=timeout_seconds),
        _probe(redis_probe, timeout_seconds=timeout_seconds),
    )
    return ReadinessResult(
        checks={"postgres": postgres, "redis": redis_status},
        checked_at_epoch_seconds=time(),
    )


async def _probe(
    operation: Callable[[], Awaitable[None]],
    *,
    timeout_seconds: float,
) -> DependencyStatus:
    started_at = perf_counter()
    try:
        async with asyncio.timeout(timeout_seconds):
            await operation()
    except Exception as error:
        return DependencyStatus(
            ready=False,
            latency_seconds=perf_counter() - started_at,
            error=type(error).__name__,
        )
    return DependencyStatus(
        ready=True,
        latency_seconds=perf_counter() - started_at,
    )
