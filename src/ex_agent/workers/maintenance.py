from __future__ import annotations

import asyncio
import logging
from time import perf_counter
from typing import Any

from ex_agent.metrics import (
    CHECKPOINT_POOL,
    DELIVERY_BACKLOG,
    OUTBOX_PUBLISHED,
    OUTBOX_RELAY_SECONDS,
    REDIS_STREAM_LAG,
    REDIS_STREAM_PENDING,
    WORKER_RETRIES,
    record_readiness,
    update_database_pool_metrics,
)
from ex_agent.readiness import probe_dependencies
from ex_agent.workers.context import WorkerContext

logger = logging.getLogger(__name__)


class WorkerMaintenance(WorkerContext):
    async def _metrics_loop(self) -> None:
        while True:
            try:
                readiness = await probe_dependencies(
                    self._engine,
                    self._redis,
                    timeout_seconds=(
                        self._settings.readiness_probe_timeout_seconds
                    ),
                )
                self._readiness.update(readiness)
                record_readiness("worker", readiness)
                if readiness.ready:
                    await self._collect_runtime_metrics()
            except Exception:
                logger.exception("Runtime metrics collection failed")
                WORKER_RETRIES.labels(component="metrics").inc()
            await asyncio.sleep(self._settings.worker_metrics_refresh_seconds)

    async def _collect_runtime_metrics(self) -> None:
        update_database_pool_metrics("worker", self._engine)
        for kind, states in {
            "command": ("PENDING", "PUBLISHING"),
            "task_event": ("PENDING", "PUBLISHING"),
        }.items():
            for state in states:
                DELIVERY_BACKLOG.labels(kind=kind, state=state).set(0)
        for (kind, state), count in (
            await self._repository.delivery_backlog_counts()
        ).items():
            DELIVERY_BACKLOG.labels(kind=kind, state=state).set(count)
        await self._set_stream_metrics(
            logical_name="commands",
            stream=self._settings.agent_command_stream,
            group=self._settings.agent_command_consumer_group,
        )
        await self._set_stream_metrics(
            logical_name="executor_events",
            stream=self._settings.executor_event_stream,
            group=self._settings.executor_event_consumer_group,
        )
        if self._checkpoint_pool is not None:
            for stat, value in self._checkpoint_pool.get_stats().items():
                if isinstance(value, int | float):
                    CHECKPOINT_POOL.labels(stat=stat).set(value)

    async def _set_stream_metrics(
        self,
        *,
        logical_name: str,
        stream: str,
        group: str,
    ) -> None:
        pending: Any = await self._redis.xpending(stream, group)
        pending_count = (
            pending.get("pending", 0) if isinstance(pending, dict) else 0
        )
        REDIS_STREAM_PENDING.labels(stream=logical_name).set(pending_count)
        groups: Any = await self._redis.xinfo_groups(stream)
        lag = 0
        for group_info in groups:
            if group_info.get("name") == group:
                lag = group_info.get("lag") or 0
                break
        REDIS_STREAM_LAG.labels(stream=logical_name).set(lag)

    async def _outbox_loop(self) -> None:
        retry_delay = self._settings.worker_retry_initial_seconds
        poll_milliseconds = self._settings.outbox_poll_milliseconds
        while True:
            started_at = perf_counter()
            try:
                published = await self._publisher.publish_pending()
                OUTBOX_PUBLISHED.inc(published)
                retry_delay = self._settings.worker_retry_initial_seconds
                if published:
                    poll_milliseconds = self._settings.outbox_poll_milliseconds
                else:
                    poll_milliseconds = min(
                        poll_milliseconds * 2,
                        self._settings.outbox_idle_max_milliseconds,
                    )
            except Exception:
                logger.exception("Outbox relay iteration failed")
                WORKER_RETRIES.labels(component="outbox").inc()
                await asyncio.sleep(retry_delay)
                retry_delay = self._next_retry_delay(retry_delay)
                continue
            finally:
                OUTBOX_RELAY_SECONDS.observe(perf_counter() - started_at)
            await asyncio.sleep(poll_milliseconds / 1000)

    def _next_retry_delay(self, current: float) -> float:
        return min(current * 2, self._settings.worker_retry_max_seconds)


__all__ = ["WorkerMaintenance"]
