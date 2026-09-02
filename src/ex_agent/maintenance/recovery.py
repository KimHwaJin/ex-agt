from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from uuid import UUID

from ex_agent.maintenance.store import StreamMaintenanceStore
from ex_agent.transport.stream_maintenance import SafeStreamTrimmer

logger = logging.getLogger(__name__)


class StreamMaintenanceRecovery:
    """Run durable trim jobs only in the background Worker lifecycle."""

    def __init__(
        self,
        store: StreamMaintenanceStore,
        redis,
        *,
        concurrency: int = 2,
        batch_size: int = 10,
        poll_seconds: float = 2,
        claim_timeout_seconds: float = 60,
        max_attempts: int = 5,
        retry_seconds: float = 5,
    ) -> None:
        if (
            min(
                concurrency,
                batch_size,
                poll_seconds,
                claim_timeout_seconds,
                max_attempts,
                retry_seconds,
            )
            <= 0
        ):
            raise ValueError("Maintenance recovery budgets must be positive")
        self._store = store
        self._redis = redis
        self._concurrency = concurrency
        self._batch_size = batch_size
        self._poll_seconds = poll_seconds
        self._claim_timeout_seconds = claim_timeout_seconds
        self._max_attempts = max_attempts
        self._retry_seconds = retry_seconds

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.once()
            except Exception as error:
                logger.warning(
                    "Stream maintenance poll deferred: %s",
                    type(error).__name__,
                )
            try:
                await asyncio.wait_for(stop.wait(), self._poll_seconds)
            except TimeoutError:
                pass

    async def once(self) -> int:
        job_ids = await self._store.due(
            limit=self._batch_size,
            claim_timeout_seconds=self._claim_timeout_seconds,
        )
        semaphore = asyncio.Semaphore(self._concurrency)

        async def execute(job_id: UUID) -> None:
            async with semaphore:
                await self.execute(job_id)

        await asyncio.gather(*(execute(job_id) for job_id in job_ids))
        return len(job_ids)

    async def execute(self, job_id: UUID, *, actor: str = "WORKER") -> None:
        row = await self._store.claim(
            job_id,
            claim_timeout_seconds=self._claim_timeout_seconds,
            actor=actor,
        )
        if row is None:
            return
        try:
            trimmer = SafeStreamTrimmer(
                self._redis,
                retention_seconds=row.retention_seconds,
                minimum_retained_entries=row.minimum_retained_entries,
            )
            outcome = (
                await trimmer.plan(row.stream_key)
                if row.action == "PLAN"
                else await trimmer.trim(row.stream_key)
            )
            result = asdict(outcome)
            if row.action == "TRIM":
                result["result_recalculated_after_retry"] = row.attempts > 1
            await self._store.succeed(
                row.id,
                attempt=row.attempts,
                result=result,
                actor=actor,
            )
        except Exception as error:
            await self._store.fail(
                row.id,
                attempt=row.attempts,
                error=f"{type(error).__name__}: {error}",
                max_attempts=self._max_attempts,
                retry_seconds=self._retry_seconds,
                actor=actor,
            )


__all__ = ["StreamMaintenanceRecovery"]
