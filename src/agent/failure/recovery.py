"""Discover durable failures with bounded keyset scans; no extra broker."""

import asyncio
import logging
from uuid import UUID

from agent.failure.service import FailureService
from worker.store import Store

logger = logging.getLogger(__name__)


class FailureRecovery:
    def __init__(
        self,
        service: FailureService,
        worker_store: Store | None = None,
        *,
        batch_size: int = 32,
        concurrency: int = 4,
        poll_seconds: float = 5,
    ) -> None:
        if (
            not 1 <= batch_size <= 100
            or not 1 <= concurrency <= 32
            or poll_seconds <= 0
        ):
            raise ValueError("Invalid failure recovery budget")
        self.service, self.worker_store = service, worker_store
        self.batch_size, self.concurrency = batch_size, concurrency
        self.poll_seconds = poll_seconds
        self.api_cursor: UUID | None = None
        self.worker_cursor: tuple[UUID, int] | None = None

    async def once(self) -> None:
        # Cursors wrap, so a newly FAILED item before the cursor is found on
        # the next pass. Persistent failures cannot starve later failures.
        ids = await self.service.requests.blocked_page(
            after=self.api_cursor, limit=self.batch_size
        )
        for request_id in ids:
            await self._safe(self.service.capture_api, request_id)
        self.api_cursor = ids[-1] if len(ids) == self.batch_size else None
        if self.worker_store:
            rows = await self.worker_store.failed_page(
                after=self.worker_cursor, limit=self.batch_size
            )
            for row in rows:
                await self._safe(
                    self.service.capture_worker,
                    self.worker_store,
                    row["command_id"],
                )
            self.worker_cursor = (
                (rows[-1]["execution_id"], rows[-1]["sequence"])
                if len(rows) == self.batch_size
                else None
            )
        due = await self.service.store.due(self.batch_size)
        semaphore = asyncio.Semaphore(self.concurrency)

        async def run(task_id):
            async with semaphore:
                await self._safe(self.service.execute, task_id)

        async with asyncio.TaskGroup() as tasks:
            for task_id in due:
                tasks.create_task(run(task_id))

    async def _safe(self, call, *args) -> None:
        try:
            await call(*args)
        except Exception as error:
            logger.warning(
                "Failure recovery deferred operation=%s error=%s",
                getattr(call, "__name__", type(call).__name__),
                type(error).__name__,
            )

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await self._safe(self.once)
            try:
                await asyncio.wait_for(stop.wait(), self.poll_seconds)
            except TimeoutError:
                pass
