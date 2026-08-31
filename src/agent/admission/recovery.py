import asyncio
import logging

from agent.admission.service import AdmissionService

logger = logging.getLogger(__name__)


class RequestRecovery:
    """Host-managed, bounded polling; no additional broker or consumer."""

    def __init__(
        self,
        service: AdmissionService,
        *,
        concurrency: int = 4,
        batch_size: int = 32,
        poll_seconds: float = 2,
    ) -> None:
        if not 1 <= concurrency <= 32 or not 1 <= batch_size <= 100:
            raise ValueError("Invalid request recovery concurrency/batch size")
        if poll_seconds <= 0:
            raise ValueError("Recovery poll interval must be positive")
        self.service = service
        self.concurrency = concurrency
        self.batch_size = batch_size
        self.poll_seconds = poll_seconds

    async def once(self) -> int:
        ids = await self.service.store.due(limit=self.batch_size)
        semaphore = asyncio.Semaphore(self.concurrency)

        async def recover(request_id):
            async with semaphore:
                try:
                    await self.service.execute(request_id)
                except Exception as error:
                    # DB/Redis outages leave the request for a later poll.
                    logger.warning(
                        "API recovery deferred request_id=%s error=%s",
                        request_id,
                        type(error).__name__,
                    )

        async with asyncio.TaskGroup() as tasks:
            for request_id in ids:
                tasks.create_task(recover(request_id))
        return len(ids)

    async def run(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await self.once()
            except Exception as error:
                logger.warning(
                    "API recovery poll failed: %s", type(error).__name__
                )
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.poll_seconds)
            except TimeoutError:
                pass
