"""Bounded product-event outbox recovery shared by API and Worker."""

import asyncio
import logging
from typing import Any

logger = logging.getLogger(__name__)


class ProductEventRecovery:
    def __init__(
        self,
        publisher: Any,
        *,
        poll_seconds: float,
        idle_seconds: float,
    ) -> None:
        if poll_seconds <= 0 or idle_seconds < poll_seconds:
            raise ValueError("Invalid product-event recovery intervals")
        self.publisher = publisher
        self.poll_seconds = poll_seconds
        self.idle_seconds = idle_seconds

    async def once(self) -> int:
        return await self.publisher.publish_pending()

    async def run(self, stop: asyncio.Event) -> None:
        delay = self.poll_seconds
        while not stop.is_set():
            try:
                count = await self.once()
                delay = (
                    self.poll_seconds
                    if count
                    else min(delay * 2, self.idle_seconds)
                )
            except Exception as error:
                logger.warning(
                    "Product-event relay deferred error=%s",
                    type(error).__name__,
                )
                delay = min(max(delay * 2, 0.5), 30)
            try:
                await asyncio.wait_for(stop.wait(), delay)
            except TimeoutError:
                pass


__all__ = ["ProductEventRecovery"]
