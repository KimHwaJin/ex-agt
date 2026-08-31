from __future__ import annotations

import asyncio
import logging

import httpx
from psycopg import Error as DatabaseError
from psycopg_pool import PoolTimeout

from executor_worker.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    StreamMessage,
)
from executor_worker.contracts import ExecutorEvent
from executor_worker.store import Store

logger = logging.getLogger(__name__)


class Ingress:
    def __init__(self, store: Store) -> None:
        self.store = store

    def lock_key(self, message: StreamMessage) -> None:
        return None

    async def handle(self, message: StreamMessage) -> HandlerResult:
        try:
            event = ExecutorEvent.from_redis(message.fields)
            await self.store.ingest(event, catch_up=message.reclaimed)
        except (KeyError, ValueError, TypeError) as error:
            raise PermanentMessageError(str(error)) from error
        except (DatabaseError, PoolTimeout):
            logger.exception("Inbox unavailable; preserving pending event")
            return HandlerResult(AckDecision.DEFER, outcome="database_wait")
        return HandlerResult(AckDecision.ACK)


class EventRouter:
    def __init__(
        self,
        store: Store,
        http: httpx.AsyncClient,
        event_types: set[str],
        *,
        batch_size: int = 100,
        concurrency: int = 4,
    ) -> None:
        self.store = store
        self.http = http
        self.event_types = event_types
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(concurrency)

    async def once(self) -> int:
        rows = await self.store.scan_candidates(self.batch_size)

        async def process(row: dict) -> int:
            async with self.semaphore:
                execution_id = row["execution_id"]
                try:
                    count, gap = await self.store.advance(
                        execution_id,
                        self.event_types,
                        self.batch_size,
                    )
                    catch_up = (
                        row["catch_up_version"] > row["caught_up_version"]
                    )
                    if gap is not None or catch_up:
                        after = (
                            gap if gap is not None else row["last_sequence"]
                        )
                        response = await self.http.get(
                            f"/executions/{execution_id}/events",
                            params={
                                "after_sequence": after,
                                "limit": self.batch_size,
                            },
                        )
                        response.raise_for_status()
                        page = response.json()
                        items = page["items"]
                        if not items and (
                            gap is not None or page.get("has_more")
                        ):
                            raise ValueError("Executor history gap is open")
                        for item in items:
                            event = ExecutorEvent.model_validate(item)
                            if event.execution_id != execution_id:
                                raise ValueError("History mixed executions")
                            await self.store.ingest(event)
                        advanced, remaining_gap = await self.store.advance(
                            execution_id,
                            self.event_types,
                            self.batch_size,
                        )
                        if not advanced and remaining_gap is not None:
                            raise ValueError("History did not advance")
                        count += advanced
                        if catch_up and not page.get("has_more", False):
                            await self.store.finish_catch_up(
                                execution_id,
                                row["catch_up_version"],
                            )
                    return count
                except Exception as error:
                    logger.exception("Event routing deferred")
                    await self.store.scan_error(execution_id, str(error))
                    return 0

        return sum(await asyncio.gather(*(process(row) for row in rows)))
