from __future__ import annotations

import logging
from uuid import UUID

from psycopg import Error as DatabaseError
from psycopg_pool import PoolTimeout
from redis.exceptions import RedisError

from worker.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    StreamMessage,
)
from worker.contracts import (
    DeferEvent,
    EventHandler,
    IgnoreEvent,
    RejectEvent,
)
from worker.guard import LeaseLostError, SessionGuard
from worker.store import Store

logger = logging.getLogger(__name__)


class Dispatcher:
    def __init__(
        self,
        store: Store,
        guard: SessionGuard,
        handlers: dict[str, EventHandler],
        *,
        max_attempts: int = 5,
    ) -> None:
        self.store = store
        self.guard = guard
        self.handlers = dict(handlers)
        self.max_attempts = max_attempts

    def lock_key(self, message: StreamMessage) -> None:
        # Resolve the session from the trusted DB, not from Redis fields.
        # The core consumer still renews the pending-message lease.
        return None

    async def handle(self, message: StreamMessage) -> HandlerResult:
        try:
            if (
                message.fields["schema_version"] != "1"
                or message.fields["namespace"] != self.store.namespace
            ):
                raise ValueError("Unsupported command envelope")
            command_id = UUID(message.fields["command_id"])
            generation = int(message.fields["generation"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError(str(error)) from error
        try:
            row = await self.store.command(command_id)
            if row is None:
                raise PermanentMessageError("Unknown command ID")
            async with self.guard.hold(row["session_id"]):
                # Another invocation may have completed while we acquired.
                return await self._dispatch(command_id, generation)
        except (
            DeferEvent,
            LeaseLostError,
            DatabaseError,
            PoolTimeout,
            RedisError,
        ) as error:
            logger.warning("Command deferred: %s", type(error).__name__)
            return HandlerResult(AckDecision.DEFER, outcome="deferred")

    async def _dispatch(
        self,
        command_id: UUID,
        generation: int,
    ) -> HandlerResult:
        row = await self.store.command(command_id)
        assert row is not None
        if generation != row["generation"]:
            return HandlerResult(AckDecision.ACK, outcome="old_generation")
        if row["state"] in {"DONE", "IGNORED"}:
            return HandlerResult(AckDecision.ACK, outcome="duplicate")
        if row["state"] == "FAILED":
            raise PermanentMessageError(row["last_error"] or "Failed command")
        context = self.store.context(row)
        handler = self.handlers.get(context.event.event_type)
        if handler is None:
            # A replica with different code must not silently discard work.
            raise DeferEvent("Handler registry differs from routed event")
        await self.store.set_state(command_id, "RUNNING")
        try:
            await handler(context)
        except (
            DeferEvent,
            LeaseLostError,
            DatabaseError,
            PoolTimeout,
            RedisError,
        ):
            raise
        except IgnoreEvent as error:
            await self.store.set_state(command_id, "IGNORED", error=str(error))
            return HandlerResult(AckDecision.ACK, outcome="ignored")
        except Exception as error:
            terminal = isinstance(error, RejectEvent) or (
                row["failure_attempts"] + 1 >= self.max_attempts
            )
            reason = f"{type(error).__name__}: {error}"
            await self.store.set_state(
                command_id,
                "FAILED" if terminal else "RUNNING",
                error=reason,
                failed_attempt=True,
            )
            if terminal:
                raise PermanentMessageError(reason) from error
            # PEL owns redelivery. DB counts only real handler failures,
            # not lock contention or dependencies waiting to become ready.
            return HandlerResult(AckDecision.DEFER, outcome="handler_retry")
        await self.store.set_state(command_id, "DONE")
        return HandlerResult(AckDecision.ACK)
