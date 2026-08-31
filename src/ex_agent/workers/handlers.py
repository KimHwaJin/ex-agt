from __future__ import annotations

import logging
from typing import Any, Protocol
from uuid import UUID

from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.transport.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    StreamMessage,
)

logger = logging.getLogger(__name__)

FAILURE_COMPENSATION = "FAILURE_COMPENSATION"


class ProcessCommand(Protocol):
    async def __call__(self, graph: Any, command_id: UUID) -> None: ...


class RecordCommandFailure(Protocol):
    async def __call__(
        self,
        command_id: UUID,
        task_id: UUID,
        error: Exception,
    ) -> None: ...


class ProcessExecutorEvent(Protocol):
    async def __call__(
        self,
        stream: str,
        event: ExecutorEvent,
        *,
        catch_up: bool = False,
    ) -> bool: ...


class CommandHandler:
    def __init__(
        self,
        graph: Any,
        process: ProcessCommand,
        record_failure: RecordCommandFailure,
    ) -> None:
        self._graph = graph
        self._process = process
        self._record_failure = record_failure

    def lock_key(self, message: StreamMessage) -> str:
        try:
            task_id = UUID(message.fields["task_id"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid command task_id: {error}"
            ) from error
        return f"agent:task-lock:{task_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        try:
            command_id = UUID(message.fields["command_id"])
            task_id = UUID(message.fields["task_id"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid command envelope: {error}"
            ) from error
        try:
            await self._process(self._graph, command_id)
        except Exception as error:
            logger.exception(
                "Workflow command failed",
                extra={"task_id": str(task_id)},
            )
            await self._record_failure(command_id, task_id, error)
            return HandlerResult(AckDecision.ACK, outcome="failed")
        return HandlerResult(AckDecision.ACK)


class ExecutorEventHandler:
    def __init__(
        self,
        stream: str,
        process: ProcessExecutorEvent,
    ) -> None:
        self._stream = stream
        self._process = process

    def _event(self, message: StreamMessage) -> ExecutorEvent:
        try:
            return ExecutorEvent.from_redis(message.fields)
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid Executor event envelope: {error}"
            ) from error

    def lock_key(self, message: StreamMessage) -> str:
        event = self._event(message)
        return f"agent:execution-lock:{event.execution_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        event = self._event(message)
        processed = await self._process(
            self._stream,
            event,
            catch_up=message.reclaimed,
        )
        if processed is False:
            return HandlerResult(
                AckDecision.RETRY,
                outcome="binding_pending",
                reason="Executor binding is not visible yet",
            )
        return HandlerResult(AckDecision.ACK)


__all__ = [
    "FAILURE_COMPENSATION",
    "CommandHandler",
    "ExecutorEventHandler",
    "ProcessCommand",
    "ProcessExecutorEvent",
    "RecordCommandFailure",
]
