from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from uuid import UUID

from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.transport.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    StreamMessage,
)

if TYPE_CHECKING:
    from ex_agent.worker import WorkflowWorker

logger = logging.getLogger(__name__)

FAILURE_COMPENSATION = "FAILURE_COMPENSATION"


class CommandHandler:
    def __init__(self, worker: WorkflowWorker, graph: Any) -> None:
        self._worker = worker
        self._graph = graph

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
            await self._worker._process_command(self._graph, command_id)
        except Exception as error:
            logger.exception(
                "Workflow command failed",
                extra={"task_id": str(task_id)},
            )
            current = await self._worker._repository.get_command(command_id)
            failure_message = f"{type(error).__name__}: {error}"
            if (
                current is not None
                and current.command_type == FAILURE_COMPENSATION
            ):
                await self._worker._repository.set_command_state(
                    command_id,
                    "PENDING",
                    failure_message,
                )
            elif current is not None and current.attempt_count >= 3:
                await self._worker._repository.prepare_failure_compensation(
                    command_id,
                    task_id,
                    failure_message,
                )
            else:
                await self._worker._repository.set_command_state(
                    command_id,
                    "PENDING",
                    failure_message,
                )
            return HandlerResult(AckDecision.ACK, outcome="failed")
        return HandlerResult(AckDecision.ACK)


class ExecutorEventHandler:
    def __init__(self, worker: WorkflowWorker, stream: str) -> None:
        self._worker = worker
        self._stream = stream

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
        processed = await self._worker._process_executor_event(
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
]
