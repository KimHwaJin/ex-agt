from __future__ import annotations

from uuid import UUID

from ex_agent.transport.consumer import (
    AckDecision,
    HandlerResult,
    PermanentMessageError,
    StreamMessage,
)
from examples.api_agent_worker.contracts import ExecutorBoundarySignal
from examples.api_agent_worker.ports import (
    BoundaryNotReadyError,
    ReadyCommandStore,
    RunBusyError,
    RunGuard,
)
from examples.api_agent_worker.runner import SharedGraphRunner
from examples.durable_event_to_langgraph.contracts import CommandState


class ExecutorResumeHandler:
    """Consume committed event commands, never START or user approvals.

    Uses the real task_id/command_id Redis envelope. The example store calls
    its task key workflow_id; a host adapter maps its own DB model to it.
    """

    def __init__(
        self,
        store: ReadyCommandStore,
        runner: SharedGraphRunner,
        guard: RunGuard,
    ) -> None:
        self.store = store
        self.runner = runner
        self.guard = guard

    def lock_key(self, message: StreamMessage) -> None:
        # RunGuard owns exclusion across API and Worker through mark_done.
        # The consumer still renews the message's PEL lease.
        return None

    async def handle(self, message: StreamMessage) -> HandlerResult:
        try:
            task_id = UUID(message.fields["task_id"])
            command_id = UUID(message.fields["command_id"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError("Invalid command identity") from error
        try:
            async with self.guard.hold(task_id):
                return await self._handle_locked(task_id, command_id)
        except (RunBusyError, BoundaryNotReadyError) as error:
            return HandlerResult(
                AckDecision.RETRY, outcome="not_ready", reason=str(error)
            )

    async def _handle_locked(
        self, task_id: UUID, command_id: UUID
    ) -> HandlerResult:
        command = await self.store.get_command(command_id)
        if command is None:
            raise BoundaryNotReadyError("Command is not visible yet")
        if command.workflow_id != str(task_id):
            raise PermanentMessageError("Command task identity mismatch")
        if command.command_type != "EXECUTOR_SIGNAL":
            raise PermanentMessageError("Worker only accepts EXECUTOR_SIGNAL")
        if command.state is CommandState.DONE:
            return HandlerResult(AckDecision.ACK, outcome="duplicate")
        try:
            signal = ExecutorBoundarySignal.model_validate(command.payload)
        except ValueError as error:
            raise PermanentMessageError("Invalid boundary signal") from error
        await self.store.mark_processing(command_id)
        try:
            applied = await self.runner.executor_signal(
                task_id, command_id, signal
            )
        except PermanentMessageError:
            raise
        except Exception as error:
            await self.store.mark_retry(command_id, str(error))
            return HandlerResult(
                AckDecision.RETRY,
                outcome="resume_retry",
                reason=str(error),
                error_type=type(error).__name__,
            )
        await self.store.mark_done(command_id)
        return HandlerResult(
            AckDecision.ACK,
            outcome="applied" if applied else "already_checkpointed",
        )
