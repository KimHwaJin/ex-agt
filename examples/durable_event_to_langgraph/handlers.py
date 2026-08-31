from __future__ import annotations

from ex_agent.transport.consumer import (
    AckDecision,
    HandlerResult,
    StreamMessage,
)
from examples.durable_event_to_langgraph.contracts import (
    CommandState,
    ExternalEvent,
    WorkflowCommand,
)
from examples.durable_event_to_langgraph.ports import (
    CommandStore,
    DurableEventBridge,
    WorkflowRunner,
)


class ExternalEventHandler:
    """Convert an external event into a durable internal command."""

    def __init__(self, bridge: DurableEventBridge) -> None:
        self._bridge = bridge

    def lock_key(self, message: StreamMessage) -> str:
        event = ExternalEvent.from_message(message)
        return f"workflow:event-lock:{event.workflow_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        event = ExternalEvent.from_message(message)
        accepted = await self._bridge.accept(event)
        outcome = "accepted" if accepted else "duplicate"
        return HandlerResult(AckDecision.ACK, outcome=outcome)


class DurableCommandHandler:
    """Apply a committed command to LangGraph with bounded redelivery."""

    def __init__(
        self,
        store: CommandStore,
        runner: WorkflowRunner,
    ) -> None:
        self._store = store
        self._runner = runner

    def lock_key(self, message: StreamMessage) -> str:
        command = WorkflowCommand.from_message(message)
        return f"workflow:command-lock:{command.workflow_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        envelope = WorkflowCommand.from_message(message)
        command = await self._store.get_command(envelope.command_id)
        if command is None:
            return HandlerResult(
                AckDecision.RETRY,
                outcome="command_not_visible",
                reason="authoritative command is not visible yet",
            )
        if command.state is CommandState.DONE:
            return HandlerResult(AckDecision.ACK, outcome="duplicate")

        await self._store.mark_processing(command.command_id)
        try:
            applied = await self._runner.resume(command)
        except Exception as error:
            failure = f"{type(error).__name__}: {error}"
            await self._store.mark_retry(command.command_id, failure)
            return HandlerResult(
                AckDecision.RETRY,
                outcome="resume_failed",
                reason=failure,
                error_type=type(error).__name__,
            )

        await self._store.mark_done(command.command_id)
        outcome = "applied" if applied else "already_checkpointed"
        return HandlerResult(AckDecision.ACK, outcome=outcome)
