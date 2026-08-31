from __future__ import annotations

import asyncio
from dataclasses import replace
from uuid import NAMESPACE_URL, UUID, uuid5

from examples.durable_event_to_langgraph.contracts import (
    CommandState,
    ExternalEvent,
    WorkflowCommand,
)

_BOUNDARY_EVENTS = {
    "workflow.step_completed",
    "workflow.completed",
}


class InMemoryDurableStore:
    """Test-only model of the transaction required in PostgreSQL."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._event_ids: set[UUID] = set()
        self._sequences: dict[str, int] = {}
        self._commands: dict[UUID, WorkflowCommand] = {}
        self.outbox: list[dict[str, str]] = []

    async def accept(self, event: ExternalEvent) -> bool:
        async with self._lock:
            if event.event_id in self._event_ids:
                return False
            expected = self._sequences.get(event.workflow_id, 0) + 1
            if event.sequence != expected:
                raise RuntimeError(
                    f"sequence gap: expected {expected}, got {event.sequence}"
                )

            command: WorkflowCommand | None = None
            if event.event_type in _BOUNDARY_EVENTS:
                command_id = uuid5(
                    NAMESPACE_URL,
                    f"durable-event:{event.event_id}:resume",
                )
                command = WorkflowCommand(
                    command_id=command_id,
                    workflow_id=event.workflow_id,
                    command_type="RESUME",
                    payload={
                        **event.payload,
                        "event_id": str(event.event_id),
                        "event_type": event.event_type,
                        "sequence": event.sequence,
                    },
                )

            self._event_ids.add(event.event_id)
            self._sequences[event.workflow_id] = event.sequence
            if command is not None:
                self._commands[command.command_id] = command
                self.outbox.append(command.stream_fields())
            return True

    async def get_command(
        self,
        command_id: UUID,
    ) -> WorkflowCommand | None:
        return self._commands.get(command_id)

    async def mark_processing(self, command_id: UUID) -> None:
        command = self._required(command_id)
        self._commands[command_id] = replace(
            command,
            state=CommandState.PROCESSING,
            attempt_count=command.attempt_count + 1,
        )

    async def mark_done(self, command_id: UUID) -> None:
        command = self._required(command_id)
        self._commands[command_id] = replace(
            command,
            state=CommandState.DONE,
            last_error=None,
        )

    async def mark_retry(self, command_id: UUID, error: str) -> None:
        command = self._required(command_id)
        self._commands[command_id] = replace(
            command,
            state=CommandState.PENDING,
            last_error=error,
        )

    def _required(self, command_id: UUID) -> WorkflowCommand:
        command = self._commands.get(command_id)
        if command is None:
            raise LookupError(f"unknown command: {command_id}")
        return command
