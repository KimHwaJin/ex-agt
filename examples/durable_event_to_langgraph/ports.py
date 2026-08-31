from __future__ import annotations

from typing import Protocol
from uuid import UUID

from examples.durable_event_to_langgraph.contracts import (
    ExternalEvent,
    WorkflowCommand,
)


class DurableEventBridge(Protocol):
    async def accept(self, event: ExternalEvent) -> bool:
        """Atomically persist inbox, sequence, command, and outbox.

        Return False only when the event was already committed.
        """
        ...


class CommandStore(Protocol):
    async def get_command(
        self,
        command_id: UUID,
    ) -> WorkflowCommand | None: ...

    async def mark_processing(self, command_id: UUID) -> None: ...

    async def mark_done(self, command_id: UUID) -> None: ...

    async def mark_retry(
        self,
        command_id: UUID,
        error: str,
    ) -> None: ...


class WorkflowRunner(Protocol):
    async def start(self, workflow_id: str, objective: str) -> None: ...

    async def resume(self, command: WorkflowCommand) -> bool:
        """Apply a command, or report that its checkpoint already exists."""
        ...
