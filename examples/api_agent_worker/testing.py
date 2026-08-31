"""Process-local fakes for tests only. Never use as Kubernetes adapters."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from uuid import NAMESPACE_URL, UUID, uuid5

from examples.api_agent_worker.contracts import ExecutorBoundarySignal
from examples.api_agent_worker.ports import ExecutionStatus, RunBusyError
from examples.durable_event_to_langgraph.contracts import (
    CommandState,
    WorkflowCommand,
)
from examples.durable_event_to_langgraph.memory_store import (
    InMemoryDurableStore,
)


class MemoryRunGuard:
    def __init__(self) -> None:
        self.locks: dict[UUID, asyncio.Lock] = {}

    @asynccontextmanager
    async def hold(self, task_id: UUID) -> AsyncIterator[None]:
        lock = self.locks.setdefault(task_id, asyncio.Lock())
        if lock.locked():
            raise RunBusyError("Task invocation is still active")
        async with lock:
            yield


class MemoryCommands(InMemoryDurableStore):
    def add(self, command: WorkflowCommand) -> None:
        self._commands[command.command_id] = command

    async def get_command(self, command_id: UUID) -> WorkflowCommand | None:
        command = self._commands.get(command_id)
        if command is None or command.state is CommandState.DONE:
            return command
        for candidate in self._commands.values():
            if (
                candidate.workflow_id == command.workflow_id
                and candidate.state is not CommandState.DONE
            ):
                return command if candidate.command_id == command_id else None
        return None


class FakeExecutor:
    def __init__(self) -> None:
        self.submissions: dict[str, UUID] = {}
        self.status: ExecutionStatus = "WAITING"
        self.submit_entered = asyncio.Event()
        self.submit_release = asyncio.Event()
        self.submit_release.set()
        self.reconcile_failures = 0
        self.reconciliations = 0

    async def submit(
        self, task_id: UUID, objective: str, idempotency_key: str
    ) -> UUID:
        execution_id = self.submissions.setdefault(
            idempotency_key, uuid5(NAMESPACE_URL, idempotency_key)
        )
        self.submit_entered.set()
        await self.submit_release.wait()
        return execution_id

    async def reconcile(
        self, signal: ExecutorBoundarySignal
    ) -> ExecutionStatus:
        if self.reconcile_failures:
            self.reconcile_failures -= 1
            raise ConnectionError("Simulated Executor result outage")
        self.reconciliations += 1
        return self.status
