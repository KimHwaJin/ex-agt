from __future__ import annotations

from contextlib import AbstractAsyncContextManager
from typing import Any, Literal, Protocol
from uuid import UUID

from examples.api_agent_worker.contracts import ExecutorBoundarySignal
from examples.durable_event_to_langgraph.contracts import WorkflowCommand
from examples.durable_event_to_langgraph.ports import CommandStore

ExecutionStatus = Literal["WAITING", "SUCCEEDED", "FAILED", "CANCELLED"]


class RunBusyError(RuntimeError):
    """Another API/Worker invocation owns this task's short-lived run gate."""


class BoundaryNotReadyError(RuntimeError):
    """The expected checkpoint boundary is not ready; do not ACK the signal."""


class RunGuard(Protocol):
    def hold(self, task_id: UUID) -> AbstractAsyncContextManager[None]:
        """Exclude API/Worker writers through checkpoint and DB completion.

        Production implementations must work across processes/Pods, renew
        leases, and abort the protected invocation if ownership is lost.
        This is NOT the session lock that rejects new user work.
        """
        ...


class ApiAdmission(Protocol):
    async def __call__(
        self,
        task_id: UUID,
        user_id: str,
        operation: str,
        body: dict[str, Any],
        /,
    ) -> None:
        """Authorize and durably record input before graph invocation.

        The host service owns task/session identity, request idempotency,
        plan freshness, and atomic session locking before approval.
        Fail closed by raising HTTPException on denied input.
        """
        ...


class ExecutorPort(Protocol):
    async def submit(
        self,
        task_id: UUID,
        objective: str,
        idempotency_key: str,
    ) -> UUID:
        """Submit idempotently and persist the execution/task binding."""
        ...

    async def reconcile(
        self, signal: ExecutorBoundarySignal
    ) -> ExecutionStatus:
        """Read REST results; event names alone do not imply success."""
        ...


class ReadyCommandStore(CommandStore, Protocol):
    async def get_command(self, command_id: UUID) -> WorkflowCommand | None:
        """Return DONE or the next runnable command, otherwise None.

        Serialize command order per task in the durable store. The run
        gate alone prevents concurrent writers, not out-of-order resume.
        """
        ...
