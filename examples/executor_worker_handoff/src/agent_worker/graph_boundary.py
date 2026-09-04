"""Minimum state and nodes at the Agent-to-Executor boundary."""

from __future__ import annotations

from typing import Any, Protocol, TypedDict
from uuid import UUID

from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt

from worker import ExecutorEvent


class ExecutorBoundaryInput(TypedDict):
    """Values the Agent creates immediately before waiting for Executor."""

    task_id: str
    execution_id: str


class ExecutorBoundaryState(ExecutorBoundaryInput, total=False):
    """Fields needed for durable Worker delivery and replay recovery."""

    ew_pending: dict[str, Any]
    ew_receipts: dict[str, str]
    ew_sequences: dict[str, int]


class ExecutionBindings(Protocol):
    """Small part of the Worker store used inside an Agent graph."""

    async def register(
        self,
        *,
        execution_id: UUID,
        session_id: str,
        task_id: str,
    ) -> None: ...


def session_id_from(config: RunnableConfig) -> str:
    """Use configurable.thread_id as the authoritative session ID."""

    session_id = config.get("configurable", {}).get("thread_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("configurable.thread_id is required")
    return session_id


class ExecutorBoundaryNodes:
    """Ready-to-use nodes for durable Agent-to-Executor delivery."""

    def __init__(self, bindings: ExecutionBindings) -> None:
        self.bindings = bindings

    async def register_execution(
        self,
        state: ExecutorBoundaryState,
        config: RunnableConfig,
    ) -> dict[str, Any]:
        """Bind execution_id to the current session and Agent task."""

        await self.bindings.register(
            execution_id=UUID(state["execution_id"]),
            session_id=session_id_from(config),
            task_id=state["task_id"],
        )
        return {}

    def wait_executor_event(
        self,
        state: ExecutorBoundaryState,
    ) -> dict[str, Any]:
        """Pause the graph until Worker supplies a matching event."""

        action = interrupt(
            {
                "kind": "EXECUTOR_EVENT",
                "task_id": state["task_id"],
                "execution_id": state["execution_id"],
            }
        )
        if not isinstance(action, dict):
            raise ValueError("Worker action must be an object")
        if action.get("task_id") != state["task_id"]:
            raise ValueError("Worker action task identity mismatch")
        event = ExecutorEvent.model_validate(action.get("event"))
        if str(event.execution_id) != state["execution_id"]:
            raise ValueError("Worker action execution identity mismatch")
        UUID(str(action.get("command_id", "")))
        return {"ew_pending": action}

    def record_executor_receipt(
        self,
        state: ExecutorBoundaryState,
    ) -> dict[str, Any]:
        """Confirm that the preceding Agent handler applied the event."""

        return receipt_update(state)


def receipt_update(
    state: ExecutorBoundaryState,
) -> dict[str, Any]:
    """Build the Worker receipt update without mutating Agent state."""

    action = state.get("ew_pending")
    if not action:
        raise ValueError("No pending Executor action")
    command_id = str(UUID(str(action.get("command_id", ""))))
    event = ExecutorEvent.model_validate(action.get("event"))
    if action.get("task_id") != state["task_id"]:
        raise ValueError("Pending action task identity mismatch")
    if str(event.execution_id) != state["execution_id"]:
        raise ValueError("Pending event execution identity mismatch")
    return {
        "ew_receipts": {
            **state.get("ew_receipts", {}),
            command_id: str(event.event_id),
        },
        "ew_sequences": {
            **state.get("ew_sequences", {}),
            str(event.execution_id): event.event_sequence,
        },
    }
