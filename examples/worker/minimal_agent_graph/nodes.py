"""Minimum graph nodes required by the validated Worker adapter."""

from __future__ import annotations

from typing import Any, Protocol
from uuid import UUID

from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt

from worker import ExecutorEvent

from .state import ExecutorBoundaryState


def get_session_id(config: RunnableConfig) -> str:
    """Read the one authoritative session ID from LangGraph config."""

    session_id = config.get("configurable", {}).get("thread_id")
    if not isinstance(session_id, str) or not session_id:
        raise ValueError("configurable.thread_id is required")
    return session_id


class ExecutionBindings(Protocol):
    """Subset of the Worker binding store used by the Agent graph."""

    async def register(
        self,
        *,
        execution_id: UUID,
        session_id: str,
        task_id: str,
    ) -> None: ...


class ExecutorBoundaryNodes:
    """Register an execution and wait for its matching Worker event."""

    def __init__(self, bindings: ExecutionBindings) -> None:
        self.bindings = bindings

    async def register_execution(
        self,
        state: ExecutorBoundaryState,
        config: RunnableConfig,
    ) -> dict[str, Any]:
        """Bind the Executor execution to its session and Agent task."""

        await self.bindings.register(
            execution_id=UUID(state["execution_id"]),
            session_id=get_session_id(config),
            task_id=state["task_id"],
        )
        return {}

    def wait_executor_event(
        self,
        state: ExecutorBoundaryState,
    ) -> dict[str, Any]:
        """Pause until the Worker resumes this exact execution boundary."""

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

        UUID(str(action.get("command_id", "")))
        event = ExecutorEvent.model_validate(action.get("event"))
        if str(event.execution_id) != state["execution_id"]:
            raise ValueError("Worker action execution identity mismatch")

        return {"executor_action": action}
