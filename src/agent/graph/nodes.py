from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, Protocol
from uuid import UUID

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt

from agent.graph.state import SessionState, TaskTurn
from ex_agent.application.state import WorkflowState
from ex_agent.domain.contracts import (
    CancelRequestedSignal,
    ExecutorBoundarySignal,
)
from ex_agent.domain.enums import TaskStatus
from worker.contracts import ExecutorEvent


class ExecutionBindings(Protocol):
    async def register(
        self,
        *,
        execution_id: UUID,
        session_id: str,
        task_id: str,
        actor: str = "api",
    ) -> None: ...


def begin_task(state: SessionState, config: RunnableConfig) -> dict[str, Any]:
    turn = TaskTurn.model_validate(state["turn"])
    if config.get("configurable", {}).get("thread_id") != turn.session_id:
        raise ValueError("thread_id must equal session_id")
    for key in ("session_id", "user_id", "project_id"):
        if key in state and state[key] != getattr(turn, key):
            raise ValueError("Session ownership cannot change")
    previous = state.get("workflow", {})
    if previous and not TaskStatus(previous["phase"]).is_terminal:
        raise ValueError("The session already has an unfinished task")
    requests = state.get("task_requests", {})
    if turn.active_task_id in requests:
        raise ValueError("Task ID has already been admitted")
    return {
        "schema_version": "session-1.0",
        "session_id": turn.session_id,
        "user_id": turn.user_id,
        "project_id": turn.project_id,
        "active_task_id": turn.active_task_id,
        "execution_id": "",
        "workflow": {
            **turn.model_dump(),
            "phase": TaskStatus.ACCEPTED,
        },
        "task_requests": {
            **requests,
            turn.active_task_id: turn.fingerprint,
        },
        "ew_pending": {},
        "invocation_owner": {},
        "messages": [
            HumanMessage(
                content=turn.user_message,
                id=turn.current_input_message_id,
            )
        ],
    }


def lift_node(node: Callable[..., Any]) -> Callable[..., Any]:
    """Reuse business nodes without giving them worker receipt state."""

    async def run(state: SessionState) -> dict[str, Any]:
        result = node(state["workflow"])
        updates = await result if inspect.isawaitable(result) else result
        workflow = {**state["workflow"], **updates}
        output: dict[str, Any] = {
            "workflow": workflow,
            "execution_id": workflow.get("execution_id", ""),
        }
        if TaskStatus(workflow["phase"]).is_terminal:
            message = (
                workflow.get("terminal_message")
                or workflow.get("report_markdown")
                or workflow.get("answer")
            )
            if message:
                output["messages"] = [
                    AIMessage(
                        content=message,
                        id=f"task:{state['active_task_id']}:result",
                    )
                ]
        return output

    return run


def lift_route(route: Callable[[WorkflowState], str]) -> Callable[..., str]:
    def choose(state: SessionState) -> str:
        return route(state["workflow"])

    return choose


class WorkerBoundaryNodes:
    def __init__(self, bindings: ExecutionBindings) -> None:
        self.bindings = bindings

    async def register_execution(self, state: SessionState) -> dict[str, Any]:
        execution_id = state["execution_id"]
        await self.bindings.register(
            execution_id=UUID(execution_id),
            session_id=state["session_id"],
            task_id=state["active_task_id"],
            actor="agent",
        )
        return {
            "ew_sequences": {
                **state.get("ew_sequences", {}),
                execution_id: state.get("ew_sequences", {}).get(
                    execution_id, 0
                ),
            }
        }

    def wait_external_signal(self, state: SessionState) -> dict[str, Any]:
        action = interrupt(
            {
                "kind": "EXECUTOR_EVENT",
                "task_id": state["active_task_id"],
                "execution_id": state["execution_id"],
                "cancellable": True,
            }
        )
        if isinstance(action, dict) and action.get("type") == (
            "CANCEL_REQUESTED"
        ):
            signal = CancelRequestedSignal.model_validate(action)
            if str(signal.task_id) != state["active_task_id"]:
                raise ValueError("Cancel task identity mismatch")
            return {
                "workflow": {
                    **state["workflow"],
                    "external_signal": signal.model_dump(mode="json"),
                },
                "ew_pending": {},
            }
        if (
            not isinstance(action, dict)
            or action.get("task_id") != (state["active_task_id"])
        ):
            raise ValueError("Invalid worker action task identity")
        UUID(action["command_id"])
        event = ExecutorEvent.model_validate(action["event"])
        if str(event.execution_id) != state["execution_id"]:
            raise ValueError("Invalid worker action execution identity")
        if event.event_sequence <= state.get("ew_sequences", {}).get(
            state["execution_id"], 0
        ):
            raise ValueError("Worker action sequence is already applied")
        boundary = ExecutorBoundarySignal.model_validate(
            event.model_dump(
                include={
                    "execution_id",
                    "event_id",
                    "event_sequence",
                    "event_type",
                }
            )
        )
        # No external effects here: acceptance checkpoints before reconcile.
        return {
            "ew_pending": action,
            "invocation_owner": {
                "source": "EXECUTOR",
                "id": action["command_id"],
            },
            "workflow": {
                **state["workflow"],
                "external_signal": boundary.model_dump(mode="json"),
            },
        }

    def record_event_receipt(self, state: SessionState) -> dict[str, Any]:
        action = state["ew_pending"]
        event = ExecutorEvent.model_validate(action["event"])
        return {
            "ew_receipts": {
                **state.get("ew_receipts", {}),
                action["command_id"]: str(event.event_id),
            },
            "ew_sequences": {
                **state.get("ew_sequences", {}),
                str(event.execution_id): event.event_sequence,
            },
        }
