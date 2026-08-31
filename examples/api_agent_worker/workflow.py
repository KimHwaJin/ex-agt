from __future__ import annotations

from typing import Any, TypedDict, cast
from uuid import UUID

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from examples.api_agent_worker.contracts import ExecutorBoundarySignal
from examples.api_agent_worker.ports import ExecutorPort


class HandoffState(TypedDict):
    task_id: str
    objective: str
    phase: str
    execution_id: str
    pending_action: dict[str, Any]
    receipts: dict[str, str]
    last_event_sequence: int
    applied_count: int


def _record_receipt(state: HandoffState) -> dict[str, str]:
    action = state["pending_action"]
    return {**state["receipts"], action["action_id"]: action["fingerprint"]}


def _review(state: HandoffState) -> dict[str, Any]:
    action = interrupt({"kind": "PLAN_REVIEW", "task_id": state["task_id"]})
    if not isinstance(action, dict) or action.get("kind") != "USER_REVIEW":
        raise ValueError("Review requires a validated user action")
    return {"pending_action": action}


def _wait(state: HandoffState) -> dict[str, Any]:
    action = interrupt(
        {
            "kind": "EXECUTOR_EVENT",
            "execution_id": state["execution_id"],
        }
    )
    if not isinstance(action, dict) or action.get("kind") != "EXECUTOR_SIGNAL":
        raise ValueError("Execution wait requires a validated event command")
    return {"pending_action": action}


def build_graph(checkpointer: Any, executor: ExecutorPort) -> Any:
    """Small reference graph, NOT the full analysis/planning application."""

    async def submit(state: HandoffState) -> dict[str, Any]:
        if not state["pending_action"]["payload"]["approved"]:
            return {"phase": "REJECTED", "receipts": _record_receipt(state)}
        execution_id = await executor.submit(
            UUID(state["task_id"]),
            state["objective"],
            f"handoff:{state['task_id']}:submit",
        )
        return {
            "execution_id": str(execution_id),
            "phase": "WAITING",
            "receipts": _record_receipt(state),
        }

    async def apply_event(state: HandoffState) -> dict[str, Any]:
        signal = ExecutorBoundarySignal.model_validate(
            state["pending_action"]["payload"]
        )
        status = await executor.reconcile(signal)
        return {
            "phase": status,
            "last_event_sequence": signal.event_sequence,
            "applied_count": state["applied_count"] + 1,
            "receipts": _record_receipt(state),
        }

    def route(state: HandoffState) -> str:
        return "wait" if state["phase"] == "WAITING" else "end"

    builder = StateGraph(cast(Any, HandoffState))
    builder.add_node("review", _review)
    builder.add_node("submit", submit)
    builder.add_node("wait", _wait)
    builder.add_node("apply_event", apply_event)
    builder.add_edge(START, "review")
    builder.add_edge("review", "submit")
    builder.add_conditional_edges(
        "submit", route, {"wait": "wait", "end": END}
    )
    builder.add_edge("wait", "apply_event")
    builder.add_conditional_edges(
        "apply_event", route, {"wait": "wait", "end": END}
    )
    return builder.compile(checkpointer=checkpointer)
