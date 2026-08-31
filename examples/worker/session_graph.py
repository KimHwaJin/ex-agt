from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, TypedDict, cast

from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt


class State(TypedDict):
    active_task_id: str
    execution_id: str
    ew_pending: dict[str, Any]
    ew_receipts: dict[str, str]
    ew_sequences: dict[str, int]
    results: list[dict[str, Any]]
    finished: bool


def wait(state: State) -> dict[str, Any]:
    action = interrupt(
        {
            "kind": "EXECUTOR_EVENT",
            "task_id": state["active_task_id"],
            "execution_id": state["execution_id"],
        }
    )
    if (
        not isinstance(action, dict)
        or action.get("task_id") != state["active_task_id"]
        or action.get("event", {}).get("execution_id") != state["execution_id"]
    ):
        raise ValueError("Invalid Executor resume")
    # This separate node checkpoints the accepted action before effects.
    return {"ew_pending": action}


def build_graph(
    checkpointer: Any,
    effect: Callable[[dict], Awaitable[None]] | None = None,
) -> Any:
    async def apply(state: State) -> dict[str, Any]:
        action = state["ew_pending"]
        if effect is not None:
            # External calls MUST use action['command_id'] (plus a stable
            # operation suffix) as their idempotency key. Node may re-run.
            await effect(action)
        event = action["event"]
        return {
            "ew_receipts": {
                **state.get("ew_receipts", {}),
                action["command_id"]: event["event_id"],
            },
            "ew_sequences": {
                **state.get("ew_sequences", {}),
                event["execution_id"]: event["event_sequence"],
            },
            "ew_pending": action,
            "results": [*state.get("results", []), event],
            # completed means terminal, NOT necessarily successful.
            # Query Executor REST result for SUCCEEDED/FAILED/CANCELLED.
            "finished": event["event_type"] == "execution.completed",
        }

    builder = StateGraph(cast(Any, State))
    builder.add_node("wait", wait)
    builder.add_node("apply", apply)
    builder.add_edge(START, "wait")
    builder.add_edge("wait", "apply")
    builder.add_conditional_edges(
        "apply",
        lambda state: "end" if state["finished"] else "wait",
        {"end": END, "wait": "wait"},
    )
    return builder.compile(checkpointer=checkpointer)
