from __future__ import annotations

from typing import Any, TypedDict, cast

from langgraph.graph import END, START, StateGraph
from langgraph.types import Command, interrupt

from examples.durable_event_to_langgraph.contracts import WorkflowCommand


class ExampleWorkflowState(TypedDict):
    objective: str
    pending_event: dict[str, Any]
    last_command_id: str
    last_event_type: str
    applied_count: int
    terminal: bool


def _wait_for_event(
    state: ExampleWorkflowState,
) -> dict[str, dict[str, Any]]:
    resumed = interrupt(
        {
            "kind": "EXTERNAL_EVENT",
            "objective": state["objective"],
        }
    )
    if not isinstance(resumed, dict):
        raise TypeError("resume payload must be an object")
    return {"pending_event": resumed}


def _apply_event(state: ExampleWorkflowState) -> dict[str, Any]:
    event = state["pending_event"]
    command_id = event.get("command_id")
    event_type = event.get("event_type")
    if not isinstance(command_id, str) or not command_id:
        raise ValueError("resume payload requires command_id")
    if not isinstance(event_type, str) or not event_type:
        raise ValueError("resume payload requires event_type")
    return {
        "last_command_id": command_id,
        "last_event_type": event_type,
        "applied_count": state["applied_count"] + 1,
        "terminal": event_type == "workflow.completed",
    }


def _route_after_event(state: ExampleWorkflowState) -> str:
    return "end" if state["terminal"] else "wait"


def build_workflow(checkpointer: Any) -> Any:
    builder = StateGraph(cast(Any, ExampleWorkflowState))
    builder.add_node("wait_for_event", _wait_for_event)
    builder.add_node("apply_event", _apply_event)
    builder.add_edge(START, "wait_for_event")
    builder.add_edge("wait_for_event", "apply_event")
    builder.add_conditional_edges(
        "apply_event",
        _route_after_event,
        {"wait": "wait_for_event", "end": END},
    )
    return builder.compile(checkpointer=checkpointer)


class LangGraphWorkflowRunner:
    def __init__(self, graph: Any) -> None:
        self._graph = graph

    @staticmethod
    def _config(workflow_id: str) -> dict[str, Any]:
        return {"configurable": {"thread_id": workflow_id}}

    async def start(self, workflow_id: str, objective: str) -> None:
        initial: ExampleWorkflowState = {
            "objective": objective,
            "pending_event": {},
            "last_command_id": "",
            "last_event_type": "",
            "applied_count": 0,
            "terminal": False,
        }
        await self._graph.ainvoke(initial, self._config(workflow_id))

    async def resume(self, command: WorkflowCommand) -> bool:
        config = self._config(command.workflow_id)
        snapshot = await self._graph.aget_state(config)
        if snapshot.values.get("last_command_id") == str(command.command_id):
            return False
        payload = {
            **command.payload,
            "command_id": str(command.command_id),
        }
        await self._graph.ainvoke(Command(resume=payload), config)
        return True

    async def state(self, workflow_id: str) -> dict[str, Any]:
        snapshot = await self._graph.aget_state(self._config(workflow_id))
        return dict(snapshot.values)
