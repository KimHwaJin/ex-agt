"""Business nodes for the minimal Agent example."""

from __future__ import annotations

from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from langchain_core.messages import AIMessage

from agent.graph.state import AgentState


def create_task(state: AgentState) -> dict[str, Any]:
    """Create a Task inside the Agent when code execution starts."""

    del state
    return {"task_id": str(uuid4())}


async def submit_sample_execution(state: AgentState) -> dict[str, Any]:
    """Return a fake execution ID without calling a real Executor.

    Replace this node with the receiving service's idempotent Executor REST
    submission. The real node must return ``execution_id`` as a string.
    """

    execution_id = uuid5(
        NAMESPACE_URL,
        f"executor-worker-sample/{state['task_id']}",
    )
    return {"execution_id": str(execution_id)}


async def handle_executor_event(state: AgentState) -> dict[str, Any]:
    """Apply one delivered event using host Agent business logic."""

    action = state["ew_pending"]
    event = action["event"]
    return {
        "received_events": [event],
        "messages": [
            AIMessage(
                content=(
                    f"Executor 이벤트를 처리했습니다: {event['event_type']}"
                )
            )
        ],
    }
