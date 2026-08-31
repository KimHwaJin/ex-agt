"""Checkpoint acceptance at pure input nodes, before downstream effects."""

import inspect
from typing import Any

from langchain_core.runnables import RunnableConfig

from agent.graph.state import SessionState

INPUT_NODES = frozenset(
    {
        "begin_task",
        "clarify_request",
        "choose_execution_mode",
        "confirm_request_risk",
        "choose_workflow",
        "review_plan",
        "wait_external_signal",
    }
)


def with_api_receipt(name: str, node: Any) -> Any:
    if name not in INPUT_NODES:
        return node

    async def run(state: SessionState, config: RunnableConfig) -> dict:
        value = node(state, config) if name == "begin_task" else node(state)
        updates = await value if inspect.isawaitable(value) else value
        action = config.get("configurable", {}).get("api_action")
        if not action or action["target_node"] != name:
            return updates
        expected_task = updates.get(
            "active_task_id", state.get("active_task_id")
        )
        if action["task_id"] != expected_task:
            raise ValueError("API receipt Task mismatch")
        receipts = state.get("api_receipts", {})
        request_id = action["request_id"]
        receipt = {
            "task_id": expected_task,
            "fingerprint": action["fingerprint"],
        }
        if request_id in receipts:
            if receipts[request_id] != receipt:
                raise ValueError("API receipt identity mismatch")
            return updates
        return {
            **updates,
            "api_receipts": {**receipts, request_id: receipt},
            "invocation_owner": {"source": "API", "id": request_id},
        }

    return run
