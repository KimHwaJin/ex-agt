from typing import Any
from uuid import UUID

from langgraph.types import Interrupt

from ex_agent.graph.checkpoints import checkpoint_serializer
from ex_agent.transport.consumer import _autoclaim_page


def interrupt_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Interrupt):
        raw = value.value
    else:
        raw = getattr(value, "value", value)
    if not isinstance(raw, dict):
        raise TypeError("Graph interrupt payload must be an object")
    return raw


def autoclaim_entries(
    response: Any,
) -> list[tuple[str, dict[str, str]]]:
    return _autoclaim_page(response)[1]


def task_graph_config(task_id: UUID) -> dict[str, Any]:
    return {"configurable": {"thread_id": str(task_id)}}


__all__ = [
    "autoclaim_entries",
    "checkpoint_serializer",
    "interrupt_payload",
    "task_graph_config",
]
