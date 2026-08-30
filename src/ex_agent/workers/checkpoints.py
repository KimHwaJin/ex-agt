from typing import Any
from uuid import UUID

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.types import Interrupt

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


def checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(
        allowed_msgpack_modules=[
            ("ex_agent.domain.contracts", "IntentDecision"),
            ("ex_agent.domain.contracts", "PlanDraft"),
            ("ex_agent.domain.contracts", "RiskReview"),
            ("ex_agent.domain.contracts", "WorkflowCandidate"),
            ("ex_agent.domain.enums", "ExecutionMode"),
            ("ex_agent.domain.enums", "Intent"),
            ("ex_agent.domain.enums", "PlanningKind"),
            ("ex_agent.domain.enums", "RiskLevel"),
            ("ex_agent.domain.enums", "TaskStatus"),
        ]
    )


__all__ = [
    "autoclaim_entries",
    "checkpoint_serializer",
    "interrupt_payload",
    "task_graph_config",
]
