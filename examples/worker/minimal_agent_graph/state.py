"""State fields required at the Agent-to-Executor boundary."""

from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages


class ExecutorBoundaryInput(TypedDict):
    """Identifiers required when entering the Executor wait boundary."""

    task_id: str
    execution_id: str


class ExecutorBoundaryState(ExecutorBoundaryInput, total=False):
    """Boundary state extended after the Worker resumes the graph."""

    executor_action: dict[str, Any]


class AgentInput(TypedDict):
    """Example host-Agent input; replace fields with the real API input."""

    messages: list[AnyMessage]


class AgentState(ExecutorBoundaryState, total=False):
    """Example host-Agent state extended with its own fields."""

    messages: Annotated[list[AnyMessage], add_messages]
