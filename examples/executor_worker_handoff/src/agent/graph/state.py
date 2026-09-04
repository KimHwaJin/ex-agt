"""State for the smallest Agent that can wait for an Executor event."""

from __future__ import annotations

import operator
from typing import Annotated, Any, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from agent_worker.graph_boundary import ExecutorBoundaryState


class AgentInput(TypedDict):
    """Values supplied by the API when starting this sample graph."""

    messages: list[AnyMessage]


class AgentState(ExecutorBoundaryState, total=False):
    """Host Agent state plus fields required by the Worker boundary."""

    messages: Annotated[list[AnyMessage], add_messages]
    received_events: Annotated[list[dict[str, Any]], operator.add]
