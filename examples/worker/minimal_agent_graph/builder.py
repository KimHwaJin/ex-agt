"""Minimum LangGraph builder compatible with the Worker adapter."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, cast

from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, StateGraph

from .nodes import ExecutionBindings, ExecutorBoundaryNodes
from .state import AgentInput, AgentState

CreateExecutionTask = Callable[
    [AgentState, RunnableConfig],
    Awaitable[dict[str, Any]],
]
SubmitExecution = Callable[
    [AgentState],
    Awaitable[dict[str, Any]],
]


def build_graph(
    bindings: ExecutionBindings,
    create_execution_task: CreateExecutionTask,
    submit_execution: SubmitExecution,
    *,
    checkpointer: Any,
) -> Any:
    """Build the minimum task, submit, register, and event-wait flow."""

    if checkpointer is None or checkpointer is False:
        raise ValueError("A persistent checkpointer is required")

    boundary = ExecutorBoundaryNodes(bindings)
    graph = StateGraph(
        cast(Any, AgentState),
        input_schema=cast(Any, AgentInput),
    )
    graph.add_node(
        "create_execution_task",
        cast(Any, create_execution_task),
    )
    graph.add_node("submit_execution", cast(Any, submit_execution))
    graph.add_node("register_execution", boundary.register_execution)
    graph.add_node("wait_executor_event", boundary.wait_executor_event)
    graph.add_edge(START, "create_execution_task")
    graph.add_edge("create_execution_task", "submit_execution")
    graph.add_edge("submit_execution", "register_execution")
    graph.add_edge("register_execution", "wait_executor_event")
    graph.add_edge("wait_executor_event", END)
    return graph.compile(checkpointer=checkpointer)
