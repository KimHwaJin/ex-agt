"""Build the minimal Agent-to-Executor wait and resume graph."""

from __future__ import annotations

from typing import Any, cast

from langgraph.graph import END, START, StateGraph

from agent.graph.nodes import (
    create_task,
    handle_executor_event,
    submit_sample_execution,
)
from agent.graph.state import AgentInput, AgentState
from agent_worker.graph_boundary import (
    ExecutionBindings,
    ExecutorBoundaryNodes,
)


def build_graph(
    *,
    bindings: ExecutionBindings,
    checkpointer: Any,
) -> Any:
    """Compile a one-event example using the real Worker boundary."""

    if checkpointer is None or checkpointer is False:
        raise ValueError("A checkpointer is required")

    boundary = ExecutorBoundaryNodes(bindings)
    builder = StateGraph(
        cast(Any, AgentState),
        input_schema=cast(Any, AgentInput),
    )
    builder.add_node("create_task", create_task)
    builder.add_node("submit_execution", submit_sample_execution)
    builder.add_node("register_execution", boundary.register_execution)
    builder.add_node("wait_executor_event", boundary.wait_executor_event)
    builder.add_node("handle_executor_event", handle_executor_event)
    builder.add_node(
        "record_executor_receipt",
        boundary.record_executor_receipt,
    )

    builder.add_edge(START, "create_task")
    builder.add_edge("create_task", "submit_execution")
    builder.add_edge("submit_execution", "register_execution")
    builder.add_edge("register_execution", "wait_executor_event")
    builder.add_edge("wait_executor_event", "handle_executor_event")
    builder.add_edge("handle_executor_event", "record_executor_receipt")
    builder.add_edge("record_executor_receipt", END)
    return builder.compile(checkpointer=checkpointer)
