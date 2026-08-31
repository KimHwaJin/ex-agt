from __future__ import annotations

from typing import Any, cast

from langgraph.graph import StateGraph

from ex_agent.application.ports import WorkflowServices
from ex_agent.graph.nodes import WorkflowNodes
from ex_agent.graph.state import AgentGraphState
from ex_agent.graph.topology import EDGES, NODES, ROUTES


def build_workflow_graph(
    services: WorkflowServices,
    *,
    checkpointer: Any = None,
) -> Any:
    nodes = WorkflowNodes(services)
    graph = StateGraph(cast(Any, AgentGraphState))
    for name in NODES:
        graph.add_node(name, getattr(nodes, name))
    for source, target in EDGES:
        graph.add_edge(source, target)
    for source, route in ROUTES.items():
        graph.add_conditional_edges(source, route)
    return graph.compile(checkpointer=checkpointer)
