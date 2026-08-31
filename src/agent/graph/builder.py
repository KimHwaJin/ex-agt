from __future__ import annotations

from typing import Any, cast, get_args, get_type_hints

from langgraph.graph import END, START, StateGraph

from agent.admission.graph import with_api_receipt
from agent.failure.graph import failure_settled
from agent.graph.nodes import (
    ExecutionBindings,
    WorkerBoundaryNodes,
    begin_task,
    lift_node,
    lift_route,
)
from agent.graph.state import SessionInput, SessionState
from ex_agent.application.ports import WorkflowServices
from ex_agent.graph.nodes import WorkflowNodes
from ex_agent.graph.topology import EDGES, NODES, ROUTES


def build_session_graph(
    services: WorkflowServices,
    bindings: ExecutionBindings,
    *,
    checkpointer: Any,
) -> Any:
    """Build the session graph; host must serialize all calls with guard.

    Uses existing business capabilities during migration. Production host
    wiring is intentionally separate until external-effect recovery and
    durable API admission have been verified.
    """
    if checkpointer is None or checkpointer is False:
        raise ValueError("The session graph requires a checkpointer")
    nodes = WorkflowNodes(services)
    boundary = WorkerBoundaryNodes(bindings)
    graph = StateGraph(
        cast(Any, SessionState), input_schema=cast(Any, SessionInput)
    )
    graph.add_node("begin_task", with_api_receipt("begin_task", begin_task))
    for name in NODES:
        node = (
            boundary.wait_external_signal
            if name == "wait_external_signal"
            else lift_node(getattr(nodes, name))
        )
        graph.add_node(name, with_api_receipt(name, node))
    graph.add_node("register_execution", boundary.register_execution)
    graph.add_node("record_event_receipt", boundary.record_event_receipt)
    graph.add_node("failure_settled", failure_settled)
    graph.add_edge("failure_settled", END)
    graph.add_edge(START, "begin_task")
    graph.add_edge("begin_task", "hydrate_turn")
    for source, target in EDGES:
        if source == START:
            continue
        if source == "submit_execution":
            graph.add_edge(source, "register_execution")
            graph.add_edge("register_execution", target)
        else:
            graph.add_edge(source, target)
    graph.add_edge("reconcile_executor", "record_event_receipt")
    for source, route in ROUTES.items():
        if source == "reconcile_executor":
            source = "record_event_receipt"
        # Preserve Literal destinations after adapting the nested State.
        destinations = list(get_args(get_type_hints(route)["return"]))
        graph.add_conditional_edges(source, lift_route(route), destinations)
    return graph.compile(checkpointer=checkpointer)
