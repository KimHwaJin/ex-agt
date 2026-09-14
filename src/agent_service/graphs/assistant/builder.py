"""Checkpointer is supplied by the owner of the live DB connection."""

from langgraph.graph import END, START, StateGraph

from .nodes import complete, response_node
from .preparation import preparation_node
from .review import (
    classification_node,
    next_after_classification,
    next_after_review,
    planning_node,
    review_outcome,
    review_plan,
)
from .state import WorkflowState

GRAPH_VERSION = "assistant-v3"
SUPPORTED_VERSIONS = {"assistant-v1", "assistant-v2", GRAPH_VERSION}


def build_graph(
    agent,
    checkpointer,
    message_limit=40,
    *,
    router=None,
    planner=None,
    code_planners=None,
    version=GRAPH_VERSION,
):
    # ty currently cannot match TypedDict classes to LangGraph's protocol.
    builder = StateGraph(WorkflowState)  # ty: ignore[invalid-argument-type]
    builder.add_node("respond", response_node(agent, message_limit))
    builder.add_node("complete", complete)
    if version == "assistant-v1":
        builder.add_edge(START, "respond")
    elif (
        version in {"assistant-v2", GRAPH_VERSION}
        and router is not None
        and planner is not None
    ):
        builder.add_node("classify", classification_node(router))
        builder.add_node("plan", planning_node(planner))
        builder.add_node("review_plan", review_plan)
        builder.add_node("review_outcome", review_outcome)
        builder.add_edge(START, "classify")
        builder.add_conditional_edges(
            "classify", next_after_classification, ["respond", "plan"]
        )
        if version == GRAPH_VERSION:
            if code_planners is None:
                raise ValueError("Code planning agents are required")
            builder.add_node("prepare_code", preparation_node(code_planners))
            builder.add_edge("plan", "prepare_code")
            builder.add_edge("prepare_code", "review_plan")
        else:
            builder.add_edge("plan", "review_plan")
        builder.add_conditional_edges(
            "review_plan", next_after_review, ["plan", "review_outcome"]
        )
        builder.add_edge("review_outcome", "complete")
    else:
        raise ValueError("Unsupported graph version or missing intake agents")
    builder.add_edge("respond", "complete")
    builder.add_edge("complete", END)
    return builder.compile(checkpointer=checkpointer, name=version)
