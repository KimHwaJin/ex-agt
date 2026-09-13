"""Checkpointer is supplied by the owner of the live DB connection."""

from langgraph.graph import END, START, StateGraph

from .nodes import complete, response_node
from .state import AssistantState

GRAPH_VERSION = "assistant-v1"


def build_graph(agent, checkpointer, message_limit=40):
    # ty currently cannot match TypedDict classes to LangGraph's protocol.
    builder = StateGraph(AssistantState)  # ty: ignore[invalid-argument-type]
    builder.add_node("respond", response_node(agent, message_limit))
    builder.add_node("complete", complete)
    builder.add_edge(START, "respond")
    builder.add_edge("respond", "complete")
    builder.add_edge("complete", END)
    return builder.compile(checkpointer=checkpointer, name=GRAPH_VERSION)
