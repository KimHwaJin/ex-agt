"""Identifiers come from validated Run storage, never client graph commands."""

from typing import Annotated

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class AssistantState(TypedDict):
    messages: Annotated[list[AnyMessage], add_messages]
    run_id: str
    answer: str
    completed_run_id: str | None
