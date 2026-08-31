from __future__ import annotations

import hashlib
from typing import Annotated, Any, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, ConfigDict, Field

from ex_agent.application.state import WorkflowState


class TaskTurn(BaseModel):
    """Trusted host input, not an arbitrary graph-state patch from the UI."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    user_id: str = Field(min_length=1)
    project_id: str = Field(min_length=1)
    session_id: str = Field(min_length=1)
    active_task_id: str = Field(min_length=1)
    current_input_message_id: str = Field(min_length=1)
    user_message: str = Field(min_length=1)
    runtime_profile: str = Field(default="basic", min_length=1)

    @property
    def fingerprint(self) -> str:
        return hashlib.sha256(self.model_dump_json().encode()).hexdigest()


class SessionInput(TypedDict):
    turn: dict[str, Any]


class SessionState(SessionInput, total=False):
    schema_version: str
    user_id: str
    project_id: str
    session_id: str
    active_task_id: str
    execution_id: str
    # Whole task is replaced at admission, never merged with an older task.
    workflow: WorkflowState
    messages: Annotated[list[AnyMessage], add_messages]
    task_requests: dict[str, str]
    ew_pending: dict[str, Any]
    ew_receipts: dict[str, str]
    ew_sequences: dict[str, int]
