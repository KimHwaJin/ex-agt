from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TaskCreateRequest(ApiModel):
    task_id: UUID
    input_message_id: UUID
    content: str = Field(min_length=1, max_length=100000)
    idempotency_key: str = Field(min_length=1, max_length=255)


class ResumeRequest(ApiModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    signal: dict[str, Any]


class CancelRequest(ApiModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    reason: str | None = Field(default=None, max_length=1000)


class TaskAcceptedResponse(ApiModel):
    task_id: UUID
    status: str


class TaskResponse(ApiModel):
    task_id: UUID
    user_id: str
    project_id: str
    session_id: str
    status: str
    execution_id: UUID | None
    current_interrupt: dict[str, Any] | None
    terminal_message: str | None
    version: int
