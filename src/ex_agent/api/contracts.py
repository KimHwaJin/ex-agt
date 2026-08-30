from __future__ import annotations

from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from ex_agent.domain.contracts import ResourceAuditFields


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ErrorResponse(ApiModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={"example": {"detail": "Request failed"}},
    )

    detail: str | dict[str, Any] | list[dict[str, Any]]


def error_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    return {
        status: {
            "model": ErrorResponse,
            "description": f"HTTP {status} error",
        }
        for status in statuses
    }


class TaskCreateRequest(ApiModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "task_id": "00000000-0000-4000-8000-000000000001",
                "input_message_id": ("00000000-0000-4000-8000-000000000002"),
                "content": "지난달 매출 데이터를 분석해줘",
                "idempotency_key": "task-create-0001",
            }
        },
    )

    task_id: UUID
    input_message_id: UUID
    content: str = Field(min_length=1, max_length=100000)
    idempotency_key: str = Field(min_length=1, max_length=255)


class ResumeRequest(ApiModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    signal: dict[str, Any]


class CancelRequest(ApiModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "task-cancel-0001",
                "reason": "사용자 요청",
            }
        },
    )

    idempotency_key: str = Field(min_length=1, max_length=255)
    reason: str | None = Field(default=None, max_length=1000)


class TaskAcceptedResponse(ResourceAuditFields):
    task_id: UUID
    status: str


class TaskResponse(ResourceAuditFields):
    task_id: UUID
    user_id: str
    project_id: str
    session_id: str
    status: str
    execution_id: UUID | None
    current_interrupt: dict[str, Any] | None
    terminal_message: str | None
    version: int
