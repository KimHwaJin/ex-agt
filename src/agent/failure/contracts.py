"""Authenticated operator views for durable failure cleanup."""

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import ConfigDict, Field

from ex_agent.domain.contracts import ContractModel, ResourceAuditFields


class FailureCleanupView(ResourceAuditFields):
    task_id: UUID
    session_id: str
    state: str
    version: int
    reason: str
    source: dict[str, Any]
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    execution_id: UUID | None
    executor_status: str | None
    evidence_complete: bool
    preserve_terminal: bool
    final_status: str
    message: str | None
    last_operation_id: UUID | None
    last_operation_action: str | None
    last_operation_reason: str | None
    last_operation_at: datetime | None
    last_operation_by: str | None


class FailureCleanupPage(ContractModel):
    items: list[FailureCleanupView]
    next_cursor: str | None = None
    has_more: bool


class FailureOperationResult(ContractModel):
    cleanup: FailureCleanupView
    operation_replayed: bool


class FailureOperationInput(ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "failure-retry-0001",
                "expected_version": 3,
                "reason": "Executor 상태 확인 후 운영자 재시도",
            }
        },
    )

    idempotency_key: str = Field(min_length=1, max_length=255)
    expected_version: int = Field(ge=1)
    reason: str = Field(min_length=1, max_length=2000)


__all__ = [
    "FailureCleanupPage",
    "FailureCleanupView",
    "FailureOperationInput",
    "FailureOperationResult",
]
