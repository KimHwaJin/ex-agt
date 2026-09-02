from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import ConfigDict, Field

from ex_agent.domain.contracts import ContractModel, ResourceAuditFields


class StreamMaintenanceRequest(ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "stream": "executor_events",
                "idempotency_key": "stream-trim-2026-09-02-001",
                "reason": "주간 보존 정책 정리",
                "retention_seconds": 604800,
                "minimum_retained_entries": 1000,
            }
        },
    )

    stream: str = Field(min_length=1, max_length=64)
    idempotency_key: str = Field(min_length=1, max_length=255)
    reason: str = Field(min_length=1, max_length=2000)
    retention_seconds: int | None = Field(
        default=None,
        ge=1,
        le=31536000,
    )
    minimum_retained_entries: int | None = Field(
        default=None,
        ge=0,
        le=1000000,
    )


class StreamMaintenanceJobView(ResourceAuditFields):
    job_id: UUID
    stream: str
    action: str
    state: str
    reason: str
    retention_seconds: int
    minimum_retained_entries: int
    attempts: int
    next_attempt_at: datetime
    result: dict[str, Any] | None
    last_error: str | None


class StreamMaintenanceOperationResult(ContractModel):
    job: StreamMaintenanceJobView
    operation_replayed: bool


class StreamMaintenancePage(ContractModel):
    items: list[StreamMaintenanceJobView]
    next_cursor: str | None = None
    has_more: bool


__all__ = [
    "StreamMaintenanceJobView",
    "StreamMaintenanceOperationResult",
    "StreamMaintenancePage",
    "StreamMaintenanceRequest",
]
