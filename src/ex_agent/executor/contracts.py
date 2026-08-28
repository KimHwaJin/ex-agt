from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ExecutorModel(BaseModel):
    model_config = ConfigDict(extra="allow")


class ExecutorState(ExecutorModel):
    status: str
    version: int = Field(ge=0)


class OperationReceipt(ExecutorModel):
    operation_id: UUID


class CommandResponse(ExecutorModel):
    execution_id: UUID
    operation: OperationReceipt | None
    state: ExecutorState


class StepResult(ExecutorModel):
    status: str
    output_summary: dict[str, Any] = Field(default_factory=dict)
    error_message: str | None = None
    result_ref: dict[str, Any] | None = None


class ResultStep(ExecutorModel):
    step_id: UUID
    sequence: int
    lineage: dict[str, Any] = Field(default_factory=dict)
    result: StepResult


class OperationResult(ExecutorModel):
    status: str
    error_message: str | None = None


class ResultOperation(ExecutorModel):
    operation_id: UUID
    operation_number: int
    result: OperationResult
    steps: list[ResultStep]


class ResultExecution(ExecutorModel):
    execution_id: UUID
    state: ExecutorState


class ExecutionResult(ExecutorModel):
    execution: ResultExecution
    operations: list[ResultOperation]
    attempts: list[dict[str, Any]] = Field(default_factory=list)
    artifacts: list[dict[str, Any]] = Field(default_factory=list)


class ArtifactResponse(ExecutorModel):
    artifact_id: UUID


class ExecutorEvent(ExecutorModel):
    event_id: UUID
    event_type: Literal[
        "execution.started",
        "execution.operation_started",
        "execution.step_started",
        "execution.step_completed",
        "execution.operation_completed",
        "execution.completed",
    ]
    schema_version: Literal["1.0"]
    execution_id: UUID
    event_sequence: int = Field(ge=1)
    payload: dict[str, Any]
    occurred_at: str

    @classmethod
    def from_redis(cls, fields: dict[str, str]) -> ExecutorEvent:
        import json

        return cls.model_validate(
            {**fields, "payload": json.loads(fields["payload"])}
        )


class ExecutorEventPage(ExecutorModel):
    items: list[ExecutorEvent]
    next_cursor: str | None = None
    has_more: bool = False


def executor_step_payload(
    *,
    sequence: int,
    path: str,
    sha256: str,
    timeout_seconds: int,
    skill_name: str | None,
    tool_name: str | None,
    parameters: dict[str, Any],
) -> dict[str, Any]:
    return {
        "sequence": sequence,
        "payload": {
            "type": "PYTHON_EXECUTE",
            "source": {
                "type": "PATH",
                "path": path,
                "sha256": sha256,
            },
        },
        "step_timeout_seconds": timeout_seconds,
        "lineage": {
            "skill_name": skill_name,
            "tool_name": tool_name,
            "input_parameters": parameters,
        },
    }
