from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from agent.effects.store import digest
from agent.graph.state import TaskTurn
from ex_agent.domain.contracts import (
    CancelRequestedSignal,
    ExecutorBoundarySignal,
)
from ex_agent.graph.node_groups.common import validate_resume_signal


class ApiRequest(BaseModel):
    """Trusted host input; authenticate/authorize before constructing this."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    request_id: UUID
    turn: TaskTurn
    kind: Literal["START", "RESUME", "CANCEL"]
    interrupt_id: str | None = Field(default=None, min_length=1)
    payload: dict[str, Any] | None = None

    @model_validator(mode="after")
    def validate_request(self):
        UUID(self.turn.active_task_id)
        UUID(self.turn.current_input_message_id)
        if self.kind == "START":
            if self.interrupt_id is not None or self.payload is not None:
                raise ValueError("START cannot carry a resume payload")
        else:
            if self.interrupt_id is None or self.payload is None:
                raise ValueError("Resume requires interrupt ID and payload")
            signal = validate_resume_signal(self.payload)
            if isinstance(signal, ExecutorBoundarySignal):
                raise ValueError("Executor events belong to Worker")
            if isinstance(signal, CancelRequestedSignal) != (
                self.kind == "CANCEL"
            ):
                raise ValueError("Cancel signal requires CANCEL request kind")
        return self

    @property
    def fingerprint(self) -> str:
        return digest(self.model_dump(mode="json"))


class RequestRecord(BaseModel):
    model_config = ConfigDict(frozen=True)
    command: ApiRequest
    fingerprint: str
    target_node: str
    base_checkpoint_id: str | None
    state: Literal["PENDING", "RUNNING", "APPLIED", "REJECTED", "BLOCKED"]
    attempts: int
    next_attempt_at: datetime
    last_error: str | None
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str

    @property
    def action(self) -> dict[str, str]:
        return {
            "request_id": str(self.command.request_id),
            "task_id": self.command.turn.active_task_id,
            "fingerprint": self.fingerprint,
            "target_node": self.target_node,
        }
