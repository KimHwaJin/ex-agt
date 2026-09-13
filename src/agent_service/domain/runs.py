"""Public run contracts; no framework State or raw commands are accepted."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .management import Audit

RunStatus = Literal[
    "queued",
    "running",
    "awaiting_input",
    "waiting_execution",
    "cancelling",
    "completed",
    "rejected",
    "failed",
    "cancelled",
]
TERMINAL = frozenset({"completed", "rejected", "failed", "cancelled"})


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TextBlock(StrictModel):
    type: Literal["text"]
    text: str = Field(min_length=1, max_length=16000)


class MessageInput(StrictModel):
    type: Literal["message"]
    content: list[TextBlock] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def nonblank(self) -> "MessageInput":
        if not any(block.text.strip() for block in self.content):
            raise ValueError("Message cannot be blank")
        if sum(len(block.text) for block in self.content) > 32000:
            raise ValueError("Message exceeds 32000 characters")
        return self


class Approval(StrictModel):
    type: Literal["plan_review"]
    schema_version: Literal[1] = 1
    decision: Literal["approve", "reject"]


class Modification(StrictModel):
    type: Literal["plan_review"]
    schema_version: Literal[1] = 1
    decision: Literal["modify"]
    instruction: str = Field(min_length=1, max_length=8000)

    @model_validator(mode="after")
    def nonblank(self) -> "Modification":
        if not self.instruction.strip():
            raise ValueError("Modification cannot be blank")
        return self


PlanResponse = Annotated[
    Approval | Modification, Field(discriminator="decision")
]


class ResumeInput(StrictModel):
    type: Literal["resume"]
    run_id: UUID
    interrupt_id: UUID
    response: PlanResponse


class RunRequest(StrictModel):
    user_id: UUID
    session_id: UUID
    stream: bool = Field(default=False, strict=True)
    input: Annotated[MessageInput | ResumeInput, Field(discriminator="type")]


class RunReceipt(BaseModel):
    run_id: UUID
    session_id: UUID
    status: RunStatus


class Message(Audit):
    message_id: UUID
    session_id: UUID
    run_id: UUID
    role: Literal["user", "assistant"]
    event_sequence: int = 0
    content: list[TextBlock]
    status: Literal["streaming", "completed", "interrupted", "failed"]


class RunSummary(Audit):
    run_id: UUID
    session_id: UUID
    project_id: UUID
    status: RunStatus
    backend: str


class PendingInterrupt(Audit):
    interrupt_id: UUID
    response_type: str
    schema_version: int
    payload: dict


class ExecutionBinding(Audit):
    binding_id: UUID
    execution_id: UUID | None
    idempotency_key: str
    status: str
    simulated: bool


class RunError(BaseModel):
    code: str
    message: str


class RunDetail(RunSummary):
    executions: list[ExecutionBinding]
    pending_interrupts: list[PendingInterrupt]
    error: RunError | None
    last_event_id: str


class RunEvent(BaseModel):
    run_id: UUID
    sequence: int
    type: str
    data: dict
    occurred_at: datetime

    @property
    def event_id(self) -> str:
        return f"{self.run_id}:{self.sequence}"
