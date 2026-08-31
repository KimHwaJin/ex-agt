from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ExecutorBoundarySignal(BaseModel):
    """Minimal resume contract, independent of ex-agent's analysis domain."""

    model_config = ConfigDict(extra="forbid")
    type: Literal["EXECUTOR_BOUNDARY"] = "EXECUTOR_BOUNDARY"
    execution_id: UUID
    event_id: UUID
    event_sequence: int = Field(ge=1)
    event_type: Literal[
        "execution.operation_completed",
        "execution.completed",
    ]
