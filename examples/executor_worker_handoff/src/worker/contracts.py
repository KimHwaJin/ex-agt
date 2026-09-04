from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class ExecutorEvent(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True)

    event_id: UUID
    execution_id: UUID
    event_type: str = Field(min_length=1)
    event_sequence: int = Field(ge=1)
    schema_version: Literal["1.0"]
    occurred_at: str
    payload: dict[str, Any]

    @classmethod
    def from_redis(cls, fields: dict[str, str]) -> ExecutorEvent:
        return cls.model_validate(
            {**fields, "payload": json.loads(fields["payload"])}
        )


@dataclass(frozen=True)
class EventContext:
    namespace: str
    session_id: str
    task_id: str
    execution_id: UUID
    command_id: UUID
    event: ExecutorEvent

    @property
    def graph_config(self) -> dict[str, Any]:
        return {"configurable": {"thread_id": self.session_id}}


EventHandler = Callable[[EventContext], Awaitable[None]]


class DeferEvent(Exception):
    """Not ready yet: retain pending without spending a failure attempt."""


class RejectEvent(Exception):
    """Permanent business failure: persist FAILED and move to DLQ."""


class IgnoreEvent(Exception):
    """Explicitly record an obsolete event without resuming the graph."""
