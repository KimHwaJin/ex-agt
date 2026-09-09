from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
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

    def identity_document(self) -> dict[str, Any]:
        """Compare immutable envelope fields, not REST delivery metadata.

        Keep the public event and stored JSON unchanged. Normalizing both
        operands also supports Inbox rows written by older Workers.
        """
        data = self.model_dump(
            mode="json", include=set(ExecutorEvent.model_fields)
        )
        occurred_at = datetime.fromisoformat(self.occurred_at)
        if occurred_at.tzinfo is None:
            raise ValueError("Executor occurred_at requires a timezone")
        data["occurred_at"] = occurred_at.astimezone(UTC).isoformat(
            timespec="microseconds"
        )
        # JSONB can expand 1e24 to an integer. Decimal keeps the JSON numeric
        # value stable; a tuple tag keeps numbers distinct from booleans
        # and from user-provided JSON arrays. Never expose this internal
        # comparison document as an event or store it in the database.
        return json.loads(
            json.dumps(data, allow_nan=False),
            parse_int=_number_identity,
            parse_float=_number_identity,
        )

    @classmethod
    def from_redis(cls, fields: dict[str, str]) -> ExecutorEvent:
        return cls.model_validate(
            {**fields, "payload": json.loads(fields["payload"])}
        )


def _number_identity(value: str) -> tuple[Decimal]:
    return (Decimal(value),)


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
