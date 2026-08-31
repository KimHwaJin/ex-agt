from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID

from ex_agent.transport.consumer import PermanentMessageError, StreamMessage


class CommandState(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    DONE = "DONE"


@dataclass(frozen=True)
class ExternalEvent:
    event_id: UUID
    workflow_id: str
    sequence: int
    event_type: str
    payload: dict[str, Any]

    @classmethod
    def from_message(cls, message: StreamMessage) -> ExternalEvent:
        fields = message.fields
        try:
            payload = json.loads(fields.get("payload", "{}"))
            if not isinstance(payload, dict):
                raise TypeError("payload must be a JSON object")
            event = cls(
                event_id=UUID(fields["event_id"]),
                workflow_id=fields["workflow_id"],
                sequence=int(fields["sequence"]),
                event_type=fields["event_type"],
                payload=payload,
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise PermanentMessageError(
                f"Invalid external event: {error}"
            ) from error
        if not event.workflow_id or not event.event_type:
            raise PermanentMessageError(
                "workflow_id and event_type must not be empty"
            )
        if event.sequence < 1:
            raise PermanentMessageError("sequence must be positive")
        return event


@dataclass(frozen=True)
class WorkflowCommand:
    command_id: UUID
    workflow_id: str
    command_type: str
    payload: dict[str, Any]
    state: CommandState = CommandState.PENDING
    attempt_count: int = 0
    last_error: str | None = None

    @classmethod
    def from_message(cls, message: StreamMessage) -> WorkflowCommand:
        fields = message.fields
        try:
            payload = json.loads(fields.get("payload", "{}"))
            if not isinstance(payload, dict):
                raise TypeError("payload must be a JSON object")
            command = cls(
                command_id=UUID(fields["command_id"]),
                workflow_id=fields["workflow_id"],
                command_type=fields["command_type"],
                payload=payload,
            )
        except (
            KeyError,
            TypeError,
            ValueError,
            json.JSONDecodeError,
        ) as error:
            raise PermanentMessageError(
                f"Invalid workflow command: {error}"
            ) from error
        if not command.workflow_id or not command.command_type:
            raise PermanentMessageError(
                "workflow_id and command_type must not be empty"
            )
        return command

    def stream_fields(self) -> dict[str, str]:
        return {
            "command_id": str(self.command_id),
            "workflow_id": self.workflow_id,
            "command_type": self.command_type,
            "payload": json.dumps(
                self.payload,
                ensure_ascii=False,
                sort_keys=True,
            ),
        }
