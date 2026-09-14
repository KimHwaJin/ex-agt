"""Our provisional v1 protocol, not an assertion of Gaia A2A compatibility."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from agent_service.domain.runs import RunRequest, StrictModel


class WorkflowRequest(StrictModel):
    protocol_version: Literal[1] = 1
    request_id: str = Field(
        min_length=1, max_length=200, pattern=r"^[A-Za-z0-9._:-]+$"
    )
    request: RunRequest


class AuthorizedRequest(WorkflowRequest):
    # Set by trusted host integration, never copied from the client blindly.
    owner: UUID


def decode_v1(payload: dict, *, owner: UUID) -> AuthorizedRequest:
    """Host authorizes first, then decodes the explicit agent_request field.

    This envelope avoids interpreting template message/parts text as approval.
    Employee IDs and external sessions must be resolved by the host adapter.
    """
    envelope = WorkflowRequest.model_validate(payload.get("agent_request"))
    return AuthorizedRequest(**envelope.model_dump(), owner=owner)


def input_request(interrupt: dict, run_id: str) -> dict:
    """Keep UI presentation separate from the existing resume contract."""
    return {
        "protocol_version": 1,
        "type": "input_request",
        "pattern": "review",
        "stage": "plan_review",
        "question": "실행 계획을 승인·수정·거절해 주세요.",
        "run_id": run_id,
        "interrupt_id": interrupt["interrupt_id"],
        "response_type": interrupt["response_type"],
        "schema_version": interrupt["schema_version"],
        "payload": interrupt["payload"],
    }
