from __future__ import annotations

from typing import Any

from pydantic import TypeAdapter

from ex_agent.application.ports import WorkflowServices
from ex_agent.domain.contracts import ResumeSignal

_resume_adapter = TypeAdapter(ResumeSignal)


class WorkflowNodeGroup:
    def __init__(self, services: WorkflowServices) -> None:
        self._services = services


def validate_resume_signal(raw: Any) -> ResumeSignal:
    return _resume_adapter.validate_python(raw)


def persisted_plan_updates(
    plan: Any,
    persisted: Any,
) -> dict[str, Any]:
    return {
        "plan": plan,
        "plan_id": str(persisted.plan_id),
        "plan_revision_id": str(persisted.plan_revision_id),
        "plan_revision_number": persisted.plan_revision_number,
        "plan_public_payload_hash": persisted.public_payload_hash,
        "compiled_bundle_id": str(persisted.compiled_bundle_id),
    }


__all__ = [
    "WorkflowNodeGroup",
    "persisted_plan_updates",
    "validate_resume_signal",
]
