from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from pydantic import BaseModel

from ex_agent.application.promotions import WorkflowPromotionService
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    WorkflowLifecycleResult,
    WorkflowStatusRequest,
    WorkflowVersionActivationRequest,
    WorkflowVersionCreateRequest,
    WorkflowVersionReviewRequest,
)
from ex_agent.persistence.models import Workflow
from ex_agent.persistence.repository import AgentRepository


class WorkflowLifecycleForbiddenError(RuntimeError):
    pass


@dataclass(frozen=True)
class WorkflowLifecyclePolicyDecision:
    allowed: bool
    policy_version: str
    reason: str


class WorkflowLifecyclePolicy(Protocol):
    def evaluate(
        self,
        *,
        actor_user_id: str,
        workflow: Workflow,
        operation: str,
    ) -> WorkflowLifecyclePolicyDecision: ...


class OwnerWorkflowLifecyclePolicy:
    """V1 owner policy with an explicit future authorization port."""

    version = "workflow-owner-v1"

    def evaluate(
        self,
        *,
        actor_user_id: str,
        workflow: Workflow,
        operation: str,
    ) -> WorkflowLifecyclePolicyDecision:
        del operation
        allowed = bool(actor_user_id) and (
            workflow.owner_user_id == actor_user_id
        )
        return WorkflowLifecyclePolicyDecision(
            allowed=allowed,
            policy_version=self.version,
            reason=(
                "authenticated Workflow owner"
                if allowed
                else "actor does not own the Workflow"
            ),
        )


class WorkflowLifecycleService:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        promotions: WorkflowPromotionService,
        *,
        policy: WorkflowLifecyclePolicy | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._promotions = promotions
        self._policy = policy or OwnerWorkflowLifecyclePolicy()

    async def create_version(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        request: WorkflowVersionCreateRequest,
    ) -> WorkflowLifecycleResult:
        action = "VERSION_CREATED"
        request_hash = _request_hash(request)
        workflow, decision = await self._authorized(
            workflow_id,
            actor_user_id=actor_user_id,
            operation=action,
        )
        existing = await self._existing(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            action=action,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if existing is not None:
            return existing
        source = await self._promotions.eligible_source(
            request.source_task_id,
            actor_user_id,
        )
        prepared = await self._promotions.prepare_version(
            source=source,
            name=workflow.name,
            description=workflow.description,
            request_examples=request.request_examples,
            tags=request.tags,
            public_parameter_defaults=request.public_parameter_defaults,
        )
        return await self._repository.create_workflow_version(
            workflow_id=workflow_id,
            source=source,
            actor_user_id=actor_user_id,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            request_examples=request.request_examples,
            tags=request.tags,
            plan=prepared.plan,
            input_contract=prepared.input_contract,
            embedding=prepared.embedding,
            embedding_model=self._settings.agent_embedding_model,
            registry_snapshot_hash=prepared.registry_snapshot_hash,
            policy_version=decision.policy_version,
            searchable_text=prepared.searchable_text,
        )

    async def review_version(
        self,
        workflow_id: UUID,
        workflow_version_id: UUID,
        *,
        actor_user_id: str,
        request: WorkflowVersionReviewRequest,
    ) -> WorkflowLifecycleResult:
        action = (
            "VERSION_APPROVED"
            if request.decision == "APPROVE"
            else "VERSION_REJECTED"
        )
        request_hash = _request_hash(
            request,
            workflow_version_id=workflow_version_id,
        )
        _workflow, decision = await self._authorized(
            workflow_id,
            actor_user_id=actor_user_id,
            operation=action,
        )
        existing = await self._existing(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            action=action,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if existing is not None:
            return existing
        return await self._repository.review_workflow_version(
            workflow_id=workflow_id,
            workflow_version_id=workflow_version_id,
            actor_user_id=actor_user_id,
            decision=request.decision,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            policy_version=decision.policy_version,
        )

    async def activate_version(
        self,
        workflow_id: UUID,
        workflow_version_id: UUID,
        *,
        actor_user_id: str,
        request: WorkflowVersionActivationRequest,
    ) -> WorkflowLifecycleResult:
        action = "VERSION_ACTIVATED"
        request_hash = _request_hash(
            request,
            workflow_version_id=workflow_version_id,
        )
        _workflow, decision = await self._authorized(
            workflow_id,
            actor_user_id=actor_user_id,
            operation=action,
        )
        existing = await self._existing(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            action=action,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if existing is not None:
            return existing
        return await self._repository.activate_workflow_version(
            workflow_id=workflow_id,
            workflow_version_id=workflow_version_id,
            actor_user_id=actor_user_id,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            policy_version=decision.policy_version,
        )

    async def update_status(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        request: WorkflowStatusRequest,
    ) -> WorkflowLifecycleResult:
        action = (
            "WORKFLOW_ACTIVATED"
            if request.status == "ACTIVE"
            else "WORKFLOW_DEACTIVATED"
        )
        request_hash = _request_hash(request)
        _workflow, decision = await self._authorized(
            workflow_id,
            actor_user_id=actor_user_id,
            operation=action,
        )
        existing = await self._existing(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            action=action,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if existing is not None:
            return existing
        return await self._repository.update_workflow_status(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            status=request.status,
            reason=request.reason,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            policy_version=decision.policy_version,
        )

    async def _existing(
        self,
        *,
        workflow_id: UUID,
        actor_user_id: str,
        action: str,
        idempotency_key: str,
        request_hash: str,
    ) -> WorkflowLifecycleResult | None:
        return await self._repository.existing_workflow_lifecycle_action(
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            action=action,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
        )

    async def _authorized(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        operation: str,
    ) -> tuple[Workflow, WorkflowLifecyclePolicyDecision]:
        workflow = await self._repository.lifecycle_workflow(workflow_id)
        decision = self._policy.evaluate(
            actor_user_id=actor_user_id,
            workflow=workflow,
            operation=operation,
        )
        if not decision.allowed:
            raise WorkflowLifecycleForbiddenError(decision.reason)
        return workflow, decision


def _request_hash(request: BaseModel, **context: Any) -> str:
    payload = {
        "request": request.model_dump(mode="json"),
        "context": context,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "OwnerWorkflowLifecyclePolicy",
    "WorkflowLifecycleForbiddenError",
    "WorkflowLifecyclePolicy",
    "WorkflowLifecyclePolicyDecision",
    "WorkflowLifecycleService",
]
