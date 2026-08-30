from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import PlanDraft, WorkflowLifecycleResult
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    TaskEvent,
    Workflow,
    WorkflowLifecycleAction,
    WorkflowVersion,
)
from ex_agent.persistence.repositories.promotions import (
    PromotionSource,
    add_workflow_steps,
)


class WorkflowLifecycleConflictError(RuntimeError):
    pass


class WorkflowLifecycleRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def workflow(self, workflow_id: UUID) -> Workflow:
        async with self._sessions() as session:
            workflow = await session.get(Workflow, workflow_id)
            if workflow is None:
                raise LookupError(f"Unknown Workflow: {workflow_id}")
            return workflow

    async def existing(
        self,
        *,
        workflow_id: UUID,
        actor_user_id: str,
        action: str,
        idempotency_key: str,
        request_hash: str,
    ) -> WorkflowLifecycleResult | None:
        async with self._sessions() as session:
            row = await session.scalar(
                select(WorkflowLifecycleAction).where(
                    WorkflowLifecycleAction.idempotency_key == idempotency_key
                )
            )
            if row is None:
                return None
            return _existing_result(
                row,
                workflow_id=workflow_id,
                actor_user_id=actor_user_id,
                action=action,
                request_hash=request_hash,
            )

    async def create_version(
        self,
        *,
        workflow_id: UUID,
        source: PromotionSource,
        actor_user_id: str,
        idempotency_key: str,
        request_hash: str,
        request_examples: list[str],
        tags: list[str],
        plan: PlanDraft,
        input_contract: dict[str, dict[str, Any]],
        embedding: list[float],
        embedding_model: str,
        registry_snapshot_hash: str,
        policy_version: str,
        searchable_text: str,
    ) -> WorkflowLifecycleResult:
        async with transaction(self._sessions) as session:
            replay = await _locked_existing(
                session,
                workflow_id=workflow_id,
                actor_user_id=actor_user_id,
                action="VERSION_CREATED",
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            workflow = await _locked_workflow(session, workflow_id)
            next_version = workflow.latest_version + 1
            source_plan_ids = {row.source_plan_id for row in source.steps}
            source_revision_ids = {
                row.source_plan_revision_id for row in source.steps
            }
            payload = plan.model_dump(mode="json")
            public_payload_hash = _payload_hash(
                {"plan": payload, "input_contract": input_contract}
            )
            version = WorkflowVersion(
                workflow_id=workflow.id,
                version=next_version,
                source_task_id=source.task.id,
                source_plan_id=_only_value(source_plan_ids),
                source_plan_revision_id=_only_value(source_revision_ids),
                source_execution_id=source.task.execution_id,
                objective=plan.objective,
                strategy_summary=plan.strategy_summary,
                runtime_profile=plan.runtime_profile,
                input_contract=input_contract,
                output_contract={"artifacts": plan.expected_artifacts},
                tool_registry_snapshot_hash=registry_snapshot_hash,
                searchable_text=searchable_text,
                searchable_text_hash=hashlib.sha256(
                    searchable_text.encode()
                ).hexdigest(),
                embedding_model=embedding_model,
                embedding_dimension=len(embedding),
                request_examples=request_examples,
                tags=tags,
                promotion_policy_version=policy_version,
                plan_payload=payload,
                public_payload_hash=public_payload_hash,
                embedding=embedding,
                promoted_by=actor_user_id,
                active=False,
                review_status="PENDING_REVIEW",
            )
            session.add(version)
            workflow.latest_version = next_version
            await session.flush()
            add_workflow_steps(session, version.id, plan, source)
            result = WorkflowLifecycleResult.model_validate(
                {
                    "workflow_id": workflow.id,
                    "workflow_version_id": version.id,
                    "version": version.version,
                    "public_payload_hash": public_payload_hash,
                    "action": "VERSION_CREATED",
                    "workflow_status": workflow.status,
                    "review_status": version.review_status,
                    "version_active": version.active,
                    "applied": True,
                }
            )
            _add_action(
                session,
                result=result,
                actor_user_id=actor_user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                reason=None,
                policy_version=policy_version,
            )
            session.add(
                TaskEvent(
                    task_id=source.task.id,
                    event_type="workflow.version_created",
                    payload=result.model_dump(mode="json"),
                )
            )
        return result

    async def review_version(
        self,
        *,
        workflow_id: UUID,
        workflow_version_id: UUID,
        actor_user_id: str,
        decision: str,
        reason: str | None,
        idempotency_key: str,
        request_hash: str,
        policy_version: str,
    ) -> WorkflowLifecycleResult:
        action = (
            "VERSION_APPROVED" if decision == "APPROVE" else "VERSION_REJECTED"
        )
        async with transaction(self._sessions) as session:
            replay = await _locked_existing(
                session,
                workflow_id=workflow_id,
                actor_user_id=actor_user_id,
                action=action,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            workflow = await _locked_workflow(session, workflow_id)
            version = await _workflow_version(
                session,
                workflow_id,
                workflow_version_id,
            )
            if version.review_status != "PENDING_REVIEW":
                raise WorkflowLifecycleConflictError(
                    "Only PENDING_REVIEW versions can be reviewed"
                )
            now = datetime.now(UTC)
            version.review_status = (
                "APPROVED" if decision == "APPROVE" else "REJECTED"
            )
            version.reviewed_by = actor_user_id
            version.reviewed_at = now
            version.review_reason = reason
            if decision == "APPROVE":
                await _deactivate_versions(session, workflow.id)
                version.active = True
            result = _version_result(
                workflow,
                version,
                action=action,
            )
            _add_action(
                session,
                result=result,
                actor_user_id=actor_user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                reason=reason,
                policy_version=policy_version,
            )
        return result

    async def activate_version(
        self,
        *,
        workflow_id: UUID,
        workflow_version_id: UUID,
        actor_user_id: str,
        reason: str | None,
        idempotency_key: str,
        request_hash: str,
        policy_version: str,
    ) -> WorkflowLifecycleResult:
        async with transaction(self._sessions) as session:
            replay = await _locked_existing(
                session,
                workflow_id=workflow_id,
                actor_user_id=actor_user_id,
                action="VERSION_ACTIVATED",
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            workflow = await _locked_workflow(session, workflow_id)
            version = await _workflow_version(
                session,
                workflow_id,
                workflow_version_id,
            )
            if version.review_status != "APPROVED":
                raise WorkflowLifecycleConflictError(
                    "Only APPROVED versions can be activated"
                )
            await _deactivate_versions(session, workflow.id)
            version.active = True
            result = _version_result(
                workflow,
                version,
                action="VERSION_ACTIVATED",
            )
            _add_action(
                session,
                result=result,
                actor_user_id=actor_user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                reason=reason,
                policy_version=policy_version,
            )
        return result

    async def update_status(
        self,
        *,
        workflow_id: UUID,
        actor_user_id: str,
        status: str,
        reason: str | None,
        idempotency_key: str,
        request_hash: str,
        policy_version: str,
    ) -> WorkflowLifecycleResult:
        action = (
            "WORKFLOW_ACTIVATED"
            if status == "ACTIVE"
            else "WORKFLOW_DEACTIVATED"
        )
        async with transaction(self._sessions) as session:
            replay = await _locked_existing(
                session,
                workflow_id=workflow_id,
                actor_user_id=actor_user_id,
                action=action,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
            )
            if replay is not None:
                return replay
            workflow = await _locked_workflow(session, workflow_id)
            workflow.status = status
            result = WorkflowLifecycleResult.model_validate(
                {
                    "workflow_id": workflow.id,
                    "action": action,
                    "workflow_status": status,
                    "review_status": None,
                    "version_active": None,
                    "applied": True,
                }
            )
            _add_action(
                session,
                result=result,
                actor_user_id=actor_user_id,
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                reason=reason,
                policy_version=policy_version,
            )
        return result


async def _locked_workflow(
    session: AsyncSession,
    workflow_id: UUID,
) -> Workflow:
    workflow = await session.scalar(
        select(Workflow).where(Workflow.id == workflow_id).with_for_update()
    )
    if workflow is None:
        raise LookupError(f"Unknown Workflow: {workflow_id}")
    return workflow


async def _workflow_version(
    session: AsyncSession,
    workflow_id: UUID,
    version_id: UUID,
) -> WorkflowVersion:
    version = await session.scalar(
        select(WorkflowVersion).where(
            WorkflowVersion.id == version_id,
            WorkflowVersion.workflow_id == workflow_id,
        )
    )
    if version is None:
        raise LookupError(f"Unknown Workflow version: {version_id}")
    return version


async def _deactivate_versions(
    session: AsyncSession,
    workflow_id: UUID,
) -> None:
    await session.execute(
        update(WorkflowVersion)
        .where(WorkflowVersion.workflow_id == workflow_id)
        .values(active=False)
    )


async def _locked_existing(
    session: AsyncSession,
    *,
    workflow_id: UUID,
    actor_user_id: str,
    action: str,
    idempotency_key: str,
    request_hash: str,
) -> WorkflowLifecycleResult | None:
    await session.execute(
        select(func.pg_advisory_xact_lock(_idempotency_lock(idempotency_key)))
    )
    row = await session.scalar(
        select(WorkflowLifecycleAction)
        .where(WorkflowLifecycleAction.idempotency_key == idempotency_key)
        .with_for_update()
    )
    if row is None:
        return None
    return _existing_result(
        row,
        workflow_id=workflow_id,
        actor_user_id=actor_user_id,
        action=action,
        request_hash=request_hash,
    )


def _existing_result(
    row: WorkflowLifecycleAction,
    *,
    workflow_id: UUID,
    actor_user_id: str,
    action: str,
    request_hash: str,
) -> WorkflowLifecycleResult:
    if (
        row.workflow_id != workflow_id
        or row.actor_user_id != actor_user_id
        or row.action != action
        or row.request_hash != request_hash
    ):
        raise WorkflowLifecycleConflictError(
            "Idempotency key payload mismatch"
        )
    result = WorkflowLifecycleResult.model_validate(row.result_payload)
    return result.model_copy(update={"applied": False})


def _version_result(
    workflow: Workflow,
    version: WorkflowVersion,
    *,
    action: str,
) -> WorkflowLifecycleResult:
    return WorkflowLifecycleResult.model_validate(
        {
            "workflow_id": workflow.id,
            "workflow_version_id": version.id,
            "version": version.version,
            "public_payload_hash": version.public_payload_hash,
            "action": action,
            "workflow_status": workflow.status,
            "review_status": version.review_status,
            "version_active": version.active,
            "applied": True,
        }
    )


def _add_action(
    session: AsyncSession,
    *,
    result: WorkflowLifecycleResult,
    actor_user_id: str,
    idempotency_key: str,
    request_hash: str,
    reason: str | None,
    policy_version: str,
) -> None:
    session.add(
        WorkflowLifecycleAction(
            workflow_id=result.workflow_id,
            workflow_version_id=result.workflow_version_id,
            actor_user_id=actor_user_id,
            action=result.action,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            reason=reason,
            policy_version=policy_version,
            result_payload=result.model_dump(mode="json"),
        )
    )


def _only_value(values: set[UUID]) -> UUID | None:
    return next(iter(values)) if len(values) == 1 else None


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _idempotency_lock(idempotency_key: str) -> int:
    digest = hashlib.sha256(idempotency_key.encode()).digest()
    return int.from_bytes(digest[:8], byteorder="big", signed=True)


__all__ = [
    "WorkflowLifecycleConflictError",
    "WorkflowLifecycleRepository",
]
