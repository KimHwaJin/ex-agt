from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import (
    PlanDraft,
    PlanStepDraft,
    WorkflowPromotionResult,
)
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    PlanRevision,
    SuccessfulExecutionStep,
    Task,
    TaskEvent,
    Workflow,
    WorkflowPromotion,
    WorkflowStep,
    WorkflowVersion,
)


class WorkflowPromotionConflictError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromotionSource:
    task: Task
    steps: list[SuccessfulExecutionStep]
    plans_by_revision: dict[UUID, PlanDraft]


class WorkflowPromotionRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def record_successful_steps(
        self,
        *,
        task_id: UUID,
        operation_id: UUID,
        plan_id: UUID,
        plan_revision_id: UUID,
        registry_snapshot_hash: str,
        start_sequence: int,
        steps: list[PlanStepDraft],
    ) -> None:
        async with transaction(self._sessions) as session:
            for offset, step in enumerate(steps):
                sequence = start_sequence + offset
                payload = step.model_dump(mode="json")
                payload_hash = _payload_hash(payload)
                existing = await session.scalar(
                    select(SuccessfulExecutionStep).where(
                        SuccessfulExecutionStep.task_id == task_id,
                        SuccessfulExecutionStep.execution_sequence == sequence,
                    )
                )
                if existing is not None:
                    if (
                        existing.step_payload_hash != payload_hash
                        or existing.operation_id != operation_id
                    ):
                        raise ValueError(
                            "Successful execution Step lineage changed"
                        )
                    continue
                session.add(
                    SuccessfulExecutionStep(
                        task_id=task_id,
                        execution_sequence=sequence,
                        operation_id=operation_id,
                        source_plan_id=plan_id,
                        source_plan_revision_id=plan_revision_id,
                        registry_snapshot_hash=registry_snapshot_hash,
                        step_payload=payload,
                        step_payload_hash=payload_hash,
                    )
                )

    async def source(self, task_id: UUID) -> PromotionSource:
        async with self._sessions() as session:
            task = await session.get(Task, task_id)
            if task is None:
                raise LookupError(f"Unknown task: {task_id}")
            rows = await session.scalars(
                select(SuccessfulExecutionStep)
                .where(SuccessfulExecutionStep.task_id == task_id)
                .order_by(SuccessfulExecutionStep.execution_sequence)
            )
            steps = list(rows.all())
            revision_ids = {row.source_plan_revision_id for row in steps}
            revisions = (
                await session.scalars(
                    select(PlanRevision).where(
                        PlanRevision.id.in_(revision_ids)
                    )
                )
            ).all()
            return PromotionSource(
                task=task,
                steps=steps,
                plans_by_revision={
                    revision.id: PlanDraft.model_validate(
                        revision.public_payload
                    )
                    for revision in revisions
                },
            )

    async def existing(
        self,
        *,
        task_id: UUID,
        actor_user_id: str,
        idempotency_key: str,
        request_hash: str,
    ) -> WorkflowPromotionResult | None:
        async with self._sessions() as session:
            existing = await session.scalar(
                select(WorkflowPromotion).where(
                    WorkflowPromotion.idempotency_key == idempotency_key
                )
            )
            if existing is None:
                return None
            if (
                existing.task_id != task_id
                or existing.actor_user_id != actor_user_id
                or existing.request_hash != request_hash
            ):
                raise WorkflowPromotionConflictError(
                    "Idempotency key payload mismatch"
                )
            version = await session.get(
                WorkflowVersion,
                existing.workflow_version_id,
            )
            if version is None:
                raise RuntimeError("Promoted Workflow version is missing")
            return WorkflowPromotionResult(
                workflow_id=existing.workflow_id,
                workflow_version_id=existing.workflow_version_id,
                version=version.version,
                created=False,
                public_payload_hash=version.public_payload_hash,
            )

    async def create(
        self,
        *,
        source: PromotionSource,
        actor_user_id: str,
        idempotency_key: str,
        request_hash: str,
        name: str,
        description: str,
        request_examples: list[str],
        tags: list[str],
        plan: PlanDraft,
        input_contract: dict[str, dict[str, Any]],
        embedding: list[float],
        embedding_model: str,
        registry_snapshot_hash: str,
        policy_version: str,
        searchable_text: str,
    ) -> WorkflowPromotionResult:
        async with transaction(self._sessions) as session:
            existing = await session.scalar(
                select(WorkflowPromotion).where(
                    WorkflowPromotion.idempotency_key == idempotency_key
                )
            )
            if existing is not None:
                return await _existing_result(
                    session,
                    existing,
                    task_id=source.task.id,
                    actor_user_id=actor_user_id,
                    request_hash=request_hash,
                )
            if await session.scalar(
                select(Workflow.id).where(Workflow.name == name)
            ):
                raise WorkflowPromotionConflictError(
                    "Workflow name already exists"
                )
            workflow = Workflow(
                name=name,
                description=description,
                owner_user_id=source.task.user_id,
                owner_project_id=source.task.project_id,
                visibility="SERVICE",
                status="ACTIVE",
                latest_version=1,
                access_policy={"version": "service-v1"},
                created_by=actor_user_id,
                updated_by=actor_user_id,
            )
            session.add(workflow)
            try:
                await session.flush()
            except IntegrityError as error:
                raise WorkflowPromotionConflictError(
                    "Workflow name already exists"
                ) from error
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
                version=1,
                source_task_id=source.task.id,
                source_plan_id=(
                    next(iter(source_plan_ids))
                    if len(source_plan_ids) == 1
                    else None
                ),
                source_plan_revision_id=(
                    next(iter(source_revision_ids))
                    if len(source_revision_ids) == 1
                    else None
                ),
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
                active=True,
                review_status="APPROVED",
                reviewed_by=actor_user_id,
                reviewed_at=datetime.now(UTC),
                created_by=actor_user_id,
                updated_by=actor_user_id,
            )
            session.add(version)
            await session.flush()
            add_workflow_steps(session, version.id, plan, source)
            session.add(
                WorkflowPromotion(
                    task_id=source.task.id,
                    actor_user_id=actor_user_id,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    workflow_id=workflow.id,
                    workflow_version_id=version.id,
                    policy_version=policy_version,
                )
            )
            session.add(
                TaskEvent(
                    task_id=source.task.id,
                    event_type="workflow.promoted",
                    payload={
                        "workflow_id": str(workflow.id),
                        "workflow_version_id": str(version.id),
                        "version": version.version,
                    },
                )
            )
            try:
                await session.flush()
            except IntegrityError as error:
                raise WorkflowPromotionConflictError(
                    "Workflow promotion conflicts with existing data"
                ) from error
        return WorkflowPromotionResult(
            workflow_id=workflow.id,
            workflow_version_id=version.id,
            version=version.version,
            created=True,
            public_payload_hash=public_payload_hash,
        )


async def _existing_result(
    session: AsyncSession,
    existing: WorkflowPromotion,
    *,
    task_id: UUID,
    actor_user_id: str,
    request_hash: str,
) -> WorkflowPromotionResult:
    if (
        existing.task_id != task_id
        or existing.actor_user_id != actor_user_id
        or existing.request_hash != request_hash
    ):
        raise WorkflowPromotionConflictError(
            "Idempotency key payload mismatch"
        )
    version = await session.get(
        WorkflowVersion,
        existing.workflow_version_id,
    )
    if version is None:
        raise RuntimeError("Promoted Workflow version is missing")
    return WorkflowPromotionResult(
        workflow_id=existing.workflow_id,
        workflow_version_id=existing.workflow_version_id,
        version=version.version,
        created=False,
        public_payload_hash=version.public_payload_hash,
    )


def add_workflow_steps(
    session: AsyncSession,
    version_id: UUID,
    plan: PlanDraft,
    source: PromotionSource,
) -> None:
    for sequence, (step, source_row) in enumerate(
        zip(plan.steps, source.steps, strict=True)
    ):
        if step.skill is None or step.tool is None:
            raise ValueError(
                "Promoted Workflow Step requires Skill/Tool lineage"
            )
        session.add(
            WorkflowStep(
                workflow_version_id=version_id,
                source_plan_revision_id=source_row.source_plan_revision_id,
                sequence=sequence,
                skill_ref=step.skill.model_dump(mode="json"),
                tool_ref=step.tool.model_dump(mode="json"),
                purpose=step.purpose,
                selection_rationale=step.selection_rationale,
                parameter_template=step.parameters,
                expected_outputs=step.expected_outputs,
                validation_criteria=step.validation_criteria,
                timeout_seconds=step.timeout_seconds,
            )
        )


def _payload_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "PromotionSource",
    "WorkflowPromotionConflictError",
    "WorkflowPromotionRepository",
    "add_workflow_steps",
]
