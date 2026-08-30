from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import (
    CompiledStep,
    PersistedPlan,
    PlanDraft,
    WorkflowCandidate,
    WorkflowLifecycleResult,
    WorkflowPromotionResult,
)
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.models import (
    ExecutorBinding,
    PlanStep,
    Task,
    TaskEvent,
    Workflow,
    WorkflowCommand,
    WorkflowVersion,
)
from ex_agent.persistence.repositories.audit import AuditRepository
from ex_agent.persistence.repositories.commands import CommandRepository
from ex_agent.persistence.repositories.delivery import DeliveryRepository
from ex_agent.persistence.repositories.executions import (
    ExecutionRepository,
)
from ex_agent.persistence.repositories.executions import (
    ExecutorEventSequenceGapError as ExecutorEventSequenceGapError,
)
from ex_agent.persistence.repositories.plans import PlanRepository
from ex_agent.persistence.repositories.promotions import (
    PromotionSource,
    WorkflowPromotionRepository,
)
from ex_agent.persistence.repositories.tasks import (
    SessionLockedError as SessionLockedError,
)
from ex_agent.persistence.repositories.tasks import TaskRepository
from ex_agent.persistence.repositories.workflow_lifecycle import (
    WorkflowLifecycleRepository,
)
from ex_agent.persistence.repositories.workflows import (
    WorkflowCatalogRepository,
)


class AgentRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self.audit = AuditRepository(sessions)
        self.commands = CommandRepository(sessions)
        self.delivery = DeliveryRepository(sessions)
        self.executions = ExecutionRepository(sessions)
        self.plans = PlanRepository(sessions)
        self.promotions = WorkflowPromotionRepository(sessions)
        self.tasks = TaskRepository(sessions)
        self.workflows = WorkflowCatalogRepository(sessions)
        self.workflow_lifecycle = WorkflowLifecycleRepository(sessions)

    async def create_task(
        self,
        *,
        task_id: UUID,
        input_message_id: UUID,
        user_id: str,
        project_id: str,
        session_id: str,
        content: str,
        idempotency_key: str,
    ) -> Task:
        return await self.tasks.create(
            task_id=task_id,
            input_message_id=input_message_id,
            user_id=user_id,
            project_id=project_id,
            session_id=session_id,
            content=content,
            idempotency_key=idempotency_key,
        )

    async def create_resume_command(
        self,
        *,
        task_id: UUID,
        idempotency_key: str,
        payload: dict[str, Any],
        lock_session: bool = False,
    ) -> UUID:
        return await self.tasks.create_resume_command(
            task_id=task_id,
            idempotency_key=idempotency_key,
            payload=payload,
            lock_session=lock_session,
        )

    async def create_system_command(
        self,
        *,
        task_id: UUID,
        command_type: str,
        idempotency_key: str,
        payload: dict[str, Any],
    ) -> UUID:
        return await self.commands.create_system_command(
            task_id=task_id,
            command_type=command_type,
            idempotency_key=idempotency_key,
            payload=payload,
        )

    async def claim_pending_commands(
        self,
        *,
        limit: int,
        claim_timeout_seconds: int,
    ) -> Sequence[WorkflowCommand]:
        return await self.delivery.claim_pending_commands(
            limit=limit,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    async def finish_command_publications(
        self,
        command_ids: Sequence[UUID],
        *,
        claimed_at: datetime,
        published: bool,
        error: str | None = None,
    ) -> None:
        await self.delivery.finish_command_publications(
            command_ids,
            claimed_at=claimed_at,
            published=published,
            error=error,
        )

    async def claim_pending_task_events(
        self,
        *,
        limit: int,
        claim_timeout_seconds: int,
    ) -> Sequence[TaskEvent]:
        return await self.delivery.claim_pending_task_events(
            limit=limit,
            claim_timeout_seconds=claim_timeout_seconds,
        )

    async def finish_task_event_publications(
        self,
        event_ids: Sequence[int],
        *,
        claimed_at: datetime,
        published: bool,
        error: str | None = None,
    ) -> None:
        await self.delivery.finish_task_event_publications(
            event_ids,
            claimed_at=claimed_at,
            published=published,
            error=error,
        )

    async def delivery_backlog_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        return await self.delivery.backlog_counts()

    async def set_command_state(
        self,
        command_id: UUID,
        state: str,
        error: str | None = None,
    ) -> None:
        await self.commands.set_state(command_id, state, error)

    async def get_task(self, task_id: UUID) -> Task | None:
        return await self.tasks.get(task_id)

    async def get_command(self, command_id: UUID) -> WorkflowCommand | None:
        return await self.commands.get(command_id)

    async def prepare_failure_compensation(
        self,
        command_id: UUID,
        task_id: UUID,
        failure_message: str,
    ) -> None:
        await self.commands.prepare_failure_compensation(
            command_id,
            task_id,
            failure_message,
        )

    async def complete_failure_compensation(
        self,
        command_id: UUID,
        task_id: UUID,
        content: str,
        *,
        failure_message: str,
        executor_status: str,
    ) -> None:
        await self.commands.complete_failure_compensation(
            command_id,
            task_id,
            content,
            failure_message=failure_message,
            executor_status=executor_status,
        )

    async def update_status(
        self,
        task_id: UUID,
        status: TaskStatus,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        await self.tasks.update_status(task_id, status, payload=payload)

    async def record_interrupt(
        self,
        task_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        await self.tasks.record_interrupt(task_id, payload)

    async def clear_interrupt(self, task_id: UUID) -> None:
        await self.tasks.clear_interrupt(task_id)

    async def commit_message(
        self,
        task_id: UUID,
        content: str,
        *,
        status: TaskStatus,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.tasks.commit_message(
            task_id,
            content,
            status=status,
            metadata=metadata,
        )

    async def lock_session(self, task_id: UUID) -> None:
        await self.tasks.lock_session(task_id)

    async def persist_plan(
        self,
        task_id: UUID,
        plan: PlanDraft,
        compiled: list[tuple[CompiledStep, str]],
        registry_snapshot_hash: str,
        feedback: str | None,
    ) -> PersistedPlan:
        return await self.plans.persist(
            task_id,
            plan,
            compiled,
            registry_snapshot_hash,
            feedback,
        )

    async def approved_steps(
        self,
        revision_id: UUID,
    ) -> Sequence[PlanStep]:
        return await self.plans.approved_steps(revision_id)

    async def bind_execution(
        self,
        *,
        task_id: UUID,
        execution_id: UUID,
        operation_id: UUID,
        execution_version: int,
        next_step_sequence: int,
    ) -> None:
        await self.executions.bind(
            task_id=task_id,
            execution_id=execution_id,
            operation_id=operation_id,
            execution_version=execution_version,
            next_step_sequence=next_step_sequence,
        )

    async def binding_for_task(self, task_id: UUID) -> ExecutorBinding:
        return await self.executions.for_task(task_id)

    async def binding_for_execution(
        self,
        execution_id: UUID,
    ) -> ExecutorBinding | None:
        return await self.executions.for_execution(execution_id)

    async def update_binding(
        self,
        task_id: UUID,
        *,
        operation_id: UUID | None = None,
        execution_version: int | None = None,
        next_step_sequence: int | None = None,
        last_event_sequence: int | None = None,
    ) -> None:
        await self.executions.update(
            task_id,
            operation_id=operation_id,
            execution_version=execution_version,
            next_step_sequence=next_step_sequence,
            last_event_sequence=last_event_sequence,
        )

    async def record_inbox(
        self,
        stream_name: str,
        message_id: str,
    ) -> bool:
        return await self.executions.record_inbox(stream_name, message_id)

    async def ingest_executor_signal(
        self,
        *,
        stream_name: str,
        message_id: str,
        task_id: UUID,
        idempotency_key: str,
        event_sequence: int,
        payload: dict[str, Any],
    ) -> bool:
        return await self.executions.ingest_signal(
            stream_name=stream_name,
            message_id=message_id,
            task_id=task_id,
            idempotency_key=idempotency_key,
            event_sequence=event_sequence,
            payload=payload,
        )

    async def record_executor_progress(
        self,
        *,
        stream_name: str,
        message_id: str,
        task_id: UUID,
        event_type: str,
        event_sequence: int,
        payload: dict[str, Any],
    ) -> bool:
        return await self.executions.record_progress(
            stream_name=stream_name,
            message_id=message_id,
            task_id=task_id,
            event_type=event_type,
            event_sequence=event_sequence,
            payload=payload,
        )

    async def events_after(
        self,
        task_id: UUID,
        after_id: int,
        limit: int = 100,
    ) -> Sequence[TaskEvent]:
        return await self.tasks.events_after(task_id, after_id, limit)

    async def append_task_event(
        self,
        task_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        await self.tasks.append_event(task_id, event_type, payload)

    async def record_model_call(
        self,
        *,
        task_id: str,
        component: str,
        duration_ms: int,
        succeeded: bool,
        metadata: dict[str, Any],
    ) -> None:
        await self.audit.record_model_call(
            task_id=task_id,
            component=component,
            duration_ms=duration_ms,
            succeeded=succeeded,
            metadata=metadata,
        )

    async def workflow_candidates(
        self,
        embedding: list[float],
        limit: int = 3,
    ) -> list[WorkflowCandidate]:
        return await self.workflows.candidates(embedding, limit)

    async def workflow_version(self, version_id: UUID) -> WorkflowVersion:
        return await self.workflows.version(version_id)

    async def record_successful_execution_steps(
        self,
        *,
        task_id: UUID,
        operation_id: UUID,
        plan_id: UUID,
        plan_revision_id: UUID,
        registry_snapshot_hash: str,
        start_sequence: int,
        steps: list[Any],
    ) -> None:
        await self.promotions.record_successful_steps(
            task_id=task_id,
            operation_id=operation_id,
            plan_id=plan_id,
            plan_revision_id=plan_revision_id,
            registry_snapshot_hash=registry_snapshot_hash,
            start_sequence=start_sequence,
            steps=steps,
        )

    async def workflow_promotion_source(
        self,
        task_id: UUID,
    ) -> PromotionSource:
        return await self.promotions.source(task_id)

    async def existing_workflow_promotion(
        self,
        **values: Any,
    ) -> WorkflowPromotionResult | None:
        return await self.promotions.existing(**values)

    async def promote_workflow(
        self,
        **values: Any,
    ) -> WorkflowPromotionResult:
        return await self.promotions.create(**values)

    async def lifecycle_workflow(self, workflow_id: UUID) -> Workflow:
        return await self.workflow_lifecycle.workflow(workflow_id)

    async def existing_workflow_lifecycle_action(
        self,
        **values: Any,
    ) -> WorkflowLifecycleResult | None:
        return await self.workflow_lifecycle.existing(**values)

    async def create_workflow_version(
        self,
        **values: Any,
    ) -> WorkflowLifecycleResult:
        return await self.workflow_lifecycle.create_version(**values)

    async def review_workflow_version(
        self,
        **values: Any,
    ) -> WorkflowLifecycleResult:
        return await self.workflow_lifecycle.review_version(**values)

    async def activate_workflow_version(
        self,
        **values: Any,
    ) -> WorkflowLifecycleResult:
        return await self.workflow_lifecycle.activate_version(**values)

    async def update_workflow_status(
        self,
        **values: Any,
    ) -> WorkflowLifecycleResult:
        return await self.workflow_lifecycle.update_status(**values)
