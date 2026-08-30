from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import (
    CompiledStep,
    PersistedPlan,
    PlanDraft,
    WorkflowCandidate,
)
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    ExecutorBinding,
    Message,
    PlanStep,
    SessionLock,
    Task,
    TaskEvent,
    WorkflowCommand,
    WorkflowVersion,
)
from ex_agent.persistence.repositories.audit import AuditRepository
from ex_agent.persistence.repositories.delivery import DeliveryRepository
from ex_agent.persistence.repositories.executions import (
    ExecutionRepository,
)
from ex_agent.persistence.repositories.executions import (
    ExecutorEventSequenceGapError as ExecutorEventSequenceGapError,
)
from ex_agent.persistence.repositories.plans import PlanRepository
from ex_agent.persistence.repositories.tasks import (
    SessionLockedError as SessionLockedError,
)
from ex_agent.persistence.repositories.tasks import (
    TaskRepository,
    required_task,
)
from ex_agent.persistence.repositories.workflows import (
    WorkflowCatalogRepository,
)


class AgentRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions
        self.audit = AuditRepository(sessions)
        self.delivery = DeliveryRepository(sessions)
        self.executions = ExecutionRepository(sessions)
        self.plans = PlanRepository(sessions)
        self.tasks = TaskRepository(sessions)
        self.workflows = WorkflowCatalogRepository(sessions)

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
        command_id = uuid4()
        statement = (
            insert(WorkflowCommand)
            .values(
                id=command_id,
                task_id=task_id,
                command_type=command_type,
                idempotency_key=idempotency_key,
                payload=payload,
            )
            .on_conflict_do_nothing(constraint="uq_agent_command_idem")
            .returning(WorkflowCommand.id)
        )
        async with transaction(self._sessions) as session:
            resolved = await session.scalar(statement)
        return resolved or command_id

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
        async with transaction(self._sessions) as session:
            command = await session.get(
                WorkflowCommand,
                command_id,
                with_for_update=True,
            )
            if command is None:
                return
            command.state = state
            command.last_error = error
            command.publish_claimed_at = None
            if state == "PROCESSING":
                command.attempt_count += 1

    async def get_task(self, task_id: UUID) -> Task | None:
        return await self.tasks.get(task_id)

    async def get_command(self, command_id: UUID) -> WorkflowCommand | None:
        async with self._sessions() as session:
            return await session.get(WorkflowCommand, command_id)

    async def prepare_failure_compensation(
        self,
        command_id: UUID,
        task_id: UUID,
        failure_message: str,
    ) -> None:
        async with transaction(self._sessions) as session:
            command = await session.get(
                WorkflowCommand,
                command_id,
                with_for_update=True,
            )
            task = await required_task(session, task_id, for_update=True)
            if command is None:
                raise LookupError(f"Unknown command: {command_id}")
            command.command_type = "FAILURE_COMPENSATION"
            command.payload = {"failure_message": failure_message}
            command.state = "PENDING"
            command.last_error = failure_message
            command.publish_claimed_at = None
            if (
                task.execution_id is not None
                and not TaskStatus(task.status).is_terminal
            ):
                task.status = TaskStatus.CANCEL_REQUESTED.value
                task.version += 1
                session.add(
                    TaskEvent(
                        task_id=task_id,
                        event_type="task.status_changed",
                        payload={
                            "status": TaskStatus.CANCEL_REQUESTED.value,
                            "reason": "agent_failure_compensation",
                        },
                    )
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
        async with transaction(self._sessions) as session:
            command = await session.get(
                WorkflowCommand,
                command_id,
                with_for_update=True,
            )
            task = await required_task(session, task_id, for_update=True)
            if command is None:
                raise LookupError(f"Unknown command: {command_id}")
            if command.state == "FAILED":
                return
            command.state = "FAILED"
            command.last_error = failure_message
            command.publish_claimed_at = None
            if TaskStatus(task.status).is_terminal:
                return
            task.status = TaskStatus.FAILED.value
            task.terminal_message = content
            task.current_interrupt = None
            task.version += 1
            session.add(
                Message(
                    task_id=task_id,
                    role="assistant",
                    content=content,
                    metadata_json={
                        "failure_message": failure_message,
                        "executor_cleanup_status": executor_status,
                    },
                )
            )
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.completed",
                    payload={
                        "status": TaskStatus.FAILED.value,
                        "executor_cleanup_status": executor_status,
                    },
                )
            )
            await session.execute(
                delete(SessionLock).where(
                    SessionLock.active_task_id == task_id
                )
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
