from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, func, or_, select, update
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
    ModelCallAudit,
    Plan,
    PlanRevision,
    PlanStep,
    SessionLock,
    StreamInbox,
    Task,
    TaskEvent,
    Workflow,
    WorkflowCommand,
    WorkflowVersion,
)


class AgentRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

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
        async with transaction(self._sessions) as session:
            existing_command = await session.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.idempotency_key == idempotency_key
                )
            )
            if existing_command is not None:
                existing_task = await _required_task(
                    session,
                    existing_command.task_id,
                )
                if (
                    existing_task.id != task_id
                    or existing_task.user_id != user_id
                    or existing_task.project_id != project_id
                    or existing_task.session_id != session_id
                    or existing_task.user_message != content
                ):
                    raise ValueError("Idempotency key payload mismatch")
                return existing_task
            locked = await session.scalar(
                select(SessionLock).where(
                    SessionLock.session_id == session_id,
                    SessionLock.locked.is_(True),
                )
            )
            if locked is not None:
                raise SessionLockedError(locked.active_task_id)
            task = Task(
                id=task_id,
                input_message_id=input_message_id,
                user_id=user_id,
                project_id=project_id,
                session_id=session_id,
                user_message=content,
                status=TaskStatus.ACCEPTED.value,
            )
            session.add(task)
            await session.flush()
            session.add(
                Message(
                    id=input_message_id,
                    task_id=task_id,
                    role="user",
                    content=content,
                )
            )
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.accepted",
                    payload={"status": TaskStatus.ACCEPTED.value},
                )
            )
            session.add(
                WorkflowCommand(
                    task_id=task_id,
                    command_type="START",
                    idempotency_key=idempotency_key,
                    payload={},
                )
            )
        return task

    async def create_resume_command(
        self,
        *,
        task_id: UUID,
        idempotency_key: str,
        payload: dict[str, Any],
        lock_session: bool = False,
    ) -> UUID:
        command_id = uuid4()
        async with transaction(self._sessions) as session:
            existing_command = await session.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.idempotency_key == idempotency_key
                )
            )
            if existing_command is not None:
                if (
                    existing_command.task_id != task_id
                    or existing_command.payload != payload
                ):
                    raise ValueError("Idempotency key payload mismatch")
                return existing_command.id
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise LookupError(f"Unknown task: {task_id}")
            if TaskStatus(task.status).is_terminal:
                raise ValueError("Terminal task cannot be resumed")
            if lock_session:
                existing = await session.get(
                    SessionLock,
                    task.session_id,
                    with_for_update=True,
                )
                if existing and existing.active_task_id != task_id:
                    raise SessionLockedError(existing.active_task_id)
                if existing is None:
                    session.add(
                        SessionLock(
                            session_id=task.session_id,
                            active_task_id=task_id,
                        )
                    )
            session.add(
                WorkflowCommand(
                    id=command_id,
                    task_id=task_id,
                    command_type="RESUME",
                    idempotency_key=idempotency_key,
                    payload=payload,
                )
            )
        return command_id

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
        stale_before = datetime.now(UTC) - timedelta(
            seconds=claim_timeout_seconds
        )
        async with transaction(self._sessions) as session:
            result = await session.scalars(
                select(WorkflowCommand)
                .where(
                    or_(
                        WorkflowCommand.state == "PENDING",
                        (
                            (WorkflowCommand.state == "PUBLISHING")
                            & (
                                WorkflowCommand.publish_claimed_at
                                < stale_before
                            )
                        ),
                    )
                )
                .order_by(WorkflowCommand.created_at)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            commands = result.all()
            claimed_at = datetime.now(UTC)
            for command in commands:
                command.state = "PUBLISHING"
                command.publish_claimed_at = claimed_at
            return commands

    async def finish_command_publications(
        self,
        command_ids: Sequence[UUID],
        *,
        claimed_at: datetime,
        published: bool,
        error: str | None = None,
    ) -> None:
        if not command_ids:
            return
        values: dict[str, Any] = {
            "state": "PUBLISHED" if published else "PENDING",
            "publish_claimed_at": None,
            "last_error": None if published else error,
        }
        async with transaction(self._sessions) as session:
            await session.execute(
                update(WorkflowCommand)
                .where(
                    WorkflowCommand.id.in_(command_ids),
                    WorkflowCommand.state == "PUBLISHING",
                    WorkflowCommand.publish_claimed_at == claimed_at,
                )
                .values(**values)
            )

    async def claim_pending_task_events(
        self,
        *,
        limit: int,
        claim_timeout_seconds: int,
    ) -> Sequence[TaskEvent]:
        stale_before = datetime.now(UTC) - timedelta(
            seconds=claim_timeout_seconds
        )
        async with transaction(self._sessions) as session:
            result = await session.scalars(
                select(TaskEvent)
                .where(
                    or_(
                        TaskEvent.delivery_state == "PENDING",
                        (
                            (TaskEvent.delivery_state == "PUBLISHING")
                            & (TaskEvent.delivery_claimed_at < stale_before)
                        ),
                    )
                )
                .order_by(TaskEvent.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            events = result.all()
            claimed_at = datetime.now(UTC)
            for event in events:
                event.delivery_state = "PUBLISHING"
                event.delivery_claimed_at = claimed_at
                event.delivery_attempt_count += 1
            return events

    async def finish_task_event_publications(
        self,
        event_ids: Sequence[int],
        *,
        claimed_at: datetime,
        published: bool,
        error: str | None = None,
    ) -> None:
        if not event_ids:
            return
        values: dict[str, Any] = {
            "delivery_state": "PUBLISHED" if published else "PENDING",
            "delivery_claimed_at": None,
            "delivery_last_error": None if published else error,
        }
        async with transaction(self._sessions) as session:
            await session.execute(
                update(TaskEvent)
                .where(
                    TaskEvent.id.in_(event_ids),
                    TaskEvent.delivery_state == "PUBLISHING",
                    TaskEvent.delivery_claimed_at == claimed_at,
                )
                .values(**values)
            )

    async def delivery_backlog_counts(
        self,
    ) -> dict[tuple[str, str], int]:
        async with self._sessions() as session:
            command_rows = (
                await session.execute(
                    select(
                        WorkflowCommand.state,
                        func.count(WorkflowCommand.id),
                    )
                    .where(
                        WorkflowCommand.state.in_(("PENDING", "PUBLISHING"))
                    )
                    .group_by(WorkflowCommand.state)
                )
            ).all()
            event_rows = (
                await session.execute(
                    select(
                        TaskEvent.delivery_state,
                        func.count(TaskEvent.id),
                    )
                    .where(
                        TaskEvent.delivery_state.in_(("PENDING", "PUBLISHING"))
                    )
                    .group_by(TaskEvent.delivery_state)
                )
            ).all()
        return {
            **{
                ("command", str(state)): int(count)
                for state, count in command_rows
            },
            **{
                ("task_event", str(state)): int(count)
                for state, count in event_rows
            },
        }

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
        async with self._sessions() as session:
            return await session.get(Task, task_id)

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
            task = await _required_task(session, task_id, for_update=True)
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
            task = await _required_task(session, task_id, for_update=True)
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
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            task.status = status.value
            task.version += 1
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.status_changed",
                    payload={
                        "status": status.value,
                        **(payload or {}),
                    },
                )
            )

    async def record_interrupt(
        self,
        task_id: UUID,
        payload: dict[str, Any],
    ) -> None:
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            task.current_interrupt = payload
            task.status = _interrupt_status(payload).value
            task.version += 1
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.interrupted",
                    payload=payload,
                )
            )

    async def clear_interrupt(self, task_id: UUID) -> None:
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            task.current_interrupt = None

    async def commit_message(
        self,
        task_id: UUID,
        content: str,
        *,
        status: TaskStatus,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            task.status = status.value
            task.terminal_message = content
            task.current_interrupt = None
            task.version += 1
            session.add(
                Message(
                    task_id=task_id,
                    role="assistant",
                    content=content,
                    metadata_json=metadata or {},
                )
            )
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.completed",
                    payload={"status": status.value},
                )
            )
            if status.is_terminal:
                await session.execute(
                    delete(SessionLock).where(
                        SessionLock.active_task_id == task_id
                    )
                )

    async def lock_session(self, task_id: UUID) -> None:
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            existing = await session.get(
                SessionLock,
                task.session_id,
                with_for_update=True,
            )
            if existing and existing.active_task_id != task_id:
                raise SessionLockedError(existing.active_task_id)
            if existing is None:
                session.add(
                    SessionLock(
                        session_id=task.session_id,
                        active_task_id=task_id,
                    )
                )

    async def persist_plan(
        self,
        task_id: UUID,
        plan: PlanDraft,
        compiled: list[tuple[CompiledStep, str]],
        registry_snapshot_hash: str,
        feedback: str | None,
    ) -> PersistedPlan:
        payload = plan.model_dump(mode="json")
        payload_hash = _payload_hash(payload)
        compiled_bundle_id = uuid4()
        async with transaction(self._sessions) as session:
            plan_row = await session.scalar(
                select(Plan).where(Plan.task_id == task_id).with_for_update()
            )
            if plan_row is None:
                plan_row = Plan(task_id=task_id, current_revision=1)
                session.add(plan_row)
                await session.flush()
                revision_number = 1
            else:
                revision_number = plan_row.current_revision + 1
                plan_row.current_revision = revision_number
            revision = PlanRevision(
                plan_id=plan_row.id,
                revision_number=revision_number,
                public_payload=payload,
                public_payload_hash=payload_hash,
                compiled_bundle_id=compiled_bundle_id,
                registry_snapshot_hash=registry_snapshot_hash,
                feedback=feedback,
            )
            session.add(revision)
            await session.flush()
            for step, path in compiled:
                draft = plan.steps[step.sequence]
                session.add(
                    PlanStep(
                        plan_revision_id=revision.id,
                        sequence=step.sequence,
                        title=draft.title,
                        purpose=draft.purpose,
                        selection_rationale=draft.selection_rationale,
                        skill_ref=(
                            draft.skill.model_dump(mode="json")
                            if draft.skill
                            else None
                        ),
                        tool_ref=(
                            draft.tool.model_dump(mode="json")
                            if draft.tool
                            else None
                        ),
                        parameters=draft.parameters,
                        compiled_source_sha256=step.source_sha256,
                        compiled_source_path=path,
                        timeout_seconds=draft.timeout_seconds,
                    )
                )
        return PersistedPlan(
            plan_id=plan_row.id,
            plan_revision_id=revision.id,
            plan_revision_number=revision_number,
            public_payload_hash=payload_hash,
            compiled_bundle_id=compiled_bundle_id,
        )

    async def approved_steps(
        self,
        revision_id: UUID,
    ) -> Sequence[PlanStep]:
        async with self._sessions() as session:
            result = await session.scalars(
                select(PlanStep)
                .where(PlanStep.plan_revision_id == revision_id)
                .order_by(PlanStep.sequence)
            )
            return result.all()

    async def bind_execution(
        self,
        *,
        task_id: UUID,
        execution_id: UUID,
        operation_id: UUID,
        execution_version: int,
        next_step_sequence: int,
    ) -> None:
        async with transaction(self._sessions) as session:
            task = await _required_task(session, task_id, for_update=True)
            task.execution_id = execution_id
            lock = await session.get(SessionLock, task.session_id)
            if lock:
                lock.execution_id = execution_id
            session.add(
                ExecutorBinding(
                    task_id=task_id,
                    execution_id=execution_id,
                    operation_id=operation_id,
                    execution_version=execution_version,
                    next_step_sequence=next_step_sequence,
                )
            )

    async def binding_for_task(self, task_id: UUID) -> ExecutorBinding:
        async with self._sessions() as session:
            binding = await session.get(ExecutorBinding, task_id)
            if binding is None:
                raise LookupError(f"Task has no Executor binding: {task_id}")
            return binding

    async def binding_for_execution(
        self,
        execution_id: UUID,
    ) -> ExecutorBinding | None:
        async with self._sessions() as session:
            return await session.scalar(
                select(ExecutorBinding).where(
                    ExecutorBinding.execution_id == execution_id
                )
            )

    async def update_binding(
        self,
        task_id: UUID,
        *,
        operation_id: UUID | None = None,
        execution_version: int | None = None,
        next_step_sequence: int | None = None,
        last_event_sequence: int | None = None,
    ) -> None:
        async with transaction(self._sessions) as session:
            binding = await session.get(
                ExecutorBinding,
                task_id,
                with_for_update=True,
            )
            if binding is None:
                raise LookupError("Executor binding does not exist")
            if operation_id is not None:
                binding.operation_id = operation_id
            if execution_version is not None:
                binding.execution_version = execution_version
            if next_step_sequence is not None:
                binding.next_step_sequence = next_step_sequence
            if last_event_sequence is not None:
                binding.last_event_sequence = max(
                    binding.last_event_sequence,
                    last_event_sequence,
                )

    async def record_inbox(
        self,
        stream_name: str,
        message_id: str,
    ) -> bool:
        statement = (
            insert(StreamInbox)
            .values(stream_name=stream_name, message_id=message_id)
            .on_conflict_do_nothing(constraint="uq_agent_stream_message")
            .returning(StreamInbox.id)
        )
        async with transaction(self._sessions) as session:
            return (await session.scalar(statement)) is not None

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
        inbox_statement = (
            insert(StreamInbox)
            .values(stream_name=stream_name, message_id=message_id)
            .on_conflict_do_nothing(constraint="uq_agent_stream_message")
            .returning(StreamInbox.id)
        )
        async with transaction(self._sessions) as session:
            inserted = await session.scalar(inbox_statement)
            if inserted is None:
                return False
            if not await _advance_executor_sequence(
                session,
                task_id,
                event_sequence,
            ):
                return False
            session.add(
                WorkflowCommand(
                    task_id=task_id,
                    command_type="EXECUTOR_SIGNAL",
                    idempotency_key=idempotency_key,
                    payload=payload,
                )
            )
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="executor.boundary_received",
                    payload=payload,
                )
            )
        return True

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
        inbox_statement = (
            insert(StreamInbox)
            .values(stream_name=stream_name, message_id=message_id)
            .on_conflict_do_nothing(constraint="uq_agent_stream_message")
            .returning(StreamInbox.id)
        )
        async with transaction(self._sessions) as session:
            inserted = await session.scalar(inbox_statement)
            if inserted is None:
                return False
            if not await _advance_executor_sequence(
                session,
                task_id,
                event_sequence,
            ):
                return False
            task = await _required_task(session, task_id, for_update=True)
            task.status = TaskStatus.EXECUTING.value
            task.version += 1
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type=event_type,
                    payload=payload,
                )
            )
        return True

    async def events_after(
        self,
        task_id: UUID,
        after_id: int,
        limit: int = 100,
    ) -> Sequence[TaskEvent]:
        async with self._sessions() as session:
            result = await session.scalars(
                select(TaskEvent)
                .where(
                    TaskEvent.task_id == task_id,
                    TaskEvent.id > after_id,
                )
                .order_by(TaskEvent.id)
                .limit(limit)
            )
            return result.all()

    async def append_task_event(
        self,
        task_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        async with transaction(self._sessions) as session:
            await _required_task(session, task_id)
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type=event_type,
                    payload=payload,
                )
            )

    async def record_model_call(
        self,
        *,
        task_id: str,
        component: str,
        duration_ms: int,
        succeeded: bool,
        metadata: dict[str, Any],
    ) -> None:
        async with transaction(self._sessions) as session:
            session.add(
                ModelCallAudit(
                    task_id=UUID(task_id),
                    component=component,
                    duration_ms=duration_ms,
                    succeeded=succeeded,
                    metadata_json=metadata,
                )
            )

    async def workflow_candidates(
        self,
        embedding: list[float],
        limit: int = 3,
    ) -> list[WorkflowCandidate]:
        distance = WorkflowVersion.embedding.cosine_distance(embedding)
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(
                        WorkflowVersion, Workflow, distance.label("distance")
                    )
                    .join(Workflow, Workflow.id == WorkflowVersion.workflow_id)
                    .where(
                        WorkflowVersion.active.is_(True),
                        WorkflowVersion.embedding.is_not(None),
                    )
                    .order_by(distance)
                    .limit(limit)
                )
            ).all()
        return [
            WorkflowCandidate(
                workflow_version_id=version.id,
                name=workflow.name,
                description=workflow.description,
                score=max(0.0, 1.0 - float(distance_value)),
                plan=PlanDraft.model_validate(version.plan_payload),
                public_payload_hash=version.public_payload_hash,
            )
            for version, workflow, distance_value in rows
        ]

    async def workflow_version(self, version_id: UUID) -> WorkflowVersion:
        async with self._sessions() as session:
            version = await session.get(WorkflowVersion, version_id)
            if version is None or not version.active:
                raise LookupError(f"Unknown Workflow version: {version_id}")
            return version


class SessionLockedError(RuntimeError):
    def __init__(self, active_task_id: UUID) -> None:
        super().__init__(f"Session is locked by task {active_task_id}")
        self.active_task_id = active_task_id


class ExecutorEventSequenceGapError(RuntimeError):
    pass


async def _advance_executor_sequence(
    session: AsyncSession,
    task_id: UUID,
    received: int,
) -> bool:
    binding = await session.get(
        ExecutorBinding,
        task_id,
        with_for_update=True,
    )
    if binding is None:
        raise LookupError("Executor binding does not exist")
    if received <= binding.last_event_sequence:
        return False
    expected = binding.last_event_sequence + 1
    if received != expected:
        raise ExecutorEventSequenceGapError(
            f"Expected Executor event {expected}, received {received}"
        )
    binding.last_event_sequence = received
    return True


async def _required_task(
    session: AsyncSession,
    task_id: UUID,
    *,
    for_update: bool = False,
) -> Task:
    statement = select(Task).where(Task.id == task_id)
    if for_update:
        statement = statement.with_for_update()
    task = await session.scalar(statement)
    if task is None:
        raise LookupError(f"Unknown task: {task_id}")
    return task


def _payload_hash(payload: dict[str, Any]) -> str:
    value = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode()).hexdigest()


def _interrupt_status(payload: dict[str, Any]) -> TaskStatus:
    if payload.get("kind") == "PLAN_REVIEW":
        return TaskStatus.WAITING_FOR_APPROVAL
    if payload.get("kind") == "EXECUTOR_EVENT":
        return TaskStatus.WAITING_FOR_EXECUTOR_EVENT
    return TaskStatus.PLANNING
