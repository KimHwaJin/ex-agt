from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    Message,
    SessionLock,
    Task,
    TaskEvent,
    WorkflowCommand,
)


class SessionLockedError(RuntimeError):
    def __init__(self, active_task_id: UUID) -> None:
        super().__init__(f"Session is locked by task {active_task_id}")
        self.active_task_id = active_task_id


class TaskRepository:
    """Task lifecycle, conversation messages, events, and session locks."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def create(
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
                existing_task = await required_task(
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

    async def get(self, task_id: UUID) -> Task | None:
        async with self._sessions() as session:
            return await session.get(Task, task_id)

    async def update_status(
        self,
        task_id: UUID,
        status: TaskStatus,
        *,
        payload: dict[str, Any] | None = None,
    ) -> None:
        async with transaction(self._sessions) as session:
            task = await required_task(session, task_id, for_update=True)
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
            task = await required_task(session, task_id, for_update=True)
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
            task = await required_task(session, task_id, for_update=True)
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
            task = await required_task(session, task_id, for_update=True)
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
            task = await required_task(session, task_id, for_update=True)
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

    async def append_event(
        self,
        task_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        async with transaction(self._sessions) as session:
            await required_task(session, task_id)
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type=event_type,
                    payload=payload,
                )
            )


async def required_task(
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


def _interrupt_status(payload: dict[str, Any]) -> TaskStatus:
    if payload.get("kind") == "PLAN_REVIEW":
        return TaskStatus.WAITING_FOR_APPROVAL
    if payload.get("kind") == "EXECUTOR_EVENT":
        return TaskStatus.WAITING_FOR_EXECUTOR_EVENT
    return TaskStatus.PLANNING


__all__ = ["SessionLockedError", "TaskRepository", "required_task"]
