from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.audit import AGENT_ACTOR, EXECUTOR_ACTOR
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    ExecutorBinding,
    SessionLock,
    StreamInbox,
    Task,
    TaskEvent,
    WorkflowCommand,
)


class ExecutorEventSequenceGapError(RuntimeError):
    pass


class ExecutionRepository:
    """Executor bindings and idempotent event ingestion transactions."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def bind(
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
            task.updated_by = AGENT_ACTOR
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

    async def for_task(self, task_id: UUID) -> ExecutorBinding:
        async with self._sessions() as session:
            binding = await session.get(ExecutorBinding, task_id)
            if binding is None:
                raise LookupError(f"Task has no Executor binding: {task_id}")
            return binding

    async def for_execution(
        self,
        execution_id: UUID,
    ) -> ExecutorBinding | None:
        async with self._sessions() as session:
            return await session.scalar(
                select(ExecutorBinding).where(
                    ExecutorBinding.execution_id == execution_id
                )
            )

    async def update(
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
        statement = _inbox_statement(stream_name, message_id)
        async with transaction(self._sessions) as session:
            return (await session.scalar(statement)) is not None

    async def ingest_signal(
        self,
        *,
        stream_name: str,
        message_id: str,
        task_id: UUID,
        idempotency_key: str,
        event_sequence: int,
        payload: dict[str, Any],
    ) -> bool:
        async with transaction(self._sessions) as session:
            inserted = await session.scalar(
                _inbox_statement(stream_name, message_id)
            )
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

    async def record_progress(
        self,
        *,
        stream_name: str,
        message_id: str,
        task_id: UUID,
        event_type: str,
        event_sequence: int,
        payload: dict[str, Any],
    ) -> bool:
        async with transaction(self._sessions) as session:
            inserted = await session.scalar(
                _inbox_statement(stream_name, message_id)
            )
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
            task.updated_by = EXECUTOR_ACTOR
            task.version += 1
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type=event_type,
                    payload=payload,
                )
            )
        return True


def _inbox_statement(stream_name: str, message_id: str):
    return (
        insert(StreamInbox)
        .values(stream_name=stream_name, message_id=message_id)
        .on_conflict_do_nothing(constraint="uq_agent_stream_message")
        .returning(StreamInbox.id)
    )


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


__all__ = [
    "ExecutionRepository",
    "ExecutorEventSequenceGapError",
]
