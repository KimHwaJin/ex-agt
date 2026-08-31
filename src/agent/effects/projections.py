"""Idempotent business writes after a journaled Executor response."""

from typing import Any
from uuid import UUID

from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    ExecutorBinding,
    Message,
    SessionLock,
    Task,
    TaskEvent,
)


class EffectProjections:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def binding(
        self,
        task_id: UUID,
        *,
        execution_id: UUID,
        execution_version: int,
        operation_id: UUID | None = None,
        next_step_sequence: int | None = None,
        create: bool = False,
    ) -> None:
        async with transaction(self.sessions) as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise LookupError("Unknown effect Task")
            binding = await session.get(
                ExecutorBinding, task_id, with_for_update=True
            )
            if binding is None:
                if not create or operation_id is None:
                    raise LookupError("Executor binding does not exist")
                if next_step_sequence is None:
                    raise ValueError("Initial binding requires Step sequence")
                if task.execution_id not in (None, execution_id):
                    raise ValueError("Task is bound to another Execution")
                task.execution_id = execution_id
                task.updated_by = "AGENT"
                lock = await session.get(SessionLock, task.session_id)
                if lock and lock.active_task_id == task_id:
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
                return
            if binding.execution_id != execution_id:
                raise ValueError("Task is bound to another Execution")
            if next_step_sequence is not None:
                if next_step_sequence == binding.next_step_sequence:
                    if operation_id != binding.operation_id:
                        raise ValueError("Step sequence has another Operation")
                elif next_step_sequence > binding.next_step_sequence:
                    if operation_id is None or create:
                        raise ValueError("Invalid binding sequence advance")
                    binding.next_step_sequence = next_step_sequence
                    binding.operation_id = operation_id
                # An obsolete receipt must not rewind the current Operation.
            binding.execution_version = max(
                binding.execution_version, execution_version
            )

    async def terminal(
        self,
        task_id: UUID,
        *,
        status: TaskStatus,
        message: str,
        metadata: dict[str, Any],
    ) -> None:
        if not status.is_terminal:
            raise ValueError("Terminal projection requires terminal status")
        async with transaction(self.sessions) as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise LookupError("Unknown terminal Task")
            if TaskStatus(task.status).is_terminal:
                previous = await session.scalar(
                    select(Message)
                    .where(
                        Message.task_id == task_id,
                        Message.role == "assistant",
                    )
                    .order_by(Message.created_at.desc())
                )
                if (
                    task.status != status.value
                    or task.terminal_message != message
                    or previous is None
                    or previous.content != message
                    or previous.metadata_json != metadata
                ):
                    raise ValueError("Terminal Task result cannot change")
                return
            task.status = status.value
            task.terminal_message = message
            task.current_interrupt = None
            task.updated_by = "AGENT"
            task.version += 1
            session.add(
                Message(
                    task_id=task_id,
                    role="assistant",
                    content=message,
                    metadata_json=metadata,
                )
            )
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.completed",
                    payload={"status": status.value},
                )
            )
            await session.execute(
                delete(SessionLock).where(
                    SessionLock.active_task_id == task_id
                )
            )

    async def promotion(self, task_id: UUID) -> None:
        async with transaction(self.sessions) as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None or task.status != TaskStatus.SUCCEEDED.value:
                raise ValueError("Promotion requires a successful Task")
            exists = await session.scalar(
                select(TaskEvent.id)
                .where(
                    TaskEvent.task_id == task_id,
                    TaskEvent.event_type == "workflow.promotion_available",
                )
                .limit(1)
            )
            if exists is None:
                session.add(
                    TaskEvent(
                        task_id=task_id,
                        event_type="workflow.promotion_available",
                        payload={
                            "task_id": str(task_id),
                            "draft_path": (
                                f"/api/v1/tasks/{task_id}/workflow-promotion-draft"
                            ),
                        },
                    )
                )
