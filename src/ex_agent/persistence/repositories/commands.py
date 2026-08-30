from __future__ import annotations

from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import (
    Message,
    SessionLock,
    TaskEvent,
    WorkflowCommand,
)
from ex_agent.persistence.repositories.tasks import required_task


class CommandRepository:
    """Workflow command state and atomic failure compensation."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

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
            if resolved is not None:
                return resolved
            existing = await session.scalar(
                select(WorkflowCommand).where(
                    WorkflowCommand.idempotency_key == idempotency_key
                )
            )
            if existing is None:
                raise RuntimeError("Command idempotency conflict was lost")
            if (
                existing.task_id != task_id
                or existing.command_type != command_type
                or existing.payload != payload
            ):
                raise ValueError("Idempotency key payload mismatch")
            return existing.id

    async def set_state(
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

    async def get(self, command_id: UUID) -> WorkflowCommand | None:
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


__all__ = ["CommandRepository"]
