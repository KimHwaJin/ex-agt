"""Idempotent Task read-model projection from durable graph snapshots."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.audit import AGENT_ACTOR
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import Task, TaskEvent


class TaskStateProjector:
    """Project one stable checkpoint without duplicating Task events."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self.sessions = sessions

    async def __call__(self, snapshot: Any) -> None:
        values = snapshot.values
        task_id = values.get("active_task_id")
        workflow = values.get("workflow")
        if not task_id or not isinstance(workflow, dict):
            return
        phase = TaskStatus(workflow["phase"])
        boundaries = [
            boundary for task in snapshot.tasks for boundary in task.interrupts
        ]
        if len(boundaries) > 1:
            raise ValueError("Task projection requires one interrupt at most")
        interrupt = None
        if boundaries:
            value = boundaries[0].value
            if not isinstance(value, dict):
                raise TypeError("Graph interrupt payload must be an object")
            interrupt = {**value, "interrupt_id": boundaries[0].id}
        status = _projected_status(phase, interrupt)
        await self._write(UUID(task_id), status, interrupt)

    async def _write(
        self,
        task_id: UUID,
        status: TaskStatus,
        interrupt: dict[str, Any] | None,
    ) -> None:
        async with transaction(self.sessions) as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None:
                raise LookupError("Graph Task projection target is missing")
            current = TaskStatus(task.status)
            if current.is_terminal:
                return
            if status.is_terminal:
                raise ValueError(
                    "Terminal graph state requires a committed Task result"
                )
            if task.status == status.value and (
                task.current_interrupt == interrupt
            ):
                return
            previous_interrupt = task.current_interrupt
            task.status = status.value
            task.current_interrupt = interrupt
            task.updated_by = AGENT_ACTOR
            task.version += 1
            if interrupt is not None:
                event_type = "task.interrupted"
                payload = interrupt
            else:
                event_type = "task.status_changed"
                payload = {
                    "status": status.value,
                    "interrupt_cleared": previous_interrupt is not None,
                }
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type=event_type,
                    payload=payload,
                )
            )


def _projected_status(
    phase: TaskStatus,
    interrupt: dict[str, Any] | None,
) -> TaskStatus:
    if interrupt is None:
        return phase
    kind = interrupt.get("kind")
    if kind == "PLAN_REVIEW":
        return TaskStatus.WAITING_FOR_APPROVAL
    if kind == "EXECUTOR_EVENT":
        return TaskStatus.WAITING_FOR_EXECUTOR_EVENT
    return TaskStatus.PLANNING


__all__ = ["TaskStateProjector"]
