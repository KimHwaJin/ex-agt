from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import TaskEvent, WorkflowCommand


class DeliveryRepository:
    """Transactional command and product-event outbox persistence."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

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

    async def backlog_counts(self) -> dict[tuple[str, str], int]:
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


__all__ = ["DeliveryRepository"]
