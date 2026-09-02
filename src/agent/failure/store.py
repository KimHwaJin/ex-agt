"""Durable first failure, terminal proof and atomic business completion."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, delete, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.admission.models import ApiRequestRow
from agent.failure.models import FailureCleanup
from agent.graph.state import TaskTurn
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import Message, SessionLock, Task, TaskEvent


class FailureStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def get(self, task_id: UUID) -> FailureCleanup | None:
        async with self.sessions() as session:
            return await session.get(FailureCleanup, task_id)

    async def defer_busy(self, task_id: UUID, delay: float) -> None:
        async with transaction(self.sessions) as session:
            row = await required(session, task_id)
            if row.state == "PENDING":
                row.next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=delay
                )
                touch(row)

    async def blocked_page(
        self,
        *,
        before: tuple[datetime, UUID] | None,
        limit: int,
    ) -> list[FailureCleanup]:
        if not 1 <= limit <= 100:
            raise ValueError("Page size must be between 1 and 100")
        async with self.sessions() as session:
            query = select(FailureCleanup).where(
                FailureCleanup.state == "BLOCKED"
            )
            if before is not None:
                updated_at, task_id = before
                query = query.where(
                    or_(
                        FailureCleanup.updated_at < updated_at,
                        and_(
                            FailureCleanup.updated_at == updated_at,
                            FailureCleanup.task_id < task_id,
                        ),
                    )
                )
            return list(
                await session.scalars(
                    query.order_by(
                        FailureCleanup.updated_at.desc(),
                        FailureCleanup.task_id.desc(),
                    ).limit(limit + 1)
                )
            )

    async def retry_blocked(
        self,
        task_id: UUID,
        *,
        operation_id: UUID,
        operation_hash: str,
        action: str,
        actor: str,
        reason: str,
        expected_version: int,
    ) -> tuple[FailureCleanup, bool]:
        if action not in {"RETRY", "FINALIZE"}:
            raise ValueError("Unsupported failure operation")
        async with transaction(self.sessions) as session:
            row = await required(session, task_id)
            if row.last_operation_id == operation_id:
                if row.last_operation_hash != operation_hash:
                    raise FailureOperationConflict(
                        "Idempotency key was reused with another request"
                    )
                return row, True
            if row.state != "BLOCKED":
                raise FailureOperationConflict(
                    f"Failure cleanup is not BLOCKED: {row.state}"
                )
            if row.version != expected_version:
                raise FailureOperationConflict(
                    "Failure cleanup version changed; refresh before retry"
                )
            previous_error = row.last_error
            row.state = "PENDING"
            row.attempts = 0
            row.next_attempt_at = datetime.now(UTC)
            row.last_error = None
            row.last_operation_id = operation_id
            row.last_operation_action = action
            row.last_operation_hash = operation_hash
            row.last_operation_reason = reason[:2000]
            row.last_operation_at = datetime.now(UTC)
            row.last_operation_by = actor
            touch(row, actor)
            session.add(
                TaskEvent(
                    task_id=task_id,
                    event_type="task.failure_cleanup_operator_requested",
                    payload={
                        "operation_id": str(operation_id),
                        "action": action,
                        "actor": actor,
                        "reason": row.last_operation_reason,
                        "previous_error": previous_error,
                        "version": row.version,
                    },
                )
            )
            await session.flush()
            return row, False

    async def ensure(
        self,
        task_id: UUID,
        session_id: str,
        *,
        source: dict,
        reason: str,
        execution_id: UUID | None = None,
    ) -> FailureCleanup:
        # All callers also hold the shared API/Worker session guard.
        async with transaction(self.sessions) as session:
            task = await session.get(Task, task_id, with_for_update=True)
            if task is None or task.session_id != session_id:
                raise ValueError("Failure source Task identity mismatch")
            existing = await session.get(FailureCleanup, task_id)
            if existing:
                if existing.session_id != session_id or (
                    execution_id
                    and existing.execution_id
                    and existing.execution_id != execution_id
                ):
                    raise ValueError("Existing failure binding mismatch")
                return existing
            if execution_id and task.execution_id not in (None, execution_id):
                raise ValueError("Failure source Execution identity mismatch")
            start = await session.scalar(
                select(ApiRequestRow).where(
                    ApiRequestRow.task_id == task_id,
                    ApiRequestRow.command["kind"].astext == "START",
                )
            )
            turn = (
                TaskTurn.model_validate(start.command["turn"])
                if start
                else TaskTurn(
                    user_id=task.user_id,
                    project_id=task.project_id,
                    session_id=task.session_id,
                    active_task_id=str(task.id),
                    current_input_message_id=str(task.input_message_id),
                    user_message=task.user_message,
                )
            )
            terminal = TaskStatus(task.status).is_terminal
            if terminal and not task.terminal_message:
                raise ValueError("Terminal Task has no durable result")
            row = FailureCleanup(
                task_id=task_id,
                session_id=session_id,
                turn=turn.model_dump(mode="json"),
                source=source,
                reason=reason[:2000],
                next_attempt_at=datetime.now(UTC),
                execution_id=execution_id or task.execution_id,
                preserve_terminal=terminal,
                final_status=task.status
                if terminal
                else TaskStatus.FAILED.value,
                message=task.terminal_message if terminal else None,
                executor_status="ALREADY_TERMINAL" if terminal else None,
            )
            session.add(row)
            if not terminal:
                lock = await session.get(
                    SessionLock, session_id, with_for_update=True
                )
                if lock and lock.active_task_id != task_id:
                    raise ValueError("Another Task owns the session lock")
                if lock is None:
                    session.add(
                        SessionLock(
                            session_id=session_id,
                            active_task_id=task_id,
                            execution_id=row.execution_id,
                        )
                    )
                else:
                    lock.locked = True
                session.add(
                    TaskEvent(
                        task_id=task_id,
                        event_type="task.failure_cleanup_pending",
                        payload={"source": source, "reason": row.reason},
                    )
                )
            await session.flush()
            return row

    async def bind_execution(
        self, record: FailureCleanup, execution_id: UUID
    ) -> FailureCleanup:
        """Persist a recovered Executor identity before cancellation."""

        async with transaction(self.sessions) as session:
            row = await fenced(session, record)
            if row.execution_id not in (None, execution_id):
                raise ValueError("Cleanup Execution identity cannot change")
            task = await session.get(Task, row.task_id, with_for_update=True)
            assert task is not None
            if task.execution_id not in (None, execution_id):
                raise ValueError("Task Execution identity cannot change")
            lock = await session.get(
                SessionLock, row.session_id, with_for_update=True
            )
            if lock is None or lock.active_task_id != row.task_id:
                raise ValueError("Cleanup session lock is missing")
            if lock.execution_id not in (None, execution_id):
                raise ValueError("Lock Execution identity cannot change")
            row.execution_id = execution_id
            task.execution_id = execution_id
            lock.execution_id = execution_id
            touch(row)
            task.updated_by = "AGENT"
            task.version += 1
            await session.flush()
            return row

    async def claim(
        self, task_id: UUID, *, max_attempts: int, delay: float
    ) -> FailureCleanup:
        async with transaction(self.sessions) as session:
            row = await required(session, task_id)
            if row.state != "PENDING":
                return row
            if row.attempts >= max_attempts:
                row.state = "BLOCKED"
                row.last_error = (
                    row.last_error or "Cleanup attempt limit reached"
                )
            else:
                row.attempts += 1
                row.next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=delay
                )
            touch(row)
            await session.flush()
            return row

    async def confirm(
        self,
        record: FailureCleanup,
        *,
        execution_id: UUID | None,
        status: str,
        message: str,
    ) -> FailureCleanup:
        if status not in {"NOT_REQUIRED", "SUCCEEDED", "FAILED", "CANCELLED"}:
            raise ValueError("Executor terminal proof is required")
        async with transaction(self.sessions) as session:
            row = await fenced(session, record)
            row.execution_id, row.executor_status = execution_id, status
            row.message = message
            touch(row)
            await session.flush()
            return row

    async def defer(
        self,
        record: FailureCleanup,
        error: str,
        *,
        delay: float,
        blocked: bool = False,
    ) -> None:
        async with transaction(self.sessions) as session:
            row = await fenced(session, record)
            row.state = "BLOCKED" if blocked else "PENDING"
            row.last_error = error[:2000]
            row.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
            touch(row)

    async def due(self, limit: int) -> list[UUID]:
        if not 1 <= limit <= 100:
            raise ValueError("Cleanup batch size must be between 1 and 100")
        async with self.sessions() as session:
            return list(
                await session.scalars(
                    select(FailureCleanup.task_id)
                    .where(
                        FailureCleanup.state == "PENDING",
                        FailureCleanup.next_attempt_at <= datetime.now(UTC),
                    )
                    .order_by(
                        FailureCleanup.next_attempt_at, FailureCleanup.task_id
                    )
                    .limit(limit)
                )
            )

    async def finish(self, record: FailureCleanup) -> None:
        if not record.executor_status or not record.message:
            raise ValueError("Cannot finish without terminal proof")
        async with transaction(self.sessions) as session:
            row = await fenced(session, record)
            task = await session.get(Task, row.task_id, with_for_update=True)
            assert task is not None
            if TaskStatus(task.status).is_terminal:
                if (
                    task.status != row.final_status
                    or task.terminal_message != row.message
                ):
                    raise ValueError("Existing terminal result cannot change")
            else:
                task.status, task.terminal_message = (
                    row.final_status,
                    row.message,
                )
                task.current_interrupt = None
                task.execution_id = row.execution_id
                task.updated_by = "AGENT"
                task.version += 1
                session.add(
                    Message(
                        task_id=row.task_id,
                        role="assistant",
                        content=row.message,
                        metadata_json={
                            "execution_id": str(row.execution_id)
                            if row.execution_id
                            else None,
                            "failure_source": row.source,
                            "executor_status": row.executor_status,
                        },
                    )
                )
                session.add(
                    TaskEvent(
                        task_id=row.task_id,
                        event_type="task.completed",
                        payload={
                            "status": row.final_status,
                            "failure_source": row.source,
                            "executor_status": row.executor_status,
                        },
                    )
                )
            await session.execute(
                update(ApiRequestRow)
                .where(
                    ApiRequestRow.task_id == row.task_id,
                    ApiRequestRow.state.in_(("PENDING", "RUNNING", "BLOCKED")),
                )
                .values(state="COMPENSATED", updated_by="AGENT")
            )
            await session.execute(
                delete(SessionLock).where(
                    SessionLock.active_task_id == row.task_id,
                )
            )
            row.state, row.last_error = "DONE", None
            touch(row)


async def required(session: AsyncSession, task_id: UUID) -> FailureCleanup:
    row = await session.get(FailureCleanup, task_id, with_for_update=True)
    if row is None:
        raise LookupError("Unknown failure cleanup")
    return row


async def fenced(
    session: AsyncSession, record: FailureCleanup
) -> FailureCleanup:
    row = await required(session, record.task_id)
    if row.state != "PENDING" or row.attempts != record.attempts:
        raise ValueError("Cleanup attempt was superseded")
    return row


class FailureOperationConflict(RuntimeError):
    pass


def touch(row: FailureCleanup, actor: str = "AGENT") -> None:
    row.version += 1
    row.updated_by = actor
