"""Short DB transactions for API admission, never around graph/LLM calls."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.admission.contracts import ApiRequest, RequestRecord
from agent.admission.models import ApiRequestRow
from agent.session import SessionConflictError
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import Message, SessionLock, Task, TaskEvent

ACTIVE = ("PENDING", "RUNNING", "BLOCKED")


class RequestStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self.sessions = sessions

    async def get(self, request_id: UUID) -> RequestRecord | None:
        async with self.sessions() as session:
            row = await session.get(ApiRequestRow, request_id)
            return _record(row) if row else None

    async def accept(
        self,
        command: ApiRequest,
        *,
        target_node: str,
        base_checkpoint_id: str | None,
    ) -> RequestRecord:
        try:
            async with transaction(self.sessions) as session:
                existing = await session.get(ApiRequestRow, command.request_id)
                if existing:
                    validate_command(_record(existing), command)
                    return _record(existing)
                active = await session.scalar(
                    select(ApiRequestRow.request_id).where(
                        ApiRequestRow.session_id == command.turn.session_id,
                        ApiRequestRow.state.in_(ACTIVE),
                    )
                )
                if active is not None:
                    raise SessionConflictError(
                        "Session has a pending API request"
                    )
                await _admit_task(session, command)
                row = ApiRequestRow(
                    request_id=command.request_id,
                    task_id=UUID(command.turn.active_task_id),
                    session_id=command.turn.session_id,
                    command=command.model_dump(mode="json"),
                    fingerprint=command.fingerprint,
                    target_node=target_node,
                    base_checkpoint_id=base_checkpoint_id,
                    next_attempt_at=datetime.now(UTC),
                    created_by=command.turn.user_id,
                    updated_by=command.turn.user_id,
                )
                session.add(row)
                await session.flush()
                return _record(row)
        except IntegrityError as error:
            # Constraint arbitration also protects a lost session lease.
            existing = await self.get(command.request_id)
            if existing is not None:
                validate_command(existing, command)
                return existing
            raise SessionConflictError(
                "API admission conflicts with existing work"
            ) from error

    async def claim(
        self,
        request_id: UUID,
        *,
        max_attempts: int,
        recovery_delay: float,
    ) -> RequestRecord:
        async with transaction(self.sessions) as session:
            row = await _required(session, request_id)
            if row.state not in ("PENDING", "RUNNING"):
                return _record(row)
            if row.attempts >= max_attempts:
                row.state = "BLOCKED"
                row.last_error = (
                    row.last_error or "API recovery attempt limit reached"
                )
            else:
                row.state = "RUNNING"
                row.attempts += 1
                row.next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=recovery_delay
                )
            row.updated_by = "AGENT"
            await session.flush()
            return _record(row)

    async def finish(
        self,
        record: RequestRecord,
        *,
        state: str,
        error: str | None = None,
        delay: float = 0,
    ) -> RequestRecord:
        if state not in ("PENDING", "APPLIED", "REJECTED", "BLOCKED"):
            raise ValueError("Invalid API request transition")
        async with transaction(self.sessions) as session:
            row = await _required(session, record.command.request_id)
            if (
                record.state not in ("PENDING", "RUNNING")
                or row.state != record.state
                or row.attempts != record.attempts
            ):
                raise SessionConflictError(
                    "API request attempt was superseded"
                )
            row.state = state
            row.last_error = error
            row.next_attempt_at = datetime.now(UTC) + timedelta(seconds=delay)
            row.updated_by = "AGENT"
            await session.flush()
            return _record(row)

    async def due(self, *, limit: int = 32) -> list[UUID]:
        if not 1 <= limit <= 100:
            raise ValueError("Recovery batch limit must be between 1 and 100")
        async with self.sessions() as session:
            result = await session.scalars(
                select(ApiRequestRow.request_id)
                .where(
                    ApiRequestRow.state.in_(("PENDING", "RUNNING")),
                    ApiRequestRow.next_attempt_at <= datetime.now(UTC),
                )
                .order_by(
                    ApiRequestRow.next_attempt_at, ApiRequestRow.request_id
                )
                .limit(limit)
            )
            return list(result)

    async def defer_busy(self, request_id: UUID, delay: float) -> None:
        # Do not spend attempt budget on lock contention, and avoid starvation
        # of other sessions when a busy request sorts first on every poll.
        async with transaction(self.sessions) as session:
            row = await _required(session, request_id)
            if row.state in ("PENDING", "RUNNING"):
                row.next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=delay
                )
                row.updated_by = "AGENT"


async def _admit_task(session: AsyncSession, command: ApiRequest) -> None:
    turn = command.turn
    task_id = UUID(turn.active_task_id)
    task = await session.get(Task, task_id, with_for_update=True)
    if command.kind != "START":
        if (
            task is None
            or any(
                getattr(task, field) != getattr(turn, field)
                for field in (
                    "user_id",
                    "project_id",
                    "session_id",
                    "user_message",
                )
            )
            or task.input_message_id != UUID(turn.current_input_message_id)
        ):
            raise SessionConflictError("Task identity mismatch")
        if TaskStatus(task.status).is_terminal:
            raise SessionConflictError("Terminal Task cannot accept new input")
        return
    if task is not None:
        raise SessionConflictError(
            "Task ID already exists; reuse its request ID"
        )
    lock = await session.get(SessionLock, turn.session_id)
    unfinished = await session.scalar(
        select(Task.id)
        .where(
            Task.session_id == turn.session_id,
            Task.status.not_in(
                [status.value for status in TaskStatus if status.is_terminal]
            ),
        )
        .limit(1)
    )
    if (lock and lock.locked) or unfinished is not None:
        raise SessionConflictError("Session has unfinished work")
    session.add(
        Task(
            id=task_id,
            input_message_id=UUID(turn.current_input_message_id),
            user_id=turn.user_id,
            project_id=turn.project_id,
            session_id=turn.session_id,
            user_message=turn.user_message,
            status=TaskStatus.ACCEPTED.value,
            created_by=turn.user_id,
            updated_by=turn.user_id,
        )
    )
    await session.flush()
    session.add(
        Message(
            id=UUID(turn.current_input_message_id),
            task_id=task_id,
            role="user",
            content=turn.user_message,
        )
    )
    session.add(
        TaskEvent(
            task_id=task_id,
            event_type="task.accepted",
            payload={"status": TaskStatus.ACCEPTED.value},
        )
    )
    # Deliberately no WorkflowCommand START/RESUME insertion here.


async def _required(session: AsyncSession, request_id: UUID) -> ApiRequestRow:
    row = await session.get(ApiRequestRow, request_id, with_for_update=True)
    if row is None:
        raise LookupError("Unknown API request")
    return row


def validate_command(record: RequestRecord, command: ApiRequest) -> None:
    if record.command != command or record.fingerprint != command.fingerprint:
        raise SessionConflictError(
            "Request ID was reused with different input"
        )


def _record(row: ApiRequestRow) -> RequestRecord:
    command = ApiRequest.model_validate(row.command)
    if command.fingerprint != row.fingerprint:
        raise ValueError("Stored API request fingerprint mismatch")
    return RequestRecord.model_validate(
        {
            "command": command,
            "fingerprint": row.fingerprint,
            "target_node": row.target_node,
            "base_checkpoint_id": row.base_checkpoint_id,
            "state": row.state,
            "attempts": row.attempts,
            "next_attempt_at": row.next_attempt_at,
            "last_error": row.last_error,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
            "created_by": row.created_by,
            "updated_by": row.updated_by,
        }
    )
