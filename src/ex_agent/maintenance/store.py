from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import and_, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.maintenance.models import StreamMaintenanceJob
from ex_agent.persistence.database import transaction


class StreamMaintenanceConflict(RuntimeError):
    pass


class StreamMaintenanceStore:
    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def get(self, job_id: UUID) -> StreamMaintenanceJob | None:
        async with self._sessions() as session:
            return await session.get(StreamMaintenanceJob, job_id)

    async def create(
        self,
        *,
        stream_alias: str,
        stream_key: str,
        action: str,
        actor: str,
        idempotency_key: str,
        request_hash: str,
        reason: str,
        retention_seconds: int,
        minimum_retained_entries: int,
    ) -> tuple[StreamMaintenanceJob, bool]:
        try:
            async with transaction(self._sessions) as session:
                existing = await session.scalar(
                    select(StreamMaintenanceJob)
                    .where(
                        StreamMaintenanceJob.created_by == actor,
                        StreamMaintenanceJob.idempotency_key
                        == idempotency_key,
                    )
                    .with_for_update()
                )
                if existing is not None:
                    return _replay(existing, request_hash)
                row = StreamMaintenanceJob(
                    stream_alias=stream_alias,
                    stream_key=stream_key,
                    action=action,
                    idempotency_key=idempotency_key,
                    request_hash=request_hash,
                    reason=reason[:2000],
                    retention_seconds=retention_seconds,
                    minimum_retained_entries=minimum_retained_entries,
                    next_attempt_at=datetime.now(UTC),
                    created_by=actor,
                    updated_by=actor,
                )
                session.add(row)
                await session.flush()
                return row, False
        except IntegrityError as error:
            existing = await self.by_idempotency(actor, idempotency_key)
            if existing is not None:
                return _replay(existing, request_hash)
            raise StreamMaintenanceConflict(
                f"Another trim is active for Stream: {stream_alias}"
            ) from error

    async def by_idempotency(
        self,
        actor: str,
        idempotency_key: str,
    ) -> StreamMaintenanceJob | None:
        async with self._sessions() as session:
            return await session.scalar(
                select(StreamMaintenanceJob).where(
                    StreamMaintenanceJob.created_by == actor,
                    StreamMaintenanceJob.idempotency_key == idempotency_key,
                )
            )

    async def page(
        self,
        *,
        before: tuple[datetime, UUID] | None,
        limit: int,
    ) -> list[StreamMaintenanceJob]:
        if not 1 <= limit <= 100:
            raise ValueError("Page size must be between 1 and 100")
        async with self._sessions() as session:
            query = select(StreamMaintenanceJob)
            if before is not None:
                created_at, job_id = before
                query = query.where(
                    or_(
                        StreamMaintenanceJob.created_at < created_at,
                        and_(
                            StreamMaintenanceJob.created_at == created_at,
                            StreamMaintenanceJob.id < job_id,
                        ),
                    )
                )
            return list(
                await session.scalars(
                    query.order_by(
                        StreamMaintenanceJob.created_at.desc(),
                        StreamMaintenanceJob.id.desc(),
                    ).limit(limit + 1)
                )
            )

    async def due(
        self,
        *,
        limit: int,
        claim_timeout_seconds: float,
    ) -> list[UUID]:
        now = datetime.now(UTC)
        stale = now - timedelta(seconds=claim_timeout_seconds)
        async with self._sessions() as session:
            return list(
                await session.scalars(
                    select(StreamMaintenanceJob.id)
                    .where(
                        or_(
                            and_(
                                StreamMaintenanceJob.state == "PENDING",
                                StreamMaintenanceJob.next_attempt_at <= now,
                            ),
                            and_(
                                StreamMaintenanceJob.state == "RUNNING",
                                StreamMaintenanceJob.claimed_at < stale,
                            ),
                        )
                    )
                    .order_by(
                        StreamMaintenanceJob.next_attempt_at,
                        StreamMaintenanceJob.id,
                    )
                    .limit(limit)
                )
            )

    async def claim(
        self,
        job_id: UUID,
        *,
        claim_timeout_seconds: float,
        actor: str = "WORKER",
    ) -> StreamMaintenanceJob | None:
        now = datetime.now(UTC)
        stale = now - timedelta(seconds=claim_timeout_seconds)
        async with transaction(self._sessions) as session:
            row = await session.get(
                StreamMaintenanceJob,
                job_id,
                with_for_update=True,
            )
            if row is None or row.state not in {"PENDING", "RUNNING"}:
                return None
            if row.state == "RUNNING" and (
                row.claimed_at is None or row.claimed_at >= stale
            ):
                return None
            if row.state == "PENDING" and row.next_attempt_at > now:
                return None
            row.state = "RUNNING"
            row.claimed_at = now
            row.attempts += 1
            row.updated_by = actor
            await session.flush()
            return row

    async def succeed(
        self,
        job_id: UUID,
        *,
        attempt: int,
        result: dict,
        actor: str = "WORKER",
    ) -> None:
        async with transaction(self._sessions) as session:
            row = await _claimed(session, job_id, attempt)
            row.state = "SUCCEEDED"
            row.result = result
            row.last_error = None
            row.claimed_at = None
            row.updated_by = actor

    async def fail(
        self,
        job_id: UUID,
        *,
        attempt: int,
        error: str,
        max_attempts: int,
        retry_seconds: float,
        actor: str = "WORKER",
    ) -> None:
        async with transaction(self._sessions) as session:
            row = await _claimed(session, job_id, attempt)
            row.last_error = error[:2000]
            row.claimed_at = None
            row.updated_by = actor
            if row.attempts >= max_attempts:
                row.state = "FAILED"
            else:
                row.state = "PENDING"
                row.next_attempt_at = datetime.now(UTC) + timedelta(
                    seconds=retry_seconds
                )


async def _claimed(
    session: AsyncSession,
    job_id: UUID,
    attempt: int,
) -> StreamMaintenanceJob:
    row = await session.get(
        StreamMaintenanceJob,
        job_id,
        with_for_update=True,
    )
    if row is None:
        raise LookupError(f"Unknown Stream maintenance job: {job_id}")
    if row.state != "RUNNING" or row.attempts != attempt:
        raise StreamMaintenanceConflict("Maintenance claim was superseded")
    return row


def _replay(
    row: StreamMaintenanceJob,
    request_hash: str,
) -> tuple[StreamMaintenanceJob, bool]:
    if row.request_hash != request_hash:
        raise StreamMaintenanceConflict(
            "Idempotency key was reused with another request"
        )
    return row, True


__all__ = ["StreamMaintenanceConflict", "StreamMaintenanceStore"]
