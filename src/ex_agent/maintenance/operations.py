from __future__ import annotations

import hashlib
import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import Error as BinasciiError
from datetime import datetime
from uuid import UUID

from ex_agent.config import Settings
from ex_agent.maintenance.contracts import (
    StreamMaintenanceJobView,
    StreamMaintenanceOperationResult,
    StreamMaintenancePage,
    StreamMaintenanceRequest,
)
from ex_agent.maintenance.models import StreamMaintenanceJob
from ex_agent.maintenance.recovery import StreamMaintenanceRecovery
from ex_agent.maintenance.store import StreamMaintenanceStore


class StreamMaintenanceForbidden(PermissionError):
    pass


class StreamMaintenancePolicy:
    def __init__(self, settings: Settings) -> None:
        self._actors = frozenset(
            value.strip()
            for value in settings.stream_maintenance_operator_user_ids.split(
                ","
            )
            if value.strip()
        )
        self._retention_floor = settings.stream_retention_seconds
        self._minimum_entries_floor = settings.stream_minimum_retained_entries

    def authorize(self, actor: str) -> None:
        if actor not in self._actors:
            raise StreamMaintenanceForbidden(
                "User is not authorized for Stream maintenance"
            )

    def resolve(self, request: StreamMaintenanceRequest) -> tuple[int, int]:
        retention = request.retention_seconds or self._retention_floor
        minimum_entries = (
            request.minimum_retained_entries
            if request.minimum_retained_entries is not None
            else self._minimum_entries_floor
        )
        if retention < self._retention_floor:
            raise ValueError(
                "retention_seconds cannot be below the server policy"
            )
        if minimum_entries < self._minimum_entries_floor:
            raise ValueError(
                "minimum_retained_entries cannot be below the server policy"
            )
        return retention, minimum_entries


class StreamMaintenanceOperations:
    def __init__(
        self,
        settings: Settings,
        store: StreamMaintenanceStore,
        recovery: StreamMaintenanceRecovery,
    ) -> None:
        self._store = store
        self._recovery = recovery
        self._policy = StreamMaintenancePolicy(settings)
        self._streams = {
            "agent_commands": settings.agent_command_stream,
            "agent_command_dlq": settings.agent_command_dead_letter_stream,
            "executor_events": settings.executor_event_stream,
            "executor_event_dlq": (settings.executor_event_dead_letter_stream),
            "product_events": settings.agent_product_event_stream,
        }

    async def plan(
        self,
        *,
        actor: str,
        request: StreamMaintenanceRequest,
    ) -> StreamMaintenanceOperationResult:
        row, replayed = await self._create("PLAN", actor, request)
        if row.state == "PENDING":
            await self._recovery.execute(row.id, actor=actor)
            row = await self._required(row.id)
        return StreamMaintenanceOperationResult(
            job=_view(row),
            operation_replayed=replayed,
        )

    async def submit_trim(
        self,
        *,
        actor: str,
        request: StreamMaintenanceRequest,
    ) -> StreamMaintenanceOperationResult:
        row, replayed = await self._create("TRIM", actor, request)
        return StreamMaintenanceOperationResult(
            job=_view(row),
            operation_replayed=replayed,
        )

    async def detail(
        self,
        job_id: UUID,
        *,
        actor: str,
    ) -> StreamMaintenanceJobView:
        self._policy.authorize(actor)
        return _view(await self._required(job_id))

    async def jobs(
        self,
        *,
        actor: str,
        cursor: str | None,
        limit: int,
    ) -> StreamMaintenancePage:
        self._policy.authorize(actor)
        before = _decode_cursor(cursor) if cursor else None
        rows = await self._store.page(before=before, limit=limit)
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = _encode_cursor(last.created_at, last.id)
        return StreamMaintenancePage(
            items=[_view(row) for row in visible],
            next_cursor=next_cursor,
            has_more=has_more,
        )

    async def _create(
        self,
        action: str,
        actor: str,
        request: StreamMaintenanceRequest,
    ) -> tuple[StreamMaintenanceJob, bool]:
        self._policy.authorize(actor)
        if request.stream not in self._streams:
            raise ValueError(f"Unknown registered Stream: {request.stream}")
        retention, minimum_entries = self._policy.resolve(request)
        request_hash = _request_hash(
            action,
            request,
            retention,
            minimum_entries,
        )
        return await self._store.create(
            stream_alias=request.stream,
            stream_key=self._streams[request.stream],
            action=action,
            actor=actor,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            reason=request.reason,
            retention_seconds=retention,
            minimum_retained_entries=minimum_entries,
        )

    async def _required(self, job_id: UUID) -> StreamMaintenanceJob:
        row = await self._store.get(job_id)
        if row is None:
            raise LookupError(f"Unknown Stream maintenance job: {job_id}")
        return row


def _view(row: StreamMaintenanceJob) -> StreamMaintenanceJobView:
    return StreamMaintenanceJobView(
        job_id=row.id,
        stream=row.stream_alias,
        action=row.action,
        state=row.state,
        reason=row.reason,
        retention_seconds=row.retention_seconds,
        minimum_retained_entries=row.minimum_retained_entries,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        result=row.result,
        last_error=row.last_error,
        created_at=row.created_at,
        updated_at=row.updated_at,
        created_by=row.created_by,
        updated_by=row.updated_by,
    )


def _request_hash(
    action: str,
    request: StreamMaintenanceRequest,
    retention: int,
    minimum_entries: int,
) -> str:
    payload = {
        "action": action,
        "stream": request.stream,
        "idempotency_key": request.idempotency_key,
        "reason": request.reason,
        "retention_seconds": retention,
        "minimum_retained_entries": minimum_entries,
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _encode_cursor(created_at: datetime, job_id: UUID) -> str:
    payload = json.dumps(
        {
            "kind": "stream_maintenance",
            "created_at": created_at.isoformat(),
            "job_id": str(job_id),
        },
        separators=(",", ":"),
    ).encode()
    return urlsafe_b64encode(payload).decode().rstrip("=")


def _decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(urlsafe_b64decode(cursor + padding))
        if not isinstance(payload, dict):
            raise ValueError("Cursor payload must be an object")
        if payload.get("kind") != "stream_maintenance":
            raise ValueError("Cursor kind does not match")
        created_at = datetime.fromisoformat(payload["created_at"])
        if created_at.tzinfo is None:
            raise ValueError("Cursor timestamp has no timezone")
        return created_at, UUID(payload["job_id"])
    except (
        BinasciiError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError("Invalid Stream maintenance cursor") from error


__all__ = [
    "StreamMaintenanceForbidden",
    "StreamMaintenanceOperations",
    "StreamMaintenancePolicy",
]
