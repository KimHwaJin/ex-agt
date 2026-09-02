"""Authorized, idempotent operations over BLOCKED failure cleanup."""

from __future__ import annotations

import hashlib
import json
from base64 import urlsafe_b64decode, urlsafe_b64encode
from binascii import Error as BinasciiError
from datetime import datetime
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from agent.failure.contracts import (
    FailureCleanupPage,
    FailureCleanupView,
    FailureOperationInput,
    FailureOperationResult,
)
from agent.failure.models import FailureCleanup
from agent.failure.store import FailureOperationConflict


class FailureOperationService(Protocol):
    async def execute(self, task_id: UUID) -> None: ...


class FailureOperationStore(Protocol):
    async def get(self, task_id: UUID) -> FailureCleanup | None: ...

    async def blocked_page(
        self,
        *,
        before: tuple[datetime, UUID] | None,
        limit: int,
    ) -> list[FailureCleanup]: ...

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
    ) -> tuple[FailureCleanup, bool]: ...


class FailureOperationsForbidden(PermissionError):
    pass


class FailureOperatorPolicy:
    def __init__(self, user_ids: str) -> None:
        self._user_ids = frozenset(
            value.strip() for value in user_ids.split(",") if value.strip()
        )

    def authorize(self, actor: str) -> None:
        if actor not in self._user_ids:
            raise FailureOperationsForbidden(
                "User is not authorized for failure operations"
            )


class FailureOperations:
    def __init__(
        self,
        service: FailureOperationService,
        store: FailureOperationStore,
        policy: FailureOperatorPolicy,
    ) -> None:
        self._service = service
        self._store = store
        self._policy = policy

    async def blocked(
        self,
        *,
        actor: str,
        cursor: str | None,
        limit: int,
    ) -> FailureCleanupPage:
        self._policy.authorize(actor)
        before = _decode_cursor(cursor) if cursor else None
        rows = await self._store.blocked_page(before=before, limit=limit)
        has_more = len(rows) > limit
        visible = rows[:limit]
        next_cursor = None
        if has_more and visible:
            last = visible[-1]
            next_cursor = _encode_cursor(last.updated_at, last.task_id)
        return FailureCleanupPage(
            items=[_view(row) for row in visible],
            next_cursor=next_cursor,
            has_more=has_more,
        )

    async def detail(
        self,
        task_id: UUID,
        *,
        actor: str,
    ) -> FailureCleanupView:
        self._policy.authorize(actor)
        row = await self._store.get(task_id)
        if row is None:
            raise LookupError(f"Unknown failure cleanup: {task_id}")
        return _view(row)

    async def retry(
        self,
        task_id: UUID,
        *,
        actor: str,
        request: FailureOperationInput,
    ) -> FailureOperationResult:
        return await self._request(
            task_id,
            action="RETRY",
            actor=actor,
            request=request,
            finalize=False,
        )

    async def finalize(
        self,
        task_id: UUID,
        *,
        actor: str,
        request: FailureOperationInput,
    ) -> FailureOperationResult:
        return await self._request(
            task_id,
            action="FINALIZE",
            actor=actor,
            request=request,
            finalize=True,
        )

    async def _request(
        self,
        task_id: UUID,
        *,
        action: str,
        actor: str,
        request: FailureOperationInput,
        finalize: bool,
    ) -> FailureOperationResult:
        self._policy.authorize(actor)
        operation_id = uuid5(
            NAMESPACE_URL,
            "ex-agent/failure-operation/"
            f"{actor}/{task_id}/{request.idempotency_key}",
        )
        request_hash = _request_hash(task_id, action, request)
        row, replayed = await self._store.retry_blocked(
            task_id,
            operation_id=operation_id,
            operation_hash=request_hash,
            action=action,
            actor=actor,
            reason=request.reason,
            expected_version=request.expected_version,
        )
        if finalize and row.state == "PENDING":
            await self._service.execute(task_id)
            row = await self._store.get(task_id)
            if row is None:
                raise LookupError(f"Unknown failure cleanup: {task_id}")
        if finalize and row.state != "DONE":
            raise FailureOperationConflict(
                row.last_error
                or "Terminal Executor and checkpoint evidence is incomplete"
            )
        return FailureOperationResult(
            cleanup=_view(row),
            operation_replayed=replayed,
        )


def _view(row: FailureCleanup) -> FailureCleanupView:
    return FailureCleanupView(
        task_id=row.task_id,
        session_id=row.session_id,
        state=row.state,
        version=row.version,
        reason=row.reason,
        source=row.source,
        attempts=row.attempts,
        next_attempt_at=row.next_attempt_at,
        last_error=row.last_error,
        execution_id=row.execution_id,
        executor_status=row.executor_status,
        evidence_complete=bool(row.executor_status and row.message),
        preserve_terminal=row.preserve_terminal,
        final_status=row.final_status,
        message=row.message,
        last_operation_id=row.last_operation_id,
        last_operation_action=row.last_operation_action,
        last_operation_reason=row.last_operation_reason,
        last_operation_at=row.last_operation_at,
        last_operation_by=row.last_operation_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
        created_by=row.created_by,
        updated_by=row.updated_by,
    )


def _request_hash(
    task_id: UUID,
    action: str,
    request: FailureOperationInput,
) -> str:
    payload = {
        "task_id": str(task_id),
        "action": action,
        **request.model_dump(mode="json"),
    }
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _encode_cursor(updated_at: datetime, task_id: UUID) -> str:
    payload = json.dumps(
        {
            "kind": "blocked_failure",
            "updated_at": updated_at.isoformat(),
            "task_id": str(task_id),
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
        if payload.get("kind") != "blocked_failure":
            raise ValueError("Cursor kind does not match")
        updated_at = datetime.fromisoformat(payload["updated_at"])
        if updated_at.tzinfo is None:
            raise ValueError("Cursor timestamp has no timezone")
        return updated_at, UUID(payload["task_id"])
    except (
        BinasciiError,
        KeyError,
        TypeError,
        ValueError,
        json.JSONDecodeError,
    ) as error:
        raise ValueError("Invalid blocked failure cursor") from error


__all__ = [
    "FailureOperationConflict",
    "FailureOperations",
    "FailureOperationsForbidden",
    "FailureOperatorPolicy",
]
