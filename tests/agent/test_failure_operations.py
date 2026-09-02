from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from agent.failure.contracts import FailureOperationInput
from agent.failure.operations import (
    FailureOperationConflict,
    FailureOperations,
    FailureOperationService,
    FailureOperationsForbidden,
    FailureOperationStore,
    FailureOperatorPolicy,
)


def cleanup_row(**changes):
    now = datetime.now(UTC)
    values = {
        "task_id": uuid4(),
        "session_id": "session-1",
        "state": "BLOCKED",
        "version": 3,
        "reason": "handler failed",
        "source": {"kind": "API"},
        "attempts": 20,
        "next_attempt_at": now,
        "last_error": "model unavailable",
        "execution_id": None,
        "executor_status": None,
        "preserve_terminal": False,
        "final_status": "FAILED",
        "message": None,
        "last_operation_id": None,
        "last_operation_action": None,
        "last_operation_reason": None,
        "last_operation_at": None,
        "last_operation_by": None,
        "created_at": now - timedelta(minutes=1),
        "updated_at": now,
        "created_by": "AGENT",
        "updated_by": "AGENT",
    }
    values.update(changes)
    return SimpleNamespace(**values)


def request(**changes) -> FailureOperationInput:
    values = {
        "idempotency_key": "retry-1",
        "expected_version": 3,
        "reason": "operator verified the incident",
    }
    values.update(changes)
    return FailureOperationInput(**values)


def operations(row=None, *, allowed="operator-1"):
    current = row or cleanup_row()
    store = SimpleNamespace(
        blocked_page=AsyncMock(return_value=[current]),
        get=AsyncMock(return_value=current),
        retry_blocked=AsyncMock(return_value=(current, False)),
    )
    service = SimpleNamespace(execute=AsyncMock())
    instance = FailureOperations(
        cast(FailureOperationService, service),
        cast(FailureOperationStore, store),
        FailureOperatorPolicy(allowed),
    )
    return instance, store, service, current


@pytest.mark.asyncio
async def test_empty_operator_policy_denies_every_operation() -> None:
    instance, *_ = operations(allowed="")

    with pytest.raises(FailureOperationsForbidden):
        await instance.blocked(actor="operator-1", cursor=None, limit=50)


@pytest.mark.asyncio
async def test_blocked_page_uses_opaque_keyset_cursor() -> None:
    first, second = cleanup_row(), cleanup_row()
    instance, store, *_ = operations(first)
    store.blocked_page.return_value = [first, second]

    page = await instance.blocked(actor="operator-1", cursor=None, limit=1)
    store.blocked_page.return_value = []
    await instance.blocked(
        actor="operator-1",
        cursor=page.next_cursor,
        limit=1,
    )

    assert page.has_more is True
    assert page.items[0].task_id == first.task_id
    assert store.blocked_page.await_args_list[1].kwargs["before"] == (
        first.updated_at,
        first.task_id,
    )


@pytest.mark.asyncio
async def test_invalid_cursor_is_rejected() -> None:
    instance, *_ = operations()

    with pytest.raises(ValueError, match="Invalid blocked failure cursor"):
        await instance.blocked(
            actor="operator-1",
            cursor="not-a-cursor",
            limit=50,
        )


@pytest.mark.asyncio
async def test_retry_is_idempotently_recorded_before_background_work() -> None:
    row = cleanup_row(state="PENDING", version=4)
    instance, store, _, _ = operations(row)

    result = await instance.retry(
        row.task_id,
        actor="operator-1",
        request=request(),
    )

    assert result.cleanup.state == "PENDING"
    assert result.operation_replayed is False
    call = store.retry_blocked.await_args
    assert call.kwargs["action"] == "RETRY"
    assert call.kwargs["actor"] == "operator-1"
    assert len(call.kwargs["operation_hash"]) == 64


@pytest.mark.asyncio
async def test_finalize_runs_existing_proof_based_cleanup_once() -> None:
    row = cleanup_row(state="PENDING")
    instance, store, service, _ = operations(row)

    async def finish(_task_id):
        row.state = "DONE"
        row.executor_status = "NOT_REQUIRED"
        row.message = "failed safely"

    service.execute.side_effect = finish
    result = await instance.finalize(
        row.task_id,
        actor="operator-1",
        request=request(idempotency_key="finalize-1"),
    )

    assert result.cleanup.state == "DONE"
    service.execute.assert_awaited_once_with(row.task_id)
    assert store.retry_blocked.await_args.kwargs["action"] == "FINALIZE"


@pytest.mark.asyncio
async def test_finalize_never_claims_success_without_terminal_proof() -> None:
    row = cleanup_row(state="PENDING")
    instance, *_ = operations(row)

    with pytest.raises(FailureOperationConflict):
        await instance.finalize(
            row.task_id,
            actor="operator-1",
            request=request(idempotency_key="finalize-1"),
        )
