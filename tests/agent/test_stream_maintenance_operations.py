from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from ex_agent.config import Settings
from ex_agent.maintenance.contracts import StreamMaintenanceRequest
from ex_agent.maintenance.operations import (
    StreamMaintenanceForbidden,
    StreamMaintenanceOperations,
)


def job(**changes):
    now = datetime.now(UTC)
    values = {
        "id": uuid4(),
        "stream_alias": "executor_events",
        "stream_key": "executor.events",
        "action": "TRIM",
        "state": "PENDING",
        "reason": "weekly retention",
        "retention_seconds": 604800,
        "minimum_retained_entries": 1000,
        "attempts": 0,
        "next_attempt_at": now,
        "result": None,
        "last_error": None,
        "created_at": now - timedelta(seconds=1),
        "updated_at": now,
        "created_by": "operator-1",
        "updated_by": "operator-1",
    }
    values.update(changes)
    return SimpleNamespace(**values)


def request(**changes) -> StreamMaintenanceRequest:
    values = {
        "stream": "executor_events",
        "idempotency_key": "maintenance-1",
        "reason": "weekly retention",
    }
    values.update(changes)
    return StreamMaintenanceRequest(**values)


def operations(row=None, *, allowed="operator-1"):
    current = row or job()
    store = SimpleNamespace(
        create=AsyncMock(return_value=(current, False)),
        get=AsyncMock(return_value=current),
        page=AsyncMock(return_value=[current]),
    )
    recovery = SimpleNamespace(execute=AsyncMock())
    settings = Settings(
        stream_maintenance_operator_user_ids=allowed,
    )
    instance = StreamMaintenanceOperations(
        settings,
        cast(Any, store),
        cast(Any, recovery),
    )
    return instance, store, recovery, current


async def test_stream_maintenance_is_default_deny() -> None:
    instance, *_ = operations(allowed="")

    with pytest.raises(StreamMaintenanceForbidden):
        await instance.submit_trim(actor="operator-1", request=request())


async def test_only_registered_stream_aliases_are_accepted() -> None:
    instance, *_ = operations()

    with pytest.raises(ValueError, match="Unknown registered Stream"):
        await instance.submit_trim(
            actor="operator-1",
            request=request(stream="arbitrary.redis.key"),
        )


async def test_request_cannot_weaken_server_retention_policy() -> None:
    instance, *_ = operations()

    with pytest.raises(ValueError, match="server policy"):
        await instance.submit_trim(
            actor="operator-1",
            request=request(retention_seconds=60),
        )


async def test_trim_is_durably_enqueued_without_api_side_execution() -> None:
    instance, store, recovery, row = operations()

    result = await instance.submit_trim(
        actor="operator-1",
        request=request(),
    )

    assert result.job.job_id == row.id
    assert result.job.state == "PENDING"
    recovery.execute.assert_not_awaited()
    call = store.create.await_args.kwargs
    assert call["stream_key"] == "executor.events"
    assert call["action"] == "TRIM"
    assert len(call["request_hash"]) == 64


async def test_plan_executes_safe_read_and_returns_audited_result() -> None:
    row = job(action="PLAN")
    instance, store, recovery, _ = operations(row)

    async def complete(_job_id, *, actor):
        assert actor == "operator-1"
        row.state = "SUCCEEDED"
        row.attempts = 1
        row.result = {"trim_before_id": "1000-0"}

    recovery.execute.side_effect = complete
    result = await instance.plan(actor="operator-1", request=request())

    recovery.execute.assert_awaited_once_with(row.id, actor="operator-1")
    assert store.get.await_count == 1
    assert result.job.result == {"trim_before_id": "1000-0"}


async def test_job_list_uses_opaque_cursor() -> None:
    first, second = job(), job()
    instance, store, *_ = operations(first)
    store.page.return_value = [first, second]

    page = await instance.jobs(
        actor="operator-1",
        cursor=None,
        limit=1,
    )
    store.page.return_value = []
    await instance.jobs(
        actor="operator-1",
        cursor=page.next_cursor,
        limit=1,
    )

    assert page.has_more
    assert store.page.await_args_list[1].kwargs["before"] == (
        first.created_at,
        first.id,
    )
