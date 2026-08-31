import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from agent.failure.recovery import FailureRecovery
from ex_agent.executor.client import ExecutorClient


@pytest.mark.parametrize(
    "page",
    [
        {"items": [], "has_more": True},
        {"items": [], "has_more": "false"},
        {
            "items": [{"execution_id": str(uuid4()), "context": {}}],
            "has_more": False,
        },
    ],
)
async def test_execution_lookup_refuses_ambiguous_or_wrong_identity(page):
    async with httpx.AsyncClient(
        base_url="http://executor.test",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, json=page)
        ),
    ) as http:
        client = ExecutorClient(
            "http://executor.test", timeout_seconds=1, client=http
        )
        with pytest.raises(ValueError):
            await client.find_task_execution(
                user_id="u", project_id="p", session_id="s", task_id="t"
            )


async def test_cleanup_recovery_is_bounded_and_wraps_failed_scan_cursor():
    service = AsyncMock()
    service.requests.blocked_page.side_effect = [[uuid4(), uuid4()], []]
    service.store.due.return_value = list(range(7))
    current = peak = 0

    async def execute(task_id):
        nonlocal current, peak
        current += 1
        peak = max(peak, current)
        try:
            await asyncio.sleep(0.001)
        finally:
            current -= 1

    service.execute.side_effect = execute
    recovery = FailureRecovery(service, batch_size=2, concurrency=2)
    await recovery.once()
    assert recovery.api_cursor is not None
    await recovery.once()
    assert recovery.api_cursor is None and peak == 2
