import json
from uuid import uuid4

import httpx
import pytest

from examples.failure_cleanup import cancel_and_confirm
from executor_worker import DeferEvent, EventContext, ExecutorEvent


def context():
    event = ExecutorEvent(
        event_id=uuid4(),
        execution_id=uuid4(),
        event_type="execution.completed",
        schema_version="1.0",
        event_sequence=1,
        occurred_at="now",
        payload={},
    )
    return EventContext(
        "agent", "session", "task", event.execution_id, uuid4(), event
    )


@pytest.mark.parametrize("terminal", ["FAILED", "SUCCEEDED", "CANCELLED"])
async def test_already_terminal_does_not_cancel(terminal):
    def request(req):
        assert req.method == "GET"
        return httpx.Response(
            200,
            json={
                "execution": {"state": {"status": terminal}},
            },
        )

    async with httpx.AsyncClient(
        base_url="http://executor", transport=httpx.MockTransport(request)
    ) as http:
        result = await cancel_and_confirm(http, context(), reason="failed")
        assert result == terminal


async def test_cancel_receipt_is_not_terminal_confirmation():
    requests = []
    statuses = iter(["RUNNING", "CANCELLING", "CANCELLED"])
    ctx = context()

    def request(req):
        requests.append(req)
        if req.method == "POST":
            body = json.loads(req.content)
            assert body["idempotency_key"].endswith(str(ctx.execution_id))
            return httpx.Response(202, json={"accepted": True})
        return httpx.Response(
            200,
            json={
                "execution": {"state": {"status": next(statuses)}},
            },
        )

    async with httpx.AsyncClient(
        base_url="http://executor", transport=httpx.MockTransport(request)
    ) as http:
        result = await cancel_and_confirm(
            http, ctx, reason="failed", poll_seconds=0.001
        )
        assert result == "CANCELLED"
    assert [r.method for r in requests] == ["GET", "POST", "GET", "GET"]


async def test_unconfirmed_cancel_remains_deferred():
    def request(req):
        return httpx.Response(
            200,
            json={
                "execution": {"state": {"status": "RUNNING"}},
            },
        )

    async with httpx.AsyncClient(
        base_url="http://executor", transport=httpx.MockTransport(request)
    ) as http:
        with pytest.raises(DeferEvent):
            await cancel_and_confirm(
                http,
                context(),
                reason="failed",
                timeout_seconds=0.01,
                poll_seconds=0.001,
            )
