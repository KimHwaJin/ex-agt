from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import httpx
import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver

from ex_agent.dev_chat.client import AgentApiClient, needs_input
from ex_agent.dev_chat.graph import build_chat_graph
from ex_agent.dev_chat.settings import ChatSettings


@pytest.mark.parametrize(
    "values",
    [
        {"user_id": "user:name"},
        {"project_id": "project/path"},
        {"project_id": "unscoped"},
    ],
)
def test_settings_reject_unsafe_executor_path_segments(
    values: dict[str, Any],
) -> None:
    with pytest.raises(ValueError):
        ChatSettings(_env_file=None, **values)


def task_payload(**overrides: Any) -> dict[str, Any]:
    return {
        "task_id": str(uuid4()),
        "user_id": "tester",
        "project_id": "project",
        "session_id": "session",
        "status": "EXECUTING",
        "version": 5,
        "execution_id": str(uuid4()),
        "current_interrupt": {"kind": "EXECUTOR_EVENT"},
        "terminal_message": None,
        "created_at": "2026-08-31T00:00:00Z",
        "updated_at": "2026-08-31T00:00:00Z",
        "created_by": "tester",
        "updated_by": "tester",
        **overrides,
    }


def settings() -> ChatSettings:
    return ChatSettings(_env_file=None, user_id="tester", watch_seconds=0.05)


@pytest.mark.asyncio
async def test_creation_retries_with_same_ids_and_trusted_identity() -> None:
    bodies: list[dict[str, Any]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["X-User-ID"] == "tester"
        assert request.url.path.endswith("/sessions/session/tasks")
        bodies.append(json.loads(request.content))
        if len(bodies) == 1:
            raise httpx.ReadError("lost response", request=request)
        return httpx.Response(202, json={})

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    body = {"task_id": str(uuid4()), "idempotency_key": "stable"}
    await api.create_task("session", body)
    assert bodies == [body, body]


@pytest.mark.asyncio
async def test_resume_retry_reconciles_stale_interrupt() -> None:
    payload = task_payload(version=8)
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.method == "POST":
            return httpx.Response(409, json={"detail": "not awaiting input"})
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    await api.send_signal(
        payload["task_id"],
        {"type": "EXECUTION_MODE", "mode": "SINGLE"},
        "stable",
        4,
    )
    assert calls == ["POST", "GET"]


@pytest.mark.asyncio
async def test_same_version_conflict_is_not_hidden() -> None:
    payload = task_payload(version=4)

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409 if request.method == "POST" else 200, json=payload
        )

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.HTTPStatusError):
        await api.send_signal(
            payload["task_id"],
            {"type": "EXECUTION_MODE", "mode": "SINGLE"},
            "stable",
            4,
        )


class EventStream(httpx.AsyncByteStream):
    def __init__(self, payload: dict[str, Any]) -> None:
        self.payload = payload
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield b"id: 10\nevent: executor.progress\ndata: {}\n\n"
        self.payload.update(
            status="SUCCEEDED",
            version=9,
            current_interrupt=None,
            terminal_message="완료",
        )
        yield b"id: 11\nevent: task.completed\ndata: {}\n\n"

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_sse_replays_cursor_without_progress_polling() -> None:
    payload = task_payload()
    stream = EventStream(payload)
    snapshots = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal snapshots
        if request.url.path.endswith("/events"):
            assert request.headers["Last-Event-ID"] == "9"
            return httpx.Response(200, stream=stream)
        snapshots += 1
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    result, cursor = await api.watch(
        payload["task_id"], after_event_id=9, ignored_version=-1
    )
    assert result.status == "SUCCEEDED"
    assert cursor == 11
    assert snapshots == 2  # initial + terminal, never executor.progress
    assert stream.closed


@pytest.mark.asyncio
async def test_initial_changed_snapshot_is_displayed_without_waiting() -> None:
    payload = task_payload()

    def handler(request: httpx.Request) -> httpx.Response:
        assert not request.url.path.endswith("/events")
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    result, cursor = await api.watch(
        payload["task_id"],
        after_event_id=12,
        ignored_version=-1,
        known_version=-1,
    )
    assert result.status == "EXECUTING"
    assert cursor == 12


@pytest.mark.asyncio
async def test_status_event_updates_progress_before_terminal_state() -> None:
    payload = task_payload()
    calls = []

    class StatusStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            payload.update(status="GENERATING_REPORT", version=6)
            yield b"id: 15\nevent: task.status_changed\ndata: {}\n\n"

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.path)
        if request.url.path.endswith("/events"):
            return httpx.Response(200, stream=StatusStream())
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    result, cursor = await api.watch(
        payload["task_id"],
        after_event_id=14,
        ignored_version=-1,
        known_version=5,
    )
    assert result.status == "GENERATING_REPORT"
    assert cursor == 15
    assert len(calls) == 3  # initial GET, SSE, changed snapshot GET


@pytest.mark.asyncio
async def test_reconnect_replays_cursor_after_transport_disconnect() -> None:
    payload = task_payload()
    cursors = []

    class DisconnectStream(httpx.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b"id: 10\nevent: executor.progress\ndata: {}\n\n"
            raise httpx.ReadError("connection lost")

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/events"):
            cursors.append(request.headers["Last-Event-ID"])
            stream = (
                DisconnectStream()
                if len(cursors) == 1
                else EventStream(payload)
            )
            return httpx.Response(200, stream=stream)
        return httpx.Response(200, json=payload)

    api = AgentApiClient(
        ChatSettings(_env_file=None, watch_seconds=2),
        transport=httpx.MockTransport(handler),
    )
    result, cursor = await api.watch(
        payload["task_id"], after_event_id=9, ignored_version=-1
    )
    assert cursors == ["9", "10"]
    assert cursor == 11
    assert result.status == "SUCCEEDED"


class IdleStream(httpx.AsyncByteStream):
    def __init__(self) -> None:
        self.closed = False

    async def __aiter__(self) -> AsyncIterator[bytes]:
        await asyncio.Event().wait()
        yield b""

    async def aclose(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_graph_automatically_reconnects_idle_sse_until_completion() -> (
    None
):
    payload = task_payload()
    streams: list[IdleStream] = []
    writes = []

    class CompletingStream(IdleStream):
        async def aclose(self) -> None:
            await super().aclose()
            if len(streams) == 3:
                payload.update(
                    status="SUCCEEDED",
                    version=10,
                    current_interrupt=None,
                    terminal_message="결과 리포트",
                )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST":
            writes.append(request.url.path)
            payload["task_id"] = json.loads(request.content)["task_id"]
            return httpx.Response(202, json={})
        if request.url.path.endswith("/events"):
            assert request.headers["Last-Event-ID"] == "0"
            stream = CompletingStream()
            streams.append(stream)
            return httpx.Response(200, stream=stream)
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    result = await asyncio.wait_for(
        graph.ainvoke(
            {"messages": [HumanMessage(content="EDA")]},
            {"configurable": {"thread_id": str(uuid4())}},
        ),
        timeout=3,
    )
    assert "__interrupt__" not in result
    assert result["snapshot"]["status"] == "SUCCEEDED"
    assert "결과 리포트" in result["messages"][-1].content
    assert len(result["messages"]) == 2
    assert len(writes) == 1 and writes[0].endswith("/tasks")
    assert len(streams) == 3 and all(stream.closed for stream in streams)


@pytest.mark.asyncio
async def test_watch_timeout_and_ui_stop_never_cancel_backend() -> None:
    payload = task_payload()
    stream = IdleStream()
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        if request.url.path.endswith("/events"):
            return httpx.Response(200, stream=stream)
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    result, _ = await api.watch(
        payload["task_id"], after_event_id=0, ignored_version=-1
    )
    assert result.status == "EXECUTING"
    assert stream.closed
    assert set(calls) == {"GET"}

    stream = IdleStream()
    watching = asyncio.create_task(
        api.watch(payload["task_id"], after_event_id=0, ignored_version=-1)
    )
    await asyncio.sleep(0.01)
    watching.cancel()
    with pytest.raises(asyncio.CancelledError):
        await watching
    assert stream.closed
    assert set(calls) == {"GET"}


@pytest.mark.asyncio
async def test_old_interrupt_is_not_reshown_after_accepted_resume() -> None:
    payload = task_payload(
        status="PLANNING", current_interrupt={"kind": "EXECUTION_MODE"}
    )
    stream = IdleStream()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/events"):
            return httpx.Response(200, stream=stream)
        return httpx.Response(200, json=payload)

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    result, _ = await api.watch(
        payload["task_id"], after_event_id=0, ignored_version=5
    )
    assert stream.closed  # actually waited instead of resurfacing old card
    assert not needs_input(result, 5)
    assert needs_input(result, 4)


@pytest.mark.asyncio
async def test_cancel_uses_dedicated_endpoint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/cancel")
        assert json.loads(request.content) == {
            "idempotency_key": "cancel-key",
            "reason": "stop",
        }
        return httpx.Response(202, json={})

    api = AgentApiClient(settings(), transport=httpx.MockTransport(handler))
    await api.send_signal(
        str(uuid4()),
        {"type": "CANCEL_REQUESTED", "reason": "stop"},
        "cancel-key",
        5,
    )
