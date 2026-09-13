"""Real PostgreSQL checkpointer with deterministic, async model boundaries."""

import asyncio
from uuid import UUID, uuid4

import pytest
from langchain_core.language_models.chat_models import (
    BaseChatModel,
    agenerate_from_stream,
)
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.outputs import ChatGenerationChunk
from psycopg import OperationalError
from psycopg_pool import PoolClosed
from pydantic import Field, ValidationError

from agent_service.agents.assistant import build_assistant
from agent_service.application.outputs import OutputWriter
from agent_service.graphs.assistant.builder import build_graph
from agent_service.infrastructure.database.checkpoints import Checkpoints
from agent_service.runtime.graph_driver import GraphDriver
from agent_service.settings import Settings


class TestModel(BaseChatModel):
    __test__ = False
    seen: list[list[str]] = Field(default_factory=list)
    hold: bool = False
    fail: bool = False
    entered: asyncio.Event = Field(default_factory=asyncio.Event)
    release: asyncio.Event = Field(default_factory=asyncio.Event)

    @property
    def _llm_type(self):
        return "test-async-model"

    def _generate(self, *args, **kwargs):
        raise AssertionError("Sync model calls are forbidden")

    async def _agenerate(
        self, messages, stop=None, run_manager=None, **kwargs
    ):
        return await agenerate_from_stream(self._astream(messages, **kwargs))

    async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
        texts = [m.content for m in messages if isinstance(m, HumanMessage)]
        self.seen.append(texts)
        self.entered.set()
        yield ChatGenerationChunk(message=AIMessageChunk(content="reply: "))
        if self.hold:
            await self.release.wait()
        if self.fail:
            raise ValueError("Intentional model failure")
        for text in texts:
            yield ChatGenerationChunk(message=AIMessageChunk(content=text))
            await asyncio.sleep(0)


@pytest.fixture
def model(monkeypatch):
    model = TestModel()
    monkeypatch.setattr(
        "agent_service.runtime.management.build_model", lambda _: model
    )
    return model


@pytest.fixture
def settings(settings, model):
    return settings.model_copy(
        update={
            "agent_backend": "langgraph",
            "model_name": "test-model",
            "embedded_run_worker": False,
            "worker_poll_seconds": 0.05,
            "output_flush_chars": 1,
        }
    )


@pytest.fixture
def runtime(client):
    return client._transport.app.state.management_runtime.resources


@pytest.fixture
async def session(client, home, identity):
    response = await client.post(
        "/api/v1/sessions",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json={"project_id": home["default_project"]["project_id"]},
    )
    assert response.status_code == 201
    return response.json()


async def submit(client, identity, session, text="alpha"):
    response = await client.post(
        "/api/v1/agent/runs",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json={
            "user_id": identity["X-User-UUID"],
            "session_id": session["session_id"],
            "input": {
                "type": "message",
                "content": [{"type": "text", "text": text}],
            },
        },
    )
    assert response.status_code == 202, response.text
    return UUID(response.json()["run_id"])


async def drive(runtime, identity, run_id, *, model=None):
    driver = (
        runtime.driver
        if model is None
        else GraphDriver(
            runtime.runs, runtime.checkpoints, build_assistant(model)
        )
    )
    await driver.run_one(run_id, UUID(identity["X-User-UUID"]))
    return await runtime.runs.detail(UUID(identity["X-User-UUID"]), run_id)


@pytest.mark.postgres
async def test_history_survives_new_pool_and_session_isolation(
    client, identity, session, runtime, model, home
):
    first = await submit(client, identity, session, "remember-indigo")
    assert (await drive(runtime, identity, first)).status == "completed"
    # A different runtime connection pool must load existing checkpoints.
    await runtime.checkpoints.close()
    runtime.checkpoints = Checkpoints(runtime.settings)
    await runtime.checkpoints.open()
    fresh = TestModel()
    second = await submit(client, identity, session, "what-did-I-say")
    assert (await drive(runtime, identity, second, model=fresh)).status == (
        "completed"
    )
    assert fresh.seen == [["remember-indigo", "what-did-I-say"]]
    async with runtime.checkpoints.session(session["session_id"]) as saver:
        graph = build_graph(build_assistant(fresh), saver)
        snapshot = await graph.aget_state(
            {
                "configurable": {
                    "thread_id": session["session_id"],
                }
            }
        )
        assert len(snapshot.values["messages"]) == 4
        assert not snapshot.next
    other = (
        await client.post(
            "/api/v1/sessions",
            headers={
                **identity,
                "Idempotency-Key": str(uuid4()),
            },
            json={"project_id": home["default_project"]["project_id"]},
        )
    ).json()
    third = await submit(client, identity, other, "isolated")
    await drive(runtime, identity, third, model=fresh)
    assert fresh.seen[-1] == ["isolated"]


@pytest.mark.postgres
async def test_checkpoint_complete_projection_retry_without_model_call(
    client, identity, session, runtime, monkeypatch, model
):
    original = OutputWriter.replace
    fail_once = True

    async def replace(self, message_id, text, *, complete=False):
        nonlocal fail_once
        if complete and fail_once:
            fail_once = False
            raise OperationalError("Projection connection lost")
        await original(self, message_id, text, complete=complete)

    monkeypatch.setattr(OutputWriter, "replace", replace)
    run_id = await submit(client, identity, session)
    assert (await drive(runtime, identity, run_id)).status == "running"
    assert len(model.seen) == 1
    replacement = TestModel(fail=True)
    result = await drive(runtime, identity, run_id, model=replacement)
    assert result.status == "completed"
    assert replacement.seen == []
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert len(messages) == 2
    assert messages[0]["content"][0]["text"] == "reply: alpha"


@pytest.mark.postgres
async def test_cancel_stops_graph_before_releasing_session(
    client, identity, session, runtime, model
):
    model.hold = True
    run_id = await submit(client, identity, session)
    task = asyncio.create_task(drive(runtime, identity, run_id))
    await asyncio.wait_for(model.entered.wait(), 5)
    response = await client.post(
        f"/api/v1/agent/runs/{run_id}/cancel", headers=identity
    )
    assert response.status_code == 202
    assert (await asyncio.wait_for(task, 5)).status == "cancelled"
    saved = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()
    assert saved["active_run_id"] is None
    assert len(model.seen) == 1
    # A new input must not execute the cancelled pending graph work first.
    new_model = TestModel()
    second = await submit(client, identity, session, "new-turn")
    assert (
        await drive(runtime, identity, second, model=new_model)
    ).status == ("completed")
    assert len(new_model.seen) == 1


@pytest.mark.postgres
async def test_shutdown_recovery_and_concurrent_workers(
    client, identity, session, runtime, model
):
    model.hold = True
    run_id = await submit(client, identity, session)
    task = asyncio.create_task(drive(runtime, identity, run_id))
    await asyncio.wait_for(model.entered.wait(), 5)
    competing = TestModel()
    await drive(runtime, identity, run_id, model=competing)
    assert competing.seen == []
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (
        await runtime.runs.detail(UUID(identity["X-User-UUID"]), run_id)
    ).status == "running"
    recovered = TestModel()
    assert (
        await drive(runtime, identity, run_id, model=recovered)
    ).status == ("completed")
    assert recovered.seen == [["alpha"]]
    events = (
        await client.get(
            f"/api/v1/agent/runs/{run_id}/stream", headers=identity
        )
    ).text
    assert "message.updated" in events
    assert "message.delta" in events


@pytest.mark.postgres
async def test_model_error_has_partial_message_and_no_report(
    client, identity, session, runtime, model
):
    model.fail = True
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "failed"
    assert detail.executions == []
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert len(messages) == 2
    assert messages[0]["status"] == "failed"
    assert messages[0]["content"][0]["text"] == "reply: "


@pytest.mark.postgres
async def test_checkpoint_setup_is_idempotent_and_missing_schema_fails(
    runtime,
):
    await runtime.checkpoints.setup()
    await runtime.checkpoints.setup()
    missing = Checkpoints(
        runtime.settings.model_copy(
            update={
                "checkpoint_schema": "missing_" + uuid4().hex,
            }
        )
    )
    try:
        with pytest.raises(Exception, match="does not exist"):
            await missing.open()
    finally:
        await missing.close()


def test_graph_settings_require_model_and_pool_headroom(settings):
    for changes in [
        {"model_name": None},
        {"checkpoint_pool_max_size": 2},
        {"checkpoint_schema": "public;drop"},
    ]:
        with pytest.raises(ValidationError):
            Settings.model_validate({**settings.model_dump(), **changes})


@pytest.mark.postgres
async def test_closed_idle_connection_is_replaced_without_losing_memory(
    client, identity, session, runtime, db
):
    first = await submit(client, identity, session, "saved-fact")
    assert (await drive(runtime, identity, first)).status == "completed"
    async with runtime.checkpoints.connection() as connection:
        row = await (
            await connection.execute("SELECT pg_backend_pid() AS pid")
        ).fetchone()
        pid = row["pid"]
    # This PID belongs only to this test's pool, never an external service.
    await db.execute("SELECT pg_terminate_backend(%s)", (pid,))
    second = await submit(client, identity, session, "recall")
    fresh = TestModel()
    assert (await drive(runtime, identity, second, model=fresh)).status == (
        "completed"
    )
    assert fresh.seen == [["saved-fact", "recall"]]


@pytest.mark.postgres
async def test_readiness_checks_checkpoint_connection(runtime):
    await runtime.ready()
    await runtime.checkpoints.close()
    with pytest.raises(PoolClosed):
        await runtime.ready()
