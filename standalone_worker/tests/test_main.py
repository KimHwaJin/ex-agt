from __future__ import annotations

import asyncio
import signal
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

import agent_app
import main as entrypoint
from executor_worker import EventContext, ExecutorEvent


def make_context():
    event = ExecutorEvent(
        event_id=uuid4(),
        execution_id=uuid4(),
        event_type="execution.completed",
        event_sequence=1,
        schema_version="1.0",
        occurred_at="2026-08-31T00:00:00Z",
        payload={},
    )
    return EventContext(
        "test-main", str(uuid4()), "task", event.execution_id, uuid4(), event
    )


@pytest.fixture
async def runtime(monkeypatch):
    events = []
    ctx = make_context()
    bindings, saver = object(), object()
    graph = SimpleNamespace(
        checkpointer=saver, aget_state=AsyncMock(), ainvoke=AsyncMock()
    )
    state = SimpleNamespace(
        events=events,
        ctx=ctx,
        bindings=bindings,
        saver=saver,
        graph=graph,
        fail_run=False,
        fail_signal=False,
    )

    class Worker:
        def __init__(self, settings, handlers):
            self.bindings = bindings
            self.handlers = handlers

        async def __aenter__(self):
            events.append("worker.open")
            return self

        async def __aexit__(self, *args):
            events.append("worker.close")

        def request_stop(self):
            events.append("worker.stop")

        async def run(self):
            events.append("worker.run")
            if state.fail_run:
                raise RuntimeError("run failed")
            await self.handlers[ctx.event.event_type](ctx)
            state.stop_callback()

    @asynccontextmanager
    async def connection(url):
        assert url == "test-url"
        events.append("saver.open")
        try:
            yield saver
        finally:
            events.append("saver.close")

    async def create_graph(checkpointer, store):
        assert checkpointer is saver
        assert store is bindings
        events.append("graph.create")
        return state.graph

    class Adapter:
        def __init__(self, compiled):
            assert compiled is graph
            events.append("adapter.create")

        async def __call__(self, context):
            assert context is ctx
            events.append("adapter.handle")

    def add_signal(signum, callback):
        if state.fail_signal and signum == signal.SIGINT:
            raise RuntimeError("signal failed")
        state.stop_callback = callback
        events.append(f"signal.add.{signum.name}")

    def remove_signal(signum):
        events.append(f"signal.remove.{signum.name}")

    monkeypatch.setattr(entrypoint, "ExecutorWorker", Worker)
    monkeypatch.setattr(
        entrypoint,
        "Settings",
        lambda: SimpleNamespace(database_url="test-url"),
    )
    monkeypatch.setattr(
        entrypoint,
        "AsyncPostgresSaver",
        SimpleNamespace(from_conn_string=connection),
    )
    monkeypatch.setattr(agent_app, "create_graph", create_graph)
    monkeypatch.setattr(entrypoint, "SessionGraphAdapter", Adapter)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", add_signal)
    monkeypatch.setattr(loop, "remove_signal_handler", remove_signal)
    return state


async def test_main_injects_live_resources_and_drains_before_close(runtime):
    await entrypoint.main()
    assert runtime.events == [
        "worker.open",
        "saver.open",
        "graph.create",
        "adapter.create",
        "signal.add.SIGTERM",
        "signal.add.SIGINT",
        "worker.run",
        "adapter.handle",
        "worker.stop",
        "signal.remove.SIGTERM",
        "signal.remove.SIGINT",
        "saver.close",
        "worker.close",
    ]


async def test_factory_failure_never_consumes_and_closes(runtime, monkeypatch):
    monkeypatch.setattr(
        agent_app, "create_graph", AsyncMock(side_effect=RuntimeError("build"))
    )
    with pytest.raises(RuntimeError, match="build"):
        await entrypoint.main()
    assert runtime.events == [
        "worker.open",
        "saver.open",
        "saver.close",
        "worker.close",
    ]


@pytest.mark.parametrize(
    "graph",
    [
        None,
        object(),
        SimpleNamespace(aget_state=AsyncMock(), ainvoke=AsyncMock()),
    ],
)
async def test_invalid_graph_never_consumes(runtime, graph):
    runtime.graph = graph
    with pytest.raises(ValueError, match="compiled graph"):
        await entrypoint.main()
    assert "worker.run" not in runtime.events
    assert runtime.events[-2:] == ["saver.close", "worker.close"]


@pytest.mark.parametrize("handlers", [{}, {" ": AsyncMock()}, {"event": None}])
async def test_invalid_registry_fails_before_open(
    runtime, monkeypatch, handlers
):
    monkeypatch.setattr(agent_app, "build_handlers", lambda _: handlers)
    with pytest.raises(ValueError, match="register handlers"):
        await entrypoint.main()
    assert not runtime.events


async def test_run_failure_cleans_signals_and_resources(runtime):
    runtime.fail_run = True
    with pytest.raises(RuntimeError, match="run failed"):
        await entrypoint.main()
    assert runtime.events[-4:] == [
        "signal.remove.SIGTERM",
        "signal.remove.SIGINT",
        "saver.close",
        "worker.close",
    ]


async def test_partial_signal_installation_is_cleaned(runtime):
    runtime.fail_signal = True
    with pytest.raises(RuntimeError, match="signal failed"):
        await entrypoint.main()
    assert "worker.run" not in runtime.events
    assert runtime.events[-3:] == [
        "signal.remove.SIGTERM",
        "saver.close",
        "worker.close",
    ]


async def test_unconfigured_factory_does_not_load_demo():
    with pytest.raises(NotImplementedError, match="Connect your Agent"):
        await agent_app.create_graph(AsyncMock(), AsyncMock())


async def test_progress_requires_explicit_implementation_and_registration():
    resume = AsyncMock()
    assert agent_app.build_handlers(resume) == {
        "execution.operation_completed": resume,
        "execution.completed": resume,
    }
    with pytest.raises(NotImplementedError, match="on_step_completed"):
        await agent_app.on_step_completed(make_context())


@pytest.mark.postgres
@pytest.mark.redis
async def test_main_real_stream_to_session_checkpoint(worker, monkeypatch):
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from examples.api_integration import attach_execution
    from examples.session_graph import build_graph

    ctx = make_context()
    ready = asyncio.Event()
    captured = {}
    original = entrypoint.ExecutorWorker

    def create_worker(settings, handlers):
        instance = original(settings, handlers)
        captured["worker"] = instance
        return instance

    async def create_graph(saver, bindings):
        assert bindings is captured["worker"].bindings
        graph = build_graph(saver)
        captured["graph"] = graph
        ready.set()
        return graph

    # Deployment responsibility; main must never do this itself.
    async with AsyncPostgresSaver.from_conn_string(
        worker.settings.database_url
    ) as saver:
        await saver.setup()

    monkeypatch.setattr(entrypoint, "Settings", lambda: worker.settings)
    monkeypatch.setattr(entrypoint, "ExecutorWorker", create_worker)
    monkeypatch.setattr(agent_app, "create_graph", create_graph)
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *args: None)
    monkeypatch.setattr(loop, "remove_signal_handler", lambda *args: None)
    task = asyncio.create_task(entrypoint.main())
    try:
        await asyncio.wait_for(ready.wait(), 10)
        # API has a separate saver; worker resumes the persisted same thread.
        async with AsyncPostgresSaver.from_conn_string(
            worker.settings.database_url
        ) as api_saver:
            await attach_execution(
                worker,
                build_graph(api_saver),
                execution_id=ctx.execution_id,
                session_id=ctx.session_id,
                task_id=ctx.task_id,
            )
        fields = {
            key: str(value)
            for key, value in ctx.event.model_dump(mode="json").items()
            if key != "payload"
        }
        fields["payload"] = "{}"
        # Two source entries with the SAME event must have one application.
        for _ in range(2):
            await worker.redis.xadd(
                worker.settings.executor_event_stream, fields
            )
        async with asyncio.timeout(15):
            while (await worker.store.counts()).get("command:DONE", 0) != 1:
                if task.done():
                    await task
                    raise AssertionError("Worker stopped before dispatch")
                await asyncio.sleep(0.02)
        values = (await captured["graph"].aget_state(ctx.graph_config)).values
        assert len(values["ew_receipts"]) == 1
        assert len(values["results"]) == 1
        assert values["active_task_id"] == ctx.task_id
    finally:
        if "worker" in captured:
            captured["worker"].request_stop()
        await asyncio.wait_for(task, 10)

    assert not captured["worker"]._running
    assert captured["worker"].pool.closed
