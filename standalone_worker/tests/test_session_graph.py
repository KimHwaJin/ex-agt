from typing import Any, cast
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.graph import END, START, StateGraph
from langgraph.types import interrupt

from examples.session_graph import State, build_graph
from executor_worker import (
    DeferEvent,
    EventContext,
    ExecutorEvent,
    IgnoreEvent,
)
from executor_worker.langgraph_adapter import SessionGraphAdapter


def context(
    *,
    session="s",
    task="t",
    execution=None,
    sequence=1,
    kind="execution.completed",
):
    e = ExecutorEvent(
        event_id=uuid4(),
        execution_id=execution or uuid4(),
        event_type=kind,
        schema_version="1.0",
        event_sequence=sequence,
        occurred_at="now",
        payload={},
    )
    return EventContext("ns", session, task, e.execution_id, uuid4(), e)


async def start(graph, ctx):
    await graph.ainvoke(
        {
            "active_task_id": ctx.task_id,
            "execution_id": str(ctx.execution_id),
            "ew_pending": {},
            "finished": False,
            "results": [],
        },
        ctx.graph_config,
        durability="sync",
    )


async def test_event_before_checkpoint_defers():
    adapter = SessionGraphAdapter(build_graph(InMemorySaver()))
    with pytest.raises(DeferEvent):
        await adapter(context())


async def test_new_task_event_waits_while_previous_checkpoint_is_visible():
    graph = build_graph(InMemorySaver())
    first, second = context(), context(task="second")
    await start(graph, first)
    await SessionGraphAdapter(graph)(first)
    with pytest.raises(DeferEvent, match="binding"):
        await SessionGraphAdapter(graph)(second)


async def test_receipt_before_tail_failure_continues_only_its_own_action():
    from examples.session_graph import wait

    calls = []

    def record(state):
        action = state["ew_pending"]
        return {
            "ew_receipts": {
                action["command_id"]: action["event"]["event_id"],
            }
        }

    def tail(state):
        calls.append(True)
        if len(calls) == 1:
            raise RuntimeError("tail failed after receipt")
        return {"finished": True}

    graph = (
        StateGraph(cast(Any, State))
        .add_node("wait", wait)
        .add_node("record", record)
        .add_node("tail", tail)
        .add_edge(START, "wait")
        .add_edge("wait", "record")
        .add_edge("record", "tail")
        .add_edge("tail", END)
        .compile(checkpointer=InMemorySaver())
    )
    ctx = context()
    await start(graph, ctx)
    adapter = SessionGraphAdapter(graph)
    with pytest.raises(RuntimeError):
        await adapter(ctx)
    await adapter(ctx)
    await adapter(ctx)
    assert len(calls) == 2
    assert not (await graph.aget_state(ctx.graph_config)).next


async def test_same_session_next_task_ignores_old_event_and_keeps_receipts():
    graph = build_graph(InMemorySaver())
    adapter = SessionGraphAdapter(graph)
    first = context(sequence=50)
    await start(graph, first)
    await adapter(first)
    second = context(task="next-task", sequence=1)
    await start(graph, second)
    await adapter(first)  # prior receipt; no effect on the next wait
    with pytest.raises(IgnoreEvent):
        await adapter(context(execution=first.execution_id, sequence=51))
    assert (await graph.aget_state(second.graph_config)).next == ("wait",)
    await adapter(second)
    values = (await graph.aget_state(second.graph_config)).values
    assert len(values["ew_receipts"]) == 2
    assert values["ew_sequences"][str(second.execution_id)] == 1
    assert len(values["results"]) == 1


async def test_failure_after_wait_resumes_node_not_new_interrupt():
    calls = []

    async def effect(action):
        calls.append(action["command_id"])
        if len(calls) == 1:
            raise RuntimeError("External effect failed")

    graph = build_graph(InMemorySaver(), effect)
    adapter = SessionGraphAdapter(graph)
    ctx = context()
    await start(graph, ctx)
    with pytest.raises(RuntimeError):
        await adapter(ctx)
    snapshot = await graph.aget_state(ctx.graph_config)
    assert snapshot.next == ("apply",)
    await adapter(ctx)
    await adapter(ctx)
    assert calls == [str(ctx.command_id), str(ctx.command_id)]
    assert (
        len((await graph.aget_state(ctx.graph_config)).values["results"]) == 1
    )


async def test_late_boundary_does_not_reopen_ended_graph():
    graph = build_graph(InMemorySaver())
    adapter = SessionGraphAdapter(graph)
    ctx = context()
    await start(graph, ctx)
    await adapter(ctx)
    with pytest.raises(IgnoreEvent):
        await adapter(context(execution=ctx.execution_id, sequence=2))
    assert not (await graph.aget_state(ctx.graph_config)).next


async def test_executor_event_cannot_answer_human_review():
    def review(state):
        interrupt({"kind": "USER_REVIEW"})
        return {}

    graph = (
        StateGraph(cast(Any, State))
        .add_node("review", review)
        .add_edge(START, "review")
        .add_edge("review", END)
        .compile(checkpointer=InMemorySaver())
    )
    ctx = context()
    await start(graph, ctx)
    with pytest.raises(DeferEvent, match="user input"):
        await SessionGraphAdapter(graph)(ctx)
    assert (await graph.aget_state(ctx.graph_config)).next == ("review",)


@pytest.mark.postgres
async def test_postgres_graph_checkpoint_recovers_missing_worker_done(
    worker,
    monkeypatch,
):
    from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

    from examples.api_integration import attach_execution
    from executor_worker.consumer import AckDecision, StreamMessage
    from executor_worker.dispatcher import Dispatcher

    ctx = context(session=str(uuid4()))
    url = worker.settings.database_url
    async with AsyncPostgresSaver.from_conn_string(url) as api_saver:
        await api_saver.setup()
        graph = build_graph(api_saver)
        await attach_execution(
            worker,
            graph,
            execution_id=ctx.execution_id,
            session_id=ctx.session_id,
            task_id=ctx.task_id,
        )
    await worker.store.ingest(ctx.event)
    await worker.store.advance(ctx.execution_id, {ctx.event.event_type}, 100)
    await worker.outbox.once()
    entry = (await worker.redis.xrange(worker.settings.command_stream))[0]
    message = StreamMessage(*entry)
    original = worker.store.set_state

    async def fail_done(command_id, state, **kwargs):
        if state == "DONE":
            raise RuntimeError("Crash after checkpoint before DB DONE")
        await original(command_id, state, **kwargs)

    async with AsyncPostgresSaver.from_conn_string(url) as saver:
        adapter = SessionGraphAdapter(build_graph(saver))
        dispatcher = Dispatcher(
            worker.store, worker.guard, {ctx.event.event_type: adapter}
        )
        with monkeypatch.context() as patch:
            patch.setattr(worker.store, "set_state", fail_done)
            with pytest.raises(RuntimeError):
                await dispatcher.handle(message)

    # Separate checkpoint connection and graph, same persisted thread.
    async with AsyncPostgresSaver.from_conn_string(url) as restarted:
        graph = build_graph(restarted)
        dispatcher = Dispatcher(
            worker.store,
            worker.guard,
            {ctx.event.event_type: SessionGraphAdapter(graph)},
        )
        assert (await dispatcher.handle(message)).decision == AckDecision.ACK
        values = (await graph.aget_state(ctx.graph_config)).values
        assert len(values["results"]) == 1
        assert len(values["ew_receipts"]) == 1
