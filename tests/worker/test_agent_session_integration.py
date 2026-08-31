"""Real PG/Redis delivery and checkpoints; business services are doubles."""

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.graph import build_session_graph, checkpoint_serializer
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from agent.session import SessionCoordinator
from tests.agent.support import event_context, review, services, turn
from worker import DeferEvent
from worker.consumer import AckDecision, StreamMessage
from worker.dispatcher import Dispatcher


@pytest.mark.postgres
@pytest.mark.redis
async def test_session_graph_recovers_after_checkpoint_before_worker_done(
    worker, monkeypatch
):
    service = services()
    task = turn()
    url = worker.settings.database_url
    async with AsyncPostgresSaver.from_conn_string(
        url, serde=checkpoint_serializer()
    ) as saver:
        await saver.setup()
        graph = build_session_graph(
            service, worker.bindings, checkpointer=saver
        )
        host = SessionCoordinator(graph, worker.guard)
        waiting = await review(host, task, await host.start(task))
        ctx = event_context(task, waiting.values["execution_id"])
    await worker.store.ingest(ctx.event)
    await worker.store.advance(ctx.execution_id, {ctx.event.event_type}, 100)
    await worker.outbox.once()
    entry = (await worker.redis.xrange(worker.settings.command_stream))[0]
    message = StreamMessage(*entry)
    original = worker.store.set_state

    async def fail_done(command_id, state, **kwargs):
        if state == "DONE":
            raise RuntimeError("Crash before Worker DONE")
        await original(command_id, state, **kwargs)

    async with AsyncPostgresSaver.from_conn_string(
        url, serde=checkpoint_serializer()
    ) as saver:
        graph = build_session_graph(
            service, worker.bindings, checkpointer=saver
        )
        adapter = SessionGraphAdapter(graph)
        dispatcher = Dispatcher(
            worker.store, worker.guard, {ctx.event.event_type: adapter}
        )
        with monkeypatch.context() as patch:
            patch.setattr(worker.store, "set_state", fail_done)
            with pytest.raises(RuntimeError, match="before Worker DONE"):
                await dispatcher.handle(message)
        service.generate_and_materialize_report.assert_awaited_once()

    # API + Worker share only the database, not graph/checkpointer instances.
    async with AsyncPostgresSaver.from_conn_string(
        url, serde=checkpoint_serializer()
    ) as saver:
        graph = build_session_graph(
            service, worker.bindings, checkpointer=saver
        )
        dispatcher = Dispatcher(
            worker.store,
            worker.guard,
            {ctx.event.event_type: SessionGraphAdapter(graph)},
        )
        assert (await dispatcher.handle(message)).decision == AckDecision.ACK
        assert (await dispatcher.handle(message)).decision == AckDecision.ACK
        snapshot = await graph.aget_state(ctx.graph_config)
        assert not snapshot.next
        assert snapshot.values["workflow"]["phase"] == "SUCCEEDED"
        assert len(snapshot.values["messages"]) == 2
        assert len(snapshot.values["ew_receipts"]) == 1
        service.generate_and_materialize_report.assert_awaited_once()
        service.commit_terminal.assert_awaited_once()
        host = SessionCoordinator(graph, worker.guard)
        next_task = turn(session=task.session_id)
        next_snapshot = await host.start(next_task)
        assert (
            next_snapshot.values["active_task_id"] == next_task.active_task_id
        )
        assert "report_markdown" not in next_snapshot.values["workflow"]
        assert (await dispatcher.handle(message)).decision == AckDecision.ACK


@pytest.mark.postgres
@pytest.mark.redis
async def test_api_and_worker_use_same_session_guard(worker):
    async with AsyncPostgresSaver.from_conn_string(
        worker.settings.database_url, serde=checkpoint_serializer()
    ) as saver:
        await saver.setup()
        graph = build_session_graph(
            services(), worker.bindings, checkpointer=saver
        )
        host = SessionCoordinator(graph, worker.guard)
        task = turn()
        async with worker.guard.hold(task.session_id):
            with pytest.raises(DeferEvent, match="another process"):
                await host.start(task)
        assert not (
            await graph.aget_state(
                {"configurable": {"thread_id": task.session_id}}
            )
        ).values
        await host.start(task)
