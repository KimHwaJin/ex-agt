"""Real graph/checkpoints/worker/Agent DB; faulted Executor HTTP substitute."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from sqlalchemy import select

from agent.graph import build_session_graph, checkpoint_serializer
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from agent.session import SessionCoordinator
from ex_agent.domain.contracts import MultiDecision
from ex_agent.domain.enums import ExecutionMode, MultiAction
from ex_agent.persistence.models import Message
from tests.agent.effect_support import effect_harness
from tests.agent.support import event_context, review, services
from worker.consumer import AckDecision, StreamMessage
from worker.dispatcher import Dispatcher


async def enqueue(worker, ctx):
    await worker.store.ingest(ctx.event)
    await worker.store.advance(ctx.execution_id, {ctx.event.event_type}, 100)
    await worker.outbox.once()
    return StreamMessage(
        *(
            await worker.redis.xrevrange(
                worker.settings.command_stream,
                count=1,
            )
        )[0]
    )


@pytest.mark.postgres
@pytest.mark.redis
async def test_real_effects_resume_after_binding_and_terminal_commits(
    worker,
    tmp_path,
    monkeypatch,
):
    async with effect_harness(tmp_path) as h:
        fake = services(mode=ExecutionMode.MULTI)
        for method in (
            "classify_intent",
            "review_request_risk",
            "search_workflows",
            "review_compiled_code_risk",
        ):
            monkeypatch.setattr(h.service, method, getattr(fake, method))
        monkeypatch.setattr(
            h.service, "build_plan", AsyncMock(return_value=h.state["plan"])
        )
        decision = MultiDecision(
            action=MultiAction.APPEND_STEP,
            rationale="Need another cell",
            next_step=h.state["plan"].steps[1],
        )
        adapt = AsyncMock(
            side_effect=[
                decision,
                MultiDecision(
                    action=MultiAction.FINALIZE,
                    rationale="All cells done",
                ),
            ]
        )
        monkeypatch.setattr(h.service, "adapt_multi_plan", adapt)
        url = worker.settings.database_url
        async with AsyncPostgresSaver.from_conn_string(
            url,
            serde=checkpoint_serializer(),
        ) as saver:
            await saver.setup()
            graph = build_session_graph(
                h.service, worker.bindings, checkpointer=saver
            )
            host = SessionCoordinator(graph, worker.guard)
            waiting = await review(host, h.task, await host.start(h.task))
            ctx = event_context(
                h.task,
                waiting.values["execution_id"],
                kind="execution.operation_completed",
            )
            message = await enqueue(worker, ctx)
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {
                    ctx.event.event_type: SessionGraphAdapter(graph),
                },
            )
            original = h.service.projections.binding

            async def crash_after_binding(*args, **kwargs):
                await original(*args, **kwargs)
                raise RuntimeError("Committed append binding")

            with monkeypatch.context() as patch:
                patch.setattr(
                    h.service.projections, "binding", crash_after_binding
                )
                deferred = await dispatcher.handle(message)
                assert deferred.decision == AckDecision.DEFER
                assert deferred.outcome == "handler_retry"
                command = await worker.store.command(
                    UUID(message.fields["command_id"])
                )
                assert command is not None
                assert "Committed append binding" in command["last_error"]
            assert len(h.remote.operations) == 2

        # Recreate the graph and checkpointer like a new Worker process.
        async with AsyncPostgresSaver.from_conn_string(
            url,
            serde=checkpoint_serializer(),
        ) as saver:
            graph = build_session_graph(
                h.service, worker.bindings, checkpointer=saver
            )
            adapter = SessionGraphAdapter(graph)
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {
                    "execution.operation_completed": adapter,
                    "execution.completed": adapter,
                },
            )
            assert (
                await dispatcher.handle(message)
            ).decision == AckDecision.ACK
            snapshot = await graph.aget_state(ctx.graph_config)
            assert snapshot.values["workflow"]["plan_revision_number"] == 2
            assert len(h.remote.operations) == 2
            assert len(h.remote.calls) == 2
            ctx2 = event_context(
                h.task,
                ctx.execution_id,
                sequence=2,
                kind="execution.operation_completed",
            )
            assert (
                await dispatcher.handle(await enqueue(worker, ctx2))
            ).decision == AckDecision.ACK
            ctx3 = event_context(h.task, ctx.execution_id, sequence=3)
            completed = await enqueue(worker, ctx3)
            terminal = h.service.projections.terminal

            async def crash_after_terminal(*args, **kwargs):
                await terminal(*args, **kwargs)
                raise RuntimeError("Committed terminal message")

            with monkeypatch.context() as patch:
                patch.setattr(
                    h.service.projections, "terminal", crash_after_terminal
                )
                deferred = await dispatcher.handle(completed)
                assert deferred.decision == AckDecision.DEFER
                assert deferred.outcome == "handler_retry"
                command = await worker.store.command(
                    UUID(completed.fields["command_id"])
                )
                assert command is not None
                assert "Committed terminal message" in command["last_error"]
            assert (
                await dispatcher.handle(completed)
            ).decision == AckDecision.ACK
            snapshot = await graph.aget_state(ctx3.graph_config)
            assert not snapshot.next
            assert snapshot.values["workflow"]["phase"] == "SUCCEEDED"
            assert len(h.remote.calls) == 4  # submit, append, finalize, report
            assert h.model.i == 1
            task_id = UUID(h.task.active_task_id)
            async with h.sessions() as session:
                messages = await session.scalars(
                    select(Message).where(
                        Message.task_id == task_id, Message.role == "assistant"
                    )
                )
                assert len(messages.all()) == 1
