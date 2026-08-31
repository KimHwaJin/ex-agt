"""API/Worker ownership with real PostgreSQL checkpoints and Redis guard."""

import asyncio
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.admission.contracts import ApiRequest
from agent.admission.service import AdmissionService
from agent.graph import build_session_graph, checkpoint_serializer
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from ex_agent.domain.contracts import MultiDecision
from ex_agent.domain.enums import ExecutionMode, MultiAction
from ex_agent.persistence.models import SessionLock, Task
from tests.agent.admission_support import (
    admission_harness,
    decision_request,
    snapshot,
)
from tests.agent.support import boundary, event_context
from tests.worker.test_agent_effect_recovery import enqueue
from worker.consumer import AckDecision
from worker.dispatcher import Dispatcher

pytestmark = [pytest.mark.postgres, pytest.mark.redis]


async def test_old_worker_delivery_cannot_recover_pending_user_approval(
    worker,
    tmp_path,
    monkeypatch,
):
    async with AsyncPostgresSaver.from_conn_string(
        worker.settings.database_url,
        serde=checkpoint_serializer(),
    ) as saver:
        await saver.setup()
        async with admission_harness(
            tmp_path,
            monkeypatch,
            saver=saver,
            mode=ExecutionMode.MULTI,
            guard=worker.guard,
            bindings=worker.bindings,
        ) as h:
            await h.host.handle(h.command)
            await h.host.handle(await decision_request(h))
            monkeypatch.setattr(
                h.service,
                "adapt_multi_plan",
                AsyncMock(
                    return_value=MultiDecision(
                        action=MultiAction.REQUIRE_REAPPROVAL,
                        rationale="Scope changed",
                        next_step=h.state["plan"].steps[1],
                    ),
                ),
            )
            ctx = event_context(
                h.task,
                (await snapshot(h)).values["execution_id"],
                kind="execution.operation_completed",
            )
            message = await enqueue(worker, ctx)
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {
                    ctx.event.event_type: SessionGraphAdapter(h.graph),
                },
            )
            original = worker.store.set_state

            async def crash_before_done(command_id, state, **kwargs):
                if state == "DONE":
                    raise RuntimeError("Before Worker DONE")
                await original(command_id, state, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(worker.store, "set_state", crash_before_done)
                with pytest.raises(RuntimeError, match="Before Worker DONE"):
                    await dispatcher.handle(message)
            decision = await decision_request(h)
            with monkeypatch.context() as patch:
                patch.setattr(
                    h.service,
                    "append_operation",
                    AsyncMock(
                        side_effect=RuntimeError("API append crashed"),
                    ),
                )
                result = await h.host.handle(decision)
                assert result.state == "PENDING"
                assert "API append crashed" in result.last_error
            before = await snapshot(h)
            assert before.values["invocation_owner"] == {
                "source": "API",
                "id": str(decision.request_id),
            }
            assert (
                await dispatcher.handle(message)
            ).decision == AckDecision.ACK
            assert (await snapshot(h)).next == before.next
            assert len(h.remote.operations) == 1
            assert (
                await h.host.execute(decision.request_id)
            ).state == "APPLIED"
            assert len(h.remote.operations) == 2


async def test_api_crash_then_worker_handoff_and_new_host_recovery(
    worker,
    tmp_path,
    monkeypatch,
):
    url = worker.settings.database_url
    async with AsyncPostgresSaver.from_conn_string(
        url,
        serde=checkpoint_serializer(),
    ) as saver:
        await saver.setup()
        async with admission_harness(
            tmp_path,
            monkeypatch,
            saver=saver,
            guard=worker.guard,
            bindings=worker.bindings,
        ) as h:
            await h.host.handle(h.command)
            decision = await decision_request(h)

            async def crash_before_db_finish(*args, **kwargs):
                raise asyncio.CancelledError()

            with monkeypatch.context() as patch:
                patch.setattr(h.store, "finish", crash_before_db_finish)
                with pytest.raises(asyncio.CancelledError):
                    await h.host.handle(decision)
            assert (await h.store.get(decision.request_id)).state == "RUNNING"
            ctx = event_context(
                h.task, (await snapshot(h)).values["execution_id"]
            )
            message = await enqueue(worker, ctx)
            adapter = SessionGraphAdapter(h.graph)
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {ctx.event.event_type: adapter},
            )
            original = h.service.build_report_evidence
            with monkeypatch.context() as patch:
                patch.setattr(
                    h.service,
                    "build_report_evidence",
                    AsyncMock(
                        side_effect=RuntimeError("worker report crashed"),
                    ),
                )
                result = await dispatcher.handle(message)
                assert result.decision == AckDecision.DEFER
                command = await worker.store.command(
                    UUID(message.fields["command_id"])
                )
                assert "worker report crashed" in command["last_error"]
            pending = await snapshot(h)
            assert pending.values["invocation_owner"] == {
                "source": "EXECUTOR",
                "id": message.fields["command_id"],
            }
            assert h.service.build_report_evidence == original

            # Independent connection and newly compiled graph model an API
            # process replacement. It must not take over Worker-owned work.
            async with AsyncPostgresSaver.from_conn_string(
                url,
                serde=checkpoint_serializer(),
            ) as replacement:
                graph = build_session_graph(
                    h.service,
                    worker.bindings,
                    checkpointer=replacement,
                )
                host = AdmissionService(graph, worker.guard, h.store)
                assert (
                    await host.execute(decision.request_id)
                ).state == "APPLIED"
                assert (await snapshot(h)).next == pending.next
                assert len(h.remote.calls) == 1  # Report not generated by API.
                adapter = SessionGraphAdapter(graph)
                dispatcher = Dispatcher(
                    worker.store,
                    worker.guard,
                    {ctx.event.event_type: adapter},
                )
                assert (
                    await dispatcher.handle(message)
                ).decision == AckDecision.ACK
            assert not (await snapshot(h)).next
            assert (await snapshot(h)).values["workflow"][
                "phase"
            ] == "SUCCEEDED"
            assert len(h.remote.operations) == 1
            assert len(h.remote.calls) == 2  # submit + report


async def test_cancel_admission_is_not_task_cancellation_until_executor_event(
    worker,
    tmp_path,
    monkeypatch,
):
    async with AsyncPostgresSaver.from_conn_string(
        worker.settings.database_url,
        serde=checkpoint_serializer(),
    ) as saver:
        await saver.setup()
        async with admission_harness(
            tmp_path,
            monkeypatch,
            saver=saver,
            guard=worker.guard,
            bindings=worker.bindings,
        ) as h:
            await h.host.handle(h.command)
            await h.host.handle(await decision_request(h))
            waiting = boundary(await snapshot(h))
            cancel = ApiRequest(
                request_id=uuid4(),
                turn=h.task,
                kind="CANCEL",
                interrupt_id=waiting.id,
                payload={
                    "type": "CANCEL_REQUESTED",
                    "task_id": h.task.active_task_id,
                    "reason": "test",
                },
            )
            await h.host.accept(cancel)
            async with worker.guard.hold(h.task.session_id):
                busy = await h.host.execute(cancel.request_id)
                assert busy.state == "PENDING" and busy.attempts == 0
            applied = await h.host.execute(cancel.request_id)
            assert applied.state == "APPLIED"
            assert (await snapshot(h)).values["workflow"][
                "phase"
            ] == "CANCEL_REQUESTED"
            async with h.sessions() as session:
                lock = await session.get(SessionLock, h.task.session_id)
                assert lock.locked
                task = await session.get(Task, UUID(h.task.active_task_id))
                assert task.status != "CANCELLED"
            assert (await h.host.handle(cancel)).state == "APPLIED"
            assert len(h.remote.calls) == 2  # submit and one cancel
            ctx = event_context(h.task, waiting.value["execution_id"])
            message = await enqueue(worker, ctx)
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {
                    ctx.event.event_type: SessionGraphAdapter(h.graph),
                },
            )
            assert (
                await dispatcher.handle(message)
            ).decision == AckDecision.ACK
            async with h.sessions() as session:
                assert (
                    await session.get(SessionLock, h.task.session_id) is None
                )
                task = await session.get(Task, UUID(h.task.active_task_id))
                assert task.status == "CANCELLED"
            assert len(h.remote.calls) == 2  # No failure/cancellation report.
