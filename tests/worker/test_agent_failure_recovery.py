"""Actual Worker failures, Redis session guard and PG checkpoint recovery."""

from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.failure.recovery import FailureRecovery
from agent.graph import checkpoint_serializer
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from ex_agent.domain.enums import ExecutionMode
from ex_agent.persistence.models import SessionLock, Task
from tests.agent.admission_support import (
    admission_harness,
    decision_request,
    snapshot,
)
from tests.agent.failure_support import cleanup
from tests.agent.support import event_context
from tests.worker.test_agent_effect_recovery import enqueue
from worker import DeferEvent
from worker.consumer import AckDecision, PermanentMessageError
from worker.dispatcher import Dispatcher

pytestmark = [pytest.mark.postgres, pytest.mark.redis]


async def test_worker_failed_command_drives_cleanup_and_audited_resolution(
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
            service = cleanup(h)
            handler = service.protect(SessionGraphAdapter(h.graph))
            dispatcher = Dispatcher(
                worker.store,
                worker.guard,
                {
                    "execution.operation_completed": handler,
                    "execution.completed": handler,
                },
                max_attempts=1,
            )
            ctx = event_context(
                h.task,
                (await snapshot(h)).values["execution_id"],
                kind="execution.operation_completed",
            )
            message = await enqueue(worker, ctx)
            with monkeypatch.context() as patch:
                patch.setattr(
                    h.service,
                    "adapt_multi_plan",
                    AsyncMock(
                        side_effect=RuntimeError("planner failed permanently")
                    ),
                )
                with pytest.raises(PermanentMessageError):
                    await dispatcher.handle(message)
            command_id = UUID(message.fields["command_id"])
            assert (await worker.store.command(command_id))[
                "state"
            ] == "FAILED"
            h.remote.cancel_pending = True
            recovery = FailureRecovery(service, worker.store)
            await recovery.once()
            record = await service.store.get(UUID(h.task.active_task_id))
            assert record.state == "PENDING"
            async with h.sessions() as session:
                assert (
                    await session.get(SessionLock, h.task.session_id)
                ).locked
            # An event cannot restart business logic while cleanup owns it.
            with pytest.raises(DeferEvent, match="cleanup owns"):
                await handler(
                    event_context(h.task, ctx.execution_id, sequence=2)
                )
            async with worker.guard.hold(h.task.session_id):
                await service.execute(UUID(h.task.active_task_id))
            assert (
                await service.store.get(UUID(h.task.active_task_id))
            ).attempts == record.attempts
            h.remote.status = "CANCELLED"
            await service.execute(UUID(h.task.active_task_id))
            await (
                recovery.once()
            )  # Resolve FAILED source after business completion.
            row = await worker.store.command(command_id)
            # SKIP is terminal without creating a retry generation.
            assert row["state"] == "IGNORED" and row["generation"] == 1
            assert (
                await dispatcher.handle(message)
            ).decision == AckDecision.ACK
            async with h.sessions() as session:
                assert (
                    await session.get(Task, UUID(h.task.active_task_id))
                ).status == "FAILED"
                assert (
                    await session.get(SessionLock, h.task.session_id) is None
                )
            assert not (await snapshot(h)).next
            assert len(h.remote.calls) == 2  # submit + cancellation, no report
            async with worker.store.pool.connection() as conn:
                cur = await conn.execute(
                    "SELECT count(*) FROM ew_audit "
                    "WHERE namespace=%s AND command_id=%s",
                    (worker.store.namespace, command_id),
                )
                assert await cur.fetchone() == (1,)


async def test_failed_command_keyset_page_and_namespace_isolation(worker):
    from tests.worker.test_session_graph import context

    ids = []
    for _ in range(3):
        ctx = context()
        await worker.bindings.register(
            execution_id=ctx.execution_id,
            session_id=ctx.session_id,
            task_id=ctx.task_id,
        )
        message = await enqueue(worker, ctx)
        command_id = UUID(message.fields["command_id"])
        await worker.store.set_state(command_id, "FAILED", error="test")
        ids.append(command_id)
    first = await worker.store.failed_page(limit=2)
    second = await worker.store.failed_page(
        after=(first[-1]["execution_id"], first[-1]["sequence"]), limit=2
    )
    assert {row["command_id"] for row in first + second} == set(ids)
    with pytest.raises(ValueError):
        await worker.store.failed_page(limit=101)
