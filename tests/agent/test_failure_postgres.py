"""Real Agent DB/graph, faulted HTTP; never use operational infrastructure."""

import asyncio
import os
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from sqlalchemy import select

from agent.admission.contracts import ApiRequest
from agent.failure.recovery import FailureRecovery
from agent.session import SessionConflictError
from ex_agent.domain.enums import ExecutionMode, Intent
from ex_agent.persistence.models import Message, SessionLock, Task
from tests.agent.admission_support import (
    admission_harness,
    decision_request,
    snapshot,
)
from tests.agent.failure_support import cleanup
from tests.agent.support import turn

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        "TEST_DATABASE_URL" not in os.environ,
        reason="Requires isolated PostgreSQL",
    ),
]


async def messages(h):
    async with h.sessions() as session:
        return list(
            await session.scalars(
                select(Message).where(
                    Message.task_id == UUID(h.task.active_task_id),
                    Message.role == "assistant",
                )
            )
        )


async def assert_locked(h):
    async with h.sessions() as session:
        lock = await session.get(SessionLock, h.task.session_id)
        assert lock is not None and lock.locked


async def test_failure_before_submit_closes_graph_and_unlocks_next_task(
    tmp_path, monkeypatch
):
    async with admission_harness(tmp_path, monkeypatch) as h:
        h.host.max_attempts = 1
        h.service.classify_intent.side_effect = RuntimeError(
            "model unavailable"
        )
        assert (await h.host.handle(h.command)).state == "BLOCKED"
        service = cleanup(h)
        await service.capture_api(h.command.request_id)
        await assert_locked(h)
        await service.execute(UUID(h.task.active_task_id))
        record = await service.store.get(UUID(h.task.active_task_id))
        assert (
            record.state == "DONE" and record.executor_status == "NOT_REQUIRED"
        )
        assert record.created_by == record.updated_by == "AGENT"
        assert record.created_at <= record.updated_at
        assert not h.remote.calls and not h.remote.reads
        assert not (await snapshot(h)).next
        assert (await snapshot(h)).values["workflow"]["phase"] == "FAILED"
        assert (await h.store.get(h.command.request_id)).state == "COMPENSATED"
        await service.execute(UUID(h.task.active_task_id))
        assert len(await messages(h)) == 1
        h.service.classify_intent.side_effect = None
        new = ApiRequest(
            request_id=uuid4(),
            kind="START",
            turn=turn(session=h.task.session_id),
        )
        assert (await h.host.handle(new)).state == "APPLIED"


async def test_unreceived_submit_response_lookup_does_not_resubmit(
    tmp_path, monkeypatch
):
    async with admission_harness(
        tmp_path, monkeypatch, mode=ExecutionMode.MULTI
    ) as h:
        await h.host.handle(h.command)
        approval = await decision_request(h)
        h.host.max_attempts = 1
        h.remote.lose_responses = 3
        assert (await h.host.handle(approval)).state == "BLOCKED"
        async with h.sessions() as session:
            task = await session.get(Task, UUID(h.task.active_task_id))
            assert task.execution_id is None
        service = cleanup(h)
        h.remote.cancel_pending = True
        await FailureRecovery(service).once()
        record = await service.store.get(UUID(h.task.active_task_id))
        assert record.state == "PENDING"
        assert record.execution_id == h.remote.execution_id
        async with h.sessions() as session:
            task = await session.get(Task, UUID(h.task.active_task_id))
            lock = await session.get(SessionLock, h.task.session_id)
            assert task.execution_id == h.remote.execution_id
            assert lock.execution_id == h.remote.execution_id
        lookups = sum(path.endswith("/executions") for path in h.remote.reads)
        h.remote.status = "CANCELLED"
        await service.execute(UUID(h.task.active_task_id))
        record = await service.store.get(UUID(h.task.active_task_id))
        assert record.state == "DONE"
        assert record.executor_status == "CANCELLED"
        assert len(h.remote.operations) == 1
        assert (
            len(h.remote.calls) == 4
        )  # Three lost submit responses, one cancel.
        assert h.remote.calls[-1][0].endswith("/cancel")
        assert h.remote.calls[-1][1]["actor"]["type"] == "AGENT"
        assert lookups == 1
        assert (
            sum(path.endswith("/executions") for path in h.remote.reads)
            == lookups
        )
        assert len(await messages(h)) == 1


@pytest.mark.parametrize("ambiguous", [False, True])
async def test_unresolved_submission_keeps_lock_and_never_claims_no_execution(
    tmp_path,
    monkeypatch,
    ambiguous,
):
    async with admission_harness(
        tmp_path, monkeypatch, mode=ExecutionMode.MULTI
    ) as h:
        await h.host.handle(h.command)
        approval = await decision_request(h)
        h.host.max_attempts = 1
        h.remote.lose_responses = 3
        await h.host.handle(approval)
        h.remote.lookup_items = (
            [{"execution_id": str(uuid4())}] * 2 if ambiguous else []
        )
        service = cleanup(h, max_attempts=1)
        await service.capture_api(approval.request_id)
        await service.execute(UUID(h.task.active_task_id))
        record = await service.store.get(UUID(h.task.active_task_id))
        assert record.state == "BLOCKED" and record.executor_status is None
        await assert_locked(h)
        assert not await messages(h)
        assert len(h.remote.calls) == 3
        assert (await snapshot(h)).next  # Not silently retired.


async def test_cancel_acceptance_waits_for_terminal_and_survives_recovery(
    tmp_path, monkeypatch
):
    async with admission_harness(
        tmp_path, monkeypatch, mode=ExecutionMode.MULTI
    ) as h:
        await h.host.handle(h.command)
        h.host.max_attempts = 1
        approval = await decision_request(h)
        with monkeypatch.context() as patch:
            patch.setattr(
                h.bindings,
                "register",
                AsyncMock(side_effect=RuntimeError("binding crash")),
            )
            assert (await h.host.handle(approval)).state == "BLOCKED"
        service = cleanup(h)
        await service.capture_api(approval.request_id)
        h.remote.cancel_pending = True
        await service.execute(UUID(h.task.active_task_id))
        await assert_locked(h)
        record = await service.store.get(UUID(h.task.active_task_id))
        assert record.state == "PENDING" and record.executor_status is None
        assert not await messages(h)
        with pytest.raises(SessionConflictError, match=r"pending API|cleanup"):
            await h.store.accept(
                approval.model_copy(update={"request_id": uuid4()}),
                target_node="review_plan",
                base_checkpoint_id=None,
            )
        h.remote.status = "CANCELLED"
        await cleanup(h).execute(UUID(h.task.active_task_id))
        assert (
            await service.store.get(UUID(h.task.active_task_id))
        ).state == "DONE"
        assert len(h.remote.calls) == 2  # No second cancel, no report.
        async with h.sessions() as session:
            assert await session.get(SessionLock, h.task.session_id) is None


@pytest.mark.parametrize("crash_at", ["after_clear", "after_graph"])
async def test_cleanup_checkpoint_and_db_crash_keep_proof_and_message_unique(
    tmp_path,
    monkeypatch,
    crash_at,
):
    async with admission_harness(tmp_path, monkeypatch) as h:
        h.host.max_attempts = 1
        h.service.classify_intent.side_effect = RuntimeError(
            "classification failed"
        )
        await h.host.handle(h.command)
        service = cleanup(
            h, max_attempts=1 if crash_at == "after_graph" else 5
        )
        await service.capture_api(h.command.request_id)
        original = h.graph.aupdate_state

        async def die_after_clear(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("checkpoint clear committed")

        async def die_before_finish(*args, **kwargs):
            raise asyncio.CancelledError()

        with monkeypatch.context() as patch:
            if crash_at == "after_clear":
                patch.setattr(h.graph, "aupdate_state", die_after_clear)
                await service.execute(UUID(h.task.active_task_id))
            else:
                patch.setattr(service.store, "finish", die_before_finish)
                with pytest.raises(asyncio.CancelledError):
                    await service.execute(UUID(h.task.active_task_id))
        await assert_locked(h)
        assert not await messages(h)
        await service.execute(UUID(h.task.active_task_id))
        assert (
            await service.store.get(UUID(h.task.active_task_id))
        ).state == "DONE"
        assert len(await messages(h)) == 1
        assert len((await snapshot(h)).values["messages"]) == 2
        assert not h.remote.calls


async def test_existing_success_is_preserved_after_terminal_write_crash(
    tmp_path, monkeypatch
):
    async with admission_harness(
        tmp_path, monkeypatch, intent=Intent.GENERAL_QA
    ) as h:
        h.host.max_attempts = 1
        original = h.service.projections.terminal

        async def fail_after_terminal(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("terminal commit succeeded, checkpoint failed")

        with monkeypatch.context() as patch:
            patch.setattr(
                h.service.projections, "terminal", fail_after_terminal
            )
            assert (await h.host.handle(h.command)).state == "BLOCKED"
        await FailureRecovery(cleanup(h)).once()
        assert (await snapshot(h)).values["workflow"]["phase"] == "SUCCEEDED"
        assert len(await messages(h)) == 1
        assert not h.remote.calls
        assert h.service.answer_question.await_count == 1


async def test_completed_api_checkpoint_is_not_compensated(
    tmp_path, monkeypatch
):
    async with admission_harness(tmp_path, monkeypatch) as h:
        h.host.max_attempts = 1
        original = h.store.finish

        async def fail_applied(*args, **kwargs):
            if kwargs.get("state") == "APPLIED":
                raise RuntimeError("request update unavailable")
            return await original(*args, **kwargs)

        with monkeypatch.context() as patch:
            patch.setattr(h.store, "finish", fail_applied)
            assert (await h.host.handle(h.command)).state == "BLOCKED"
        service = cleanup(h)
        await FailureRecovery(service).once()
        assert (await h.store.get(h.command.request_id)).state == "APPLIED"
        assert await service.store.get(UUID(h.task.active_task_id)) is None
        assert not await messages(h)
