"""Real admission transactions and graph checkpoints with injected crashes."""

import asyncio
import os
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, select

from agent.admission.contracts import ApiRequest
from agent.admission.models import ApiRequestRow
from agent.admission.recovery import RequestRecovery
from agent.admission.service import AdmissionService
from agent.session import SessionConflictError
from ex_agent.domain.enums import Intent, TaskStatus
from ex_agent.persistence.models import Message, Task, WorkflowCommand
from tests.agent.admission_support import (
    admission_harness,
    decision_request,
    snapshot,
)
from tests.agent.support import boundary, turn
from worker.contracts import DeferEvent

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        "TEST_DATABASE_URL" not in os.environ,
        reason="Requires isolated PostgreSQL",
    ),
]


@pytest.fixture
async def admitted(tmp_path, monkeypatch):
    async with admission_harness(tmp_path, monkeypatch) as h:
        yield h


async def count(h, model):
    key = model.id if model is Task else model.task_id
    async with h.sessions() as session:
        return await session.scalar(
            select(func.count())
            .select_from(model)
            .where(key == UUID(h.task.active_task_id))
        )


async def test_atomic_admission_without_legacy_command_and_api_restart(
    admitted,
):
    h = admitted
    saved = await h.host.accept(h.command)
    assert saved.state == "PENDING" and saved.attempts == 0
    assert saved.created_by == saved.updated_by == h.task.user_id
    assert saved.created_at <= saved.updated_at
    assert await count(h, Task) == await count(h, Message) == 1
    assert await count(h, WorkflowCommand) == 0
    assert await h.host.accept(h.command) == saved
    assert not (await snapshot(h)).values
    with pytest.raises(SessionConflictError, match="different input"):
        await h.host.accept(
            h.command.model_copy(
                update={
                    "turn": h.task.model_copy(
                        update={"user_message": "changed"}
                    ),
                }
            )
        )
    new = ApiRequest(
        request_id=uuid4(),
        kind="START",
        turn=turn(session=h.task.session_id),
    )
    with pytest.raises(SessionConflictError, match="pending API"):
        await h.host.accept(new)
    host = AdmissionService(h.graph, h.guard, h.store)
    await RequestRecovery(host).once()
    completed = await h.store.get(h.command.request_id)
    assert completed.state == "APPLIED" and completed.attempts == 1
    assert completed.updated_by == "AGENT"
    assert boundary(await snapshot(h)).value["kind"] == "PLAN_REVIEW"
    assert not h.remote.calls  # Approval is still required.
    assert await count(h, ApiRequestRow) == 1


async def test_start_input_checkpoint_recovers_without_readmitting_task(
    admitted,
    monkeypatch,
):
    h = admitted
    from agent.graph import builder

    original = builder.begin_task

    def die_before_begin(*args, **kwargs):
        raise RuntimeError("before begin receipt")

    with monkeypatch.context() as patch:
        patch.setattr(builder, "begin_task", die_before_begin)
        graph = builder.build_session_graph(
            h.service,
            h.bindings,
            checkpointer=h.graph.checkpointer,
        )
        host = AdmissionService(graph, h.guard, h.store)
        result = await host.handle(h.command)
        assert result.state == "PENDING"
        assert result.last_error is not None
        assert "before begin receipt" in result.last_error
    assert (await snapshot(h)).next == ("begin_task",)
    assert original is builder.begin_task
    result = await h.host.execute(h.command.request_id)
    assert result.state == "APPLIED" and result.attempts == 2
    assert await count(h, Task) == 1


async def test_resume_writes_before_acceptance_output_are_recoverable(
    admitted,
    monkeypatch,
):
    from agent.graph import build_session_graph
    from ex_agent.graph.node_groups.planning import PlanningNodes

    h = admitted
    await h.host.handle(h.command)
    decision = await decision_request(h)
    review = PlanningNodes.review_plan

    def fail_after_resume(self, state):
        review(self, state)
        raise RuntimeError("before approval receipt")

    with monkeypatch.context() as patch:
        patch.setattr(PlanningNodes, "review_plan", fail_after_resume)
        graph = build_session_graph(
            h.service,
            h.bindings,
            checkpointer=h.graph.checkpointer,
        )
        host = AdmissionService(graph, h.guard, h.store)
        result = await host.handle(decision)
        assert result.state == "PENDING"
        assert result.last_error is not None
        assert "before approval receipt" in result.last_error
    assert not h.remote.calls
    assert (await h.host.execute(decision.request_id)).state == "APPLIED"
    assert len(h.remote.operations) == 1
    assert boundary(await snapshot(h)).value["kind"] == "EXECUTOR_EVENT"


async def test_approval_response_loss_reuses_execution_and_receipt(admitted):
    h = admitted
    assert (await h.host.handle(h.command)).state == "APPLIED"
    decision = await decision_request(h)
    h.remote.lose_responses = 3
    result = await h.host.handle(decision)
    assert result.state == "PENDING" and "ReadTimeout" in result.last_error
    assert len(h.remote.operations) == 1
    host = AdmissionService(h.graph, h.guard, h.store)
    completed = await host.execute(decision.request_id)
    assert completed.state == "APPLIED"
    assert len(h.remote.operations) == 1
    assert len(h.remote.calls) == 4
    assert all(call == h.remote.calls[0] for call in h.remote.calls)
    assert (await host.handle(decision)).state == "APPLIED"
    assert len(h.remote.calls) == 4
    assert boundary(await snapshot(h)).value["kind"] == "EXECUTOR_EVENT"
    assert await count(h, WorkflowCommand) == 0


async def test_revision_request_retry_never_answers_the_new_interrupt(
    admitted,
    monkeypatch,
):
    h = admitted
    await h.host.handle(h.command)
    decision = await decision_request(h, "REVISE")
    finish = h.store.finish

    async def die_before_request_commit(*args, **kwargs):
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(h.store, "finish", die_before_request_commit)
        with pytest.raises(asyncio.CancelledError):
            await h.host.handle(decision)
    after = await snapshot(h)
    waiting = boundary(after)
    assert waiting.id != decision.interrupt_id
    assert waiting.value["plan_revision_number"] == 2
    assert (await h.host.execute(decision.request_id)).state == "APPLIED"
    assert boundary(await snapshot(h)).id == waiting.id
    assert not h.remote.calls
    assert h.store.finish == finish


async def test_last_attempt_can_settle_completed_checkpoint(
    admitted, monkeypatch
):
    h = admitted
    h.host.max_attempts = 1

    async def die_before_request_commit(*args, **kwargs):
        raise asyncio.CancelledError()

    with monkeypatch.context() as patch:
        patch.setattr(h.store, "finish", die_before_request_commit)
        with pytest.raises(asyncio.CancelledError):
            await h.host.handle(h.command)
    assert (await h.store.get(h.command.request_id)).state == "RUNNING"
    result = await h.host.execute(h.command.request_id)
    assert result.state == "APPLIED" and result.attempts == 1


async def test_stale_plan_and_wrong_task_rejected_before_admission(admitted):
    h = admitted
    await h.host.handle(h.command)
    decision = await decision_request(h)
    bad = decision.model_copy(
        update={
            "payload": {
                **decision.payload,
                "public_payload_hash": "0" * 64,
            }
        }
    )
    with pytest.raises(SessionConflictError, match="stale"):
        await h.host.accept(bad)
    bad = decision.model_copy(
        update={
            "turn": h.task.model_copy(
                update={
                    "user_id": "another-user",
                }
            )
        }
    )
    with pytest.raises(SessionConflictError, match="identity mismatch"):
        await h.host.accept(bad)
    assert await h.store.get(decision.request_id) is None
    assert await count(h, ApiRequestRow) == 1
    assert not h.remote.calls


async def test_retry_limit_blocks_session_without_pretending_task_failed(
    admitted,
    monkeypatch,
):
    h = admitted
    h.host.max_attempts = 1
    h.service.classify_intent.side_effect = RuntimeError("model down")
    result = await h.host.handle(h.command)
    assert result.state == "BLOCKED" and "model down" in result.last_error
    with pytest.raises(SessionConflictError, match="pending API"):
        await h.store.accept(
            ApiRequest(request_id=uuid4(), turn=h.task, kind="START"),
            target_node="begin_task",
            base_checkpoint_id=None,
        )
    assert h.command.request_id not in await h.store.due()
    assert (await h.host.execute(h.command.request_id)).attempts == 1
    async with h.sessions() as session:
        task = await session.get(Task, UUID(h.task.active_task_id))
        assert not TaskStatus(task.status).is_terminal


async def test_busy_guard_does_not_consume_attempt_budget(admitted):
    h = admitted
    await h.host.accept(h.command)

    class BusyGuard:
        @asynccontextmanager
        async def hold(self, session_id):
            raise DeferEvent("busy")
            yield

    host = AdmissionService(h.graph, BusyGuard(), h.store)
    result = await host.execute(h.command.request_id)
    assert result.state == "PENDING" and result.attempts == 0
    assert h.command.request_id not in await h.store.due()


async def test_old_attempt_cannot_finish_new_claim(admitted):
    h = admitted
    await h.host.accept(h.command)
    first = await h.store.claim(
        h.command.request_id,
        max_attempts=5,
        recovery_delay=1,
    )
    second = await h.store.claim(
        h.command.request_id,
        max_attempts=5,
        recovery_delay=1,
    )
    with pytest.raises(SessionConflictError, match="superseded"):
        await h.store.finish(first, state="APPLIED")
    assert second.attempts == 2


async def test_database_arbitrates_concurrent_session_admissions(admitted):
    h = admitted
    commands = [
        h.command,
        ApiRequest(
            request_id=uuid4(),
            kind="START",
            turn=turn(session=h.task.session_id),
        ),
    ]
    results = await asyncio.gather(
        *(
            h.store.accept(
                command,
                target_node="begin_task",
                base_checkpoint_id=None,
            )
            for command in commands
        ),
        return_exceptions=True,
    )
    assert (
        sum(isinstance(result, SessionConflictError) for result in results)
        == 1
    )
    async with h.sessions() as session:
        assert (
            await session.scalar(
                select(func.count())
                .select_from(Task)
                .where(
                    Task.session_id == h.task.session_id,
                )
            )
            == 1
        )
        assert (
            await session.scalar(
                select(func.count())
                .select_from(ApiRequestRow)
                .where(
                    ApiRequestRow.session_id == h.task.session_id,
                )
            )
            == 1
        )


async def test_question_answer_terminal_commit_is_replay_safe(
    tmp_path, monkeypatch
):
    async with admission_harness(
        tmp_path,
        monkeypatch,
        intent=Intent.GENERAL_QA,
    ) as h:
        original = h.service.projections.terminal

        async def fail_after_terminal(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError("terminal commit crash")

        with monkeypatch.context() as patch:
            patch.setattr(
                h.service.projections, "terminal", fail_after_terminal
            )
            result = await h.host.handle(h.command)
            assert result.state == "PENDING"
        assert (await h.host.execute(h.command.request_id)).state == "APPLIED"
        assert await count(h, Message) == 2  # One user, one assistant.
        assert h.service.answer_question.await_count == 1
        assert not (await snapshot(h)).next
        assert not h.remote.calls
