"""Failure windows exercised against real PostgreSQL, not repository mocks."""

import asyncio
import os
from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select, update

from agent.effects.models import ExecutorEffect
from agent.effects.store import EffectStore, digest
from ex_agent.domain.contracts import MultiDecision
from ex_agent.domain.enums import ExecutionMode, MultiAction, TaskStatus
from ex_agent.graph.node_groups.execution import ExecutionNodes
from ex_agent.persistence.models import Message, PlanRevision, TaskEvent
from tests.agent.effect_support import effect_harness, submitted
from tests.test_execution_mode_policy import plan

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        "TEST_DATABASE_URL" not in os.environ,
        reason="Requires isolated PostgreSQL",
    ),
]


@pytest.fixture
async def effects(tmp_path):
    async with effect_harness(tmp_path) as harness:
        yield harness


async def count_rows(h, model):
    async with h.sessions() as session:
        query = select(func.count()).select_from(model)
        if model is PlanRevision:
            query = query.where(model.plan_id == UUID(h.state["plan_id"]))
        else:
            query = query.where(model.task_id == UUID(h.task.active_task_id))
        return await session.scalar(query)


def next_plan(h):
    step = (
        plan(ExecutionMode.MULTI)
        .steps[0]
        .model_copy(
            update={
                "custom_code": "def next_cell():\n    return 2\nnext_cell()",
            }
        )
    )
    h.state["plan"] = h.state["plan"].model_copy(update={"steps": [step]})
    decision = MultiDecision(
        action=MultiAction.APPEND_STEP,
        rationale="next cell",
        next_step=step,
    )
    h.state["multi_decision"] = decision.model_dump(mode="json")
    return decision


async def test_plan_persistence_reuses_revision_and_rejects_drift(effects):
    h = effects
    persist = h.service.compile_and_persist_plan
    first, second = await asyncio.gather(
        persist(h.state, h.state["plan"]), persist(h.state, h.state["plan"])
    )
    assert first == second
    changed = h.state["plan"].model_copy(update={"objective": "changed"})
    with pytest.raises(ValueError, match="different input"):
        await persist(h.state, changed)
    with pytest.raises(ValueError, match="exactly once"):
        await persist({**h.state, "plan_revision_number": 5}, h.state["plan"])


async def test_submit_response_loss_reuses_body_and_binding(effects):
    h = effects
    h.remote.lose_responses = 3
    with pytest.raises(httpx.ReadTimeout):
        await submitted(h)
    h.remote.version += 2  # Executor has advanced since accepting submission.
    receipt = await h.service.submit_execution(h.state)
    again = await h.service.submit_execution(h.state)
    assert receipt == again
    assert len(h.remote.operations) == 1
    assert len(h.remote.calls) == 4
    assert all(call == h.remote.calls[0] for call in h.remote.calls)
    binding = await h.repository.binding_for_task(UUID(h.task.active_task_id))
    assert binding.execution_id == receipt.execution_id
    assert binding.next_step_sequence == 1  # MULTI submits only first cell.


@pytest.mark.parametrize("after_commit", [False, True])
async def test_submit_projection_crash_reuses_cached_response(
    effects,
    monkeypatch,
    after_commit,
):
    h = effects
    original = h.service.projections.binding

    async def crash(*args, **kwargs):
        if after_commit:
            await original(*args, **kwargs)
        raise RuntimeError("projection crash")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.projections, "binding", crash)
        with pytest.raises(RuntimeError, match="projection crash"):
            await submitted(h)
    receipt = await h.service.submit_execution(h.state)
    assert receipt.execution_id == h.remote.execution_id
    assert len(h.remote.calls) == 1


@pytest.mark.parametrize("after_commit", [False, True])
async def test_append_crash_does_not_advance_sequence_or_plan_twice(
    effects,
    monkeypatch,
    after_commit,
):
    h = effects
    await submitted(h)
    next_plan(h)
    original = h.service.projections.binding

    async def crash(*args, **kwargs):
        if after_commit:
            await original(*args, **kwargs)
        raise RuntimeError("after append response")

    with monkeypatch.context() as patch:
        patch.setattr(h.service.projections, "binding", crash)
        with pytest.raises(RuntimeError, match="after append response"):
            await ExecutionNodes(h.service).append_operation(h.state)
    updates = await ExecutionNodes(h.service).append_operation(h.state)
    assert updates["plan_revision_number"] == 2
    assert updates["plan_revision_id"] != h.state["plan_revision_id"]
    assert await count_rows(h, PlanRevision) == 2
    binding = await h.repository.binding_for_task(UUID(h.task.active_task_id))
    assert binding.next_step_sequence == 2
    assert str(binding.operation_id) == updates["current_operation_id"]
    assert len(h.remote.operations) == 2
    assert len(h.remote.calls) == 2


async def test_append_response_loss_keeps_expected_version(effects):
    h = effects
    await submitted(h)
    decision = next_plan(h)
    h.remote.lose_responses = 3
    with pytest.raises(httpx.ReadTimeout):
        await h.service.append_operation(h.state, decision)
    await h.repository.update_binding(
        UUID(h.task.active_task_id), execution_version=50
    )
    receipt = await h.service.append_operation(h.state, decision)
    assert len(h.remote.operations) == 2
    assert h.remote.calls[-1][1]["expected_version"] == 1
    assert all(call == h.remote.calls[1] for call in h.remote.calls[1:])
    assert receipt.plan is not None and receipt.plan.steps[0].sequence == 0
    binding = await h.repository.binding_for_task(UUID(h.task.active_task_id))
    assert binding.execution_version == 50


async def test_plan_commit_before_prepare_failure_is_recoverable(
    effects,
    monkeypatch,
):
    h = effects
    await submitted(h)
    decision = next_plan(h)
    store = h.service.execution.journal.store
    with monkeypatch.context() as patch:
        patch.setattr(
            store,
            "prepare",
            AsyncMock(side_effect=RuntimeError("prepare unavailable")),
        )
        with pytest.raises(RuntimeError, match="prepare unavailable"):
            await h.service.append_operation(h.state, decision)
    await h.service.append_operation(h.state, decision)
    assert await count_rows(h, PlanRevision) == 2
    assert len(h.remote.calls) == 2


@pytest.mark.parametrize("kind", ["finalize", "cancel"])
async def test_lifecycle_replay_does_not_rebuild_version_or_reason(
    effects,
    kind,
):
    h = effects
    await submitted(h)
    h.remote.lose_responses = 3
    kwargs = {
        "kind": kind,
        "reason": "requested" if kind == "cancel" else None,
    }
    with pytest.raises(httpx.ReadTimeout):
        await h.service.execution.lifecycle(h.state, **kwargs)
    await h.repository.update_binding(
        UUID(h.task.active_task_id), execution_version=90
    )
    await h.service.execution.lifecycle(h.state, **kwargs)
    await h.service.execution.lifecycle(h.state, **kwargs)
    assert all(call == h.remote.calls[1] for call in h.remote.calls[1:])
    assert len(h.remote.calls) == 5
    binding = await h.repository.binding_for_task(UUID(h.task.active_task_id))
    assert binding.execution_version == 90
    if kind == "cancel":
        with pytest.raises(ValueError, match="different input"):
            await h.service.cancel_execution(h.state, "changed reason")


async def test_report_reuses_markdown_after_lost_response_and_missing_file(
    effects,
    tmp_path,
):
    h = effects
    await submitted(h)
    await h.service.finalize_execution(h.state)
    evidence = await h.service.build_report_evidence(h.state)
    h.remote.lose_responses = 3
    with pytest.raises(httpx.ReadTimeout):
        await h.service.generate_and_materialize_report(h.state, evidence)
    request = h.remote.calls[-1][1]
    path = tmp_path / "requests" / request["source"]["path"]
    path.unlink()  # This fixture-owned file models a missing shared input.
    report = await h.service.generate_and_materialize_report(h.state, evidence)
    again = await h.service.generate_and_materialize_report(h.state, evidence)
    assert report == again
    assert report.markdown == "# 최초 결과"
    assert path.read_text() == report.markdown
    assert h.model.i == 1  # A second generation would produce different text.
    assert len(h.remote.receipts) == 3  # submit, finalize, report
    assert h.remote.calls[-1] == h.remote.calls[-2]


@pytest.mark.parametrize("status", ["FAILED", "CANCELLED", "RUNNING"])
async def test_report_rejects_non_success_evidence(effects, status):
    h = effects
    await submitted(h)
    evidence = await h.service.build_report_evidence(h.state)
    evidence["executor_result"]["execution"]["state"]["status"] = status
    with pytest.raises(ValueError, match="success result"):
        await h.service.generate_and_materialize_report(h.state, evidence)
    assert len(h.remote.calls) == 1
    assert h.model.i == 0


async def test_terminal_commit_and_promotion_are_idempotent(effects):
    h = effects
    task_id = UUID(h.task.active_task_id)
    await h.repository.lock_session(task_id)
    terminal = h.service.projections.terminal
    await asyncio.gather(
        *[
            terminal(
                task_id,
                status=TaskStatus.SUCCEEDED,
                message="done",
                metadata={},
            )
            for _ in range(3)
        ]
    )
    assert await count_rows(h, Message) == 2  # user + one assistant
    assert await count_rows(h, TaskEvent) == 2  # accepted + completed
    await asyncio.gather(
        *[h.service.projections.promotion(task_id) for _ in range(3)]
    )
    assert await count_rows(h, TaskEvent) == 3
    with pytest.raises(ValueError, match="cannot change"):
        await terminal(
            task_id, status=TaskStatus.FAILED, message="different", metadata={}
        )


async def test_store_first_prepare_wins_and_detects_tampering(effects):
    h = effects
    store = EffectStore(h.sessions)
    identity = {
        "task_id": UUID(h.task.active_task_id),
        "key": "race:" + str(uuid4()),
        "kind": "report",
        "input_sha256": digest({}),
    }
    first, other = await asyncio.gather(
        store.prepare(**identity, request={"text": "first"}),
        store.prepare(**identity, request={"text": "other"}),
    )
    assert first.request == other.request
    saved = await store.complete(first.key, {"artifact_id": "same"})
    assert saved.response == {"artifact_id": "same"}
    with pytest.raises(ValueError, match="identity changed"):
        await store.complete(first.key, {"artifact_id": "different"})
    async with h.sessions.begin() as session:
        row = await session.get(ExecutorEffect, first.key)
        assert row is not None and row.created_by == row.updated_by == "AGENT"
        assert row.created_at is not None and row.updated_at is not None
        await session.execute(
            update(ExecutorEffect)
            .where(ExecutorEffect.key == first.key)
            .values(request={"text": "tampered"})
        )
    with pytest.raises(ValueError, match="checksum"):
        await store.get(first.key)


async def test_obsolete_binding_receipt_never_rewinds_progress(effects):
    h = effects
    initial = await submitted(h)
    state = deepcopy(h.state)
    decision = next_plan(h)
    appended = await h.service.append_operation(h.state, decision)
    assert appended.operation_id != initial.operation_id
    await h.service.submit_execution(state)
    binding = await h.repository.binding_for_task(UUID(h.task.active_task_id))
    assert binding.operation_id == appended.operation_id
    assert binding.next_step_sequence == 2
    assert len(h.remote.calls) == 2
