from dataclasses import replace
from unittest.mock import AsyncMock, MagicMock

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agent.graph import build_session_graph, checkpoint_serializer
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from agent.session import SessionConflictError, SessionCoordinator
from ex_agent.domain.contracts import ExecutorReconciliation, MultiDecision
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    Intent,
    MultiAction,
    TaskStatus,
)
from tests.agent.support import (
    LocalGuard,
    boundary,
    event_context,
    review,
    services,
    turn,
)
from tests.test_execution_mode_policy import plan
from worker import DeferEvent, IgnoreEvent


def setup(*, service=None):
    service = service if service is not None else services()
    bindings = MagicMock(register=AsyncMock())
    saver = InMemorySaver(serde=checkpoint_serializer())
    graph = build_session_graph(service, bindings, checkpointer=saver)
    return (
        graph,
        service,
        bindings,
        SessionCoordinator(graph, LocalGuard()),
    )


@pytest.mark.parametrize(
    "intent", [Intent.GENERAL_QA, Intent.DATA_ANALYSIS_QA]
)
async def test_questions_finish_and_keep_session_messages(intent):
    _, service, bindings, host = setup(service=services(intent=intent))
    first = turn()
    result = await host.start(first)
    assert result.values["workflow"]["phase"] == TaskStatus.SUCCEEDED
    second = turn(session=first.session_id)
    result = await host.start(second)
    assert len(result.values["messages"]) == 4
    assert len(result.values["task_requests"]) == 2
    assert result.config["configurable"]["thread_id"] == first.session_id
    assert not result.next
    service.submit_execution.assert_not_called()
    bindings.register.assert_not_called()


async def test_single_approval_execution_receipt_and_report():
    graph, service, bindings, host = setup()
    task = turn()
    result = await host.start(task)
    assert boundary(result).value["kind"] == "PLAN_REVIEW"
    result = await review(host, task, result)
    assert boundary(result).value["kind"] == "EXECUTOR_EVENT"
    bindings.register.assert_awaited_once_with(
        execution_id=service.submit_execution.return_value.execution_id,
        session_id=task.session_id,
        task_id=task.active_task_id,
        actor="agent",
    )
    ctx = event_context(task, result.values["execution_id"])
    adapter = SessionGraphAdapter(graph)
    await adapter(ctx)
    await adapter(ctx)
    after = await graph.aget_state(ctx.graph_config)
    assert not after.next
    assert after.values["workflow"]["phase"] == TaskStatus.SUCCEEDED
    assert after.values["workflow"]["execution_mode"] == "SINGLE"
    assert after.values["messages"][-1].content == "# 분석 결과"
    assert after.values["ew_receipts"][str(ctx.command_id)] == str(
        ctx.event.event_id
    )
    service.generate_and_materialize_report.assert_awaited_once()


async def test_next_task_replaces_all_work_state_and_ignores_late_events():
    graph, service, _, host = setup()
    first = turn()
    await review(host, first, await host.start(first))
    ctx = event_context(
        first, service.submit_execution.return_value.execution_id
    )
    adapter = SessionGraphAdapter(graph)
    await adapter(ctx)
    second = turn(session=first.session_id)
    result = await host.start(second)
    workflow = result.values["workflow"]
    assert "execution_id" not in workflow
    assert "report_markdown" not in workflow
    assert "executor_reconciliation" not in workflow
    assert "external_signal" not in workflow
    assert workflow["plan_revision_number"] == 1
    assert not result.values["ew_pending"]
    assert str(ctx.command_id) in result.values["ew_receipts"]
    await adapter(ctx)
    with pytest.raises(IgnoreEvent):
        await adapter(event_context(first, ctx.execution_id, sequence=2))
    current = await graph.aget_state(ctx.graph_config)
    assert boundary(current).id == boundary(result).id
    assert current.values["active_task_id"] == second.active_task_id


async def test_registration_failure_recovers_without_resubmitting():
    graph, service, bindings, host = setup()
    bindings.register.side_effect = [RuntimeError("DB unavailable"), None]
    task = turn()
    with pytest.raises(RuntimeError, match="DB unavailable"):
        await review(host, task, await host.start(task))
    config = {"configurable": {"thread_id": task.session_id}}
    state = await graph.aget_state(config)
    assert state.next == ("register_execution",)
    ctx = event_context(task, state.values["execution_id"])
    with pytest.raises(DeferEvent):
        await SessionGraphAdapter(graph)(ctx)
    # Future durable API recovery loop owns this pre-event recovery.
    async with host.guard.hold(task.session_id):
        await graph.ainvoke(None, config, durability="sync")
    service.submit_execution.assert_awaited_once()
    assert bindings.register.await_count == 2


async def test_event_acceptance_survives_reconcile_failure_and_restart():
    graph, service, bindings, host = setup()
    task = turn()
    await review(host, task, await host.start(task))
    ctx = event_context(
        task, service.submit_execution.return_value.execution_id
    )
    service.reconcile_executor.side_effect = [
        RuntimeError("REST disconnected"),
        ExecutorReconciliation(
            outcome=ExecutorOutcome.SUCCEEDED,
            execution_id=ctx.execution_id,
            execution_version=2,
        ),
    ]
    with pytest.raises(RuntimeError, match="REST disconnected"):
        await SessionGraphAdapter(graph)(ctx)
    state = await graph.aget_state(ctx.graph_config)
    assert state.next == ("reconcile_executor",)
    assert state.values["ew_pending"]["command_id"] == str(ctx.command_id)
    restarted = build_session_graph(
        service, bindings, checkpointer=graph.checkpointer
    )
    await SessionGraphAdapter(restarted)(ctx)
    await SessionGraphAdapter(restarted)(ctx)
    assert service.reconcile_executor.await_count == 2
    service.generate_and_materialize_report.assert_awaited_once()


async def test_receipt_before_report_failure_recovers_tail_only():
    graph, service, _, host = setup()
    task = turn()
    await review(host, task, await host.start(task))
    ctx = event_context(
        task, service.submit_execution.return_value.execution_id
    )
    report = service.generate_and_materialize_report.return_value
    service.generate_and_materialize_report.side_effect = [
        RuntimeError("report interrupted"),
        report,
    ]
    adapter = SessionGraphAdapter(graph)
    with pytest.raises(RuntimeError, match="report interrupted"):
        await adapter(ctx)
    with pytest.raises(DeferEvent):
        await adapter(event_context(task, ctx.execution_id, sequence=2))
    with pytest.raises(SessionConflictError, match="unfinished"):
        await host.start(turn(session=task.session_id))
    await adapter(ctx)
    await adapter(ctx)
    service.reconcile_executor.assert_awaited_once()
    assert service.generate_and_materialize_report.await_count == 2
    service.commit_terminal.assert_awaited_once()


@pytest.mark.parametrize(
    "outcome", [ExecutorOutcome.FAILED, ExecutorOutcome.CANCELLED]
)
async def test_failure_and_cancel_never_generate_success_report(outcome):
    graph, service, _, host = setup()
    task = turn()
    waiting = await review(host, task, await host.start(task))
    if outcome == ExecutorOutcome.CANCELLED:
        waiting = await host.resume_user(
            turn=task,
            interrupt_id=boundary(waiting).id,
            payload={
                "type": "CANCEL_REQUESTED",
                "task_id": task.active_task_id,
            },
        )
        assert waiting.values["workflow"]["phase"] == "CANCEL_REQUESTED"
        service.commit_terminal.assert_not_called()
    ctx = event_context(task, waiting.values["execution_id"])
    service.reconcile_executor.side_effect = None
    service.reconcile_executor.return_value = ExecutorReconciliation(
        outcome=outcome,
        execution_id=ctx.execution_id,
        execution_version=2,
        error_message="실패 원인",
    )
    await SessionGraphAdapter(graph)(ctx)
    service.generate_and_materialize_report.assert_not_called()
    result = await graph.aget_state(ctx.graph_config)
    assert result.values["workflow"]["phase"] == outcome.value
    assert not result.next


async def test_multi_appends_then_finalizes_and_waits_for_terminal_event():
    graph, service, _, host = setup(service=services(mode=ExecutionMode.MULTI))
    task = turn()
    await review(host, task, await host.start(task))
    execution = service.submit_execution.return_value.execution_id
    service.reconcile_executor.side_effect = [
        ExecutorReconciliation(
            outcome=outcome, execution_id=execution, execution_version=index
        )
        for index, outcome in enumerate(
            [
                ExecutorOutcome.OPERATION_SUCCEEDED,
                ExecutorOutcome.OPERATION_SUCCEEDED,
                ExecutorOutcome.SUCCEEDED,
            ],
            start=1,
        )
    ]
    service.adapt_multi_plan.side_effect = [
        MultiDecision(
            action=MultiAction.APPEND_STEP,
            rationale="one more cell",
            next_step=plan(ExecutionMode.MULTI).steps[0],
        ),
        MultiDecision(action=MultiAction.FINALIZE, rationale="done"),
    ]
    adapter = SessionGraphAdapter(graph)
    for sequence in (1, 2):
        ctx = event_context(
            task,
            execution,
            sequence=sequence,
            kind="execution.operation_completed",
        )
        await adapter(ctx)
        snapshot = await graph.aget_state(ctx.graph_config)
        assert boundary(snapshot).value["kind"] == "EXECUTOR_EVENT"
        await adapter(ctx)
        service.generate_and_materialize_report.assert_not_called()
    service.append_operation.assert_awaited_once()
    service.finalize_execution.assert_awaited_once()
    await adapter(event_context(task, execution, sequence=3))
    service.generate_and_materialize_report.assert_awaited_once()


@pytest.mark.parametrize("decision", ["REVISE", "REJECT"])
async def test_plan_revision_and_rejection_are_preserved(decision):
    _, service, _, host = setup()
    task = turn()
    snapshot = await review(host, task, await host.start(task), decision)
    if decision == "REVISE":
        assert boundary(snapshot).value["plan_revision_number"] == 2
    else:
        assert not snapshot.next
        assert snapshot.values["workflow"]["phase"] == "REJECTED"
    service.submit_execution.assert_not_called()


async def test_worker_cannot_answer_user_approval():
    graph, service, _, host = setup(service=services(mode=ExecutionMode.MULTI))
    task = turn()
    await review(host, task, await host.start(task))
    ctx = event_context(
        task,
        service.submit_execution.return_value.execution_id,
        kind="execution.operation_completed",
    )
    service.reconcile_executor.side_effect = None
    service.reconcile_executor.return_value = ExecutorReconciliation(
        outcome=ExecutorOutcome.OPERATION_SUCCEEDED,
        execution_id=ctx.execution_id,
        execution_version=2,
    )
    service.adapt_multi_plan.return_value = MultiDecision(
        action=MultiAction.REQUIRE_REAPPROVAL,
        rationale="scope changed",
        next_step=plan(ExecutionMode.MULTI).steps[0],
    )
    adapter = SessionGraphAdapter(graph)
    await adapter(ctx)
    before = await graph.aget_state(ctx.graph_config)
    assert boundary(before).value["kind"] == "PLAN_REVIEW"
    await adapter(ctx)
    with pytest.raises(DeferEvent, match="user input"):
        await adapter(event_context(task, ctx.execution_id, sequence=2))
    assert (
        boundary(await graph.aget_state(ctx.graph_config)).id
        == boundary(before).id
    )


async def test_duplicate_and_conflicting_starts_do_not_modify_checkpoint():
    graph, service, _, host = setup()
    task = turn()
    before = await host.start(task)
    again = await host.start(task)
    assert again.config == before.config
    with pytest.raises(SessionConflictError, match="reuse"):
        await host.start(task.model_copy(update={"user_message": "changed"}))
    with pytest.raises(SessionConflictError, match="unfinished"):
        await host.start(turn(session=task.session_id))
    assert (await graph.aget_state(before.config)).config == before.config
    service.classify_intent.assert_awaited_once()


async def test_wrong_session_owner_and_stale_user_resume_are_rejected():
    graph, _, _, host = setup()
    task = turn()
    before = await host.start(task)
    with pytest.raises(SessionConflictError, match="ownership"):
        await host.start(task.model_copy(update={"user_id": "different"}))
    with pytest.raises(SessionConflictError, match="stale"):
        await host.resume_user(
            turn=task,
            interrupt_id="old-interrupt",
            payload={"type": "EXECUTION_MODE", "mode": "MULTI"},
        )
    assert (await graph.aget_state(before.config)).config == before.config


async def test_executor_event_before_start_defers():
    graph, service, _, _ = setup()
    task = turn()
    ctx = event_context(
        task, service.submit_execution.return_value.execution_id
    )
    with pytest.raises(DeferEvent):
        await SessionGraphAdapter(graph)(ctx)


async def test_stale_plan_payload_does_not_poison_the_interrupt():
    graph, _, _, host = setup()
    task = turn()
    before = await host.start(task)
    waiting = boundary(before)
    with pytest.raises(SessionConflictError, match="stale"):
        await host.resume_user(
            turn=task,
            interrupt_id=waiting.id,
            payload={
                "type": "PLAN_REVIEW",
                "decision": "APPROVE",
                "plan_revision_id": waiting.value["plan_revision_id"],
                "plan_revision_number": 999,
                "public_payload_hash": waiting.value["public_payload_hash"],
            },
        )
    assert (await graph.aget_state(before.config)).config == before.config
    result = await review(host, task, before)
    assert boundary(result).value["kind"] == "EXECUTOR_EVENT"


async def test_old_sequence_and_receipt_identity_are_checked():
    graph, service, _, host = setup()
    task = turn()
    await review(host, task, await host.start(task))
    ctx = event_context(
        task, service.submit_execution.return_value.execution_id
    )
    adapter = SessionGraphAdapter(graph)
    await adapter(ctx)
    with pytest.raises(IgnoreEvent):
        await adapter(event_context(task, ctx.execution_id))
    from worker.contracts import RejectEvent

    with pytest.raises(RejectEvent, match="identity"):
        await adapter(
            replace(
                ctx,
                event=ctx.event.model_copy(
                    update={
                        "event_id": event_context(
                            task, ctx.execution_id
                        ).event.event_id
                    }
                ),
            )
        )
