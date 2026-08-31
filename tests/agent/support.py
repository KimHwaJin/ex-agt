from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from agent.graph import TaskTurn
from ex_agent.domain.contracts import (
    ExecutorReconciliation,
    IntentDecision,
    MultiDecision,
    PersistedPlan,
    ReportResult,
    RiskReview,
    SubmissionReceipt,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    Intent,
    MultiAction,
    RiskLevel,
)
from tests.test_execution_mode_policy import plan
from worker import EventContext, ExecutorEvent


class LocalGuard:
    """Unit tests only. Integration tests use the real Redis guard."""

    @asynccontextmanager
    async def hold(self, session_id):
        yield


def turn(*, session=None):
    return TaskTurn(
        user_id="test-user",
        project_id="test-project",
        session_id=session or str(uuid4()),
        active_task_id=str(uuid4()),
        current_input_message_id=str(uuid4()),
        user_message="샘플 데이터 분석",
    )


def services(
    *, mode=ExecutionMode.SINGLE, intent=Intent.DATA_ANALYSIS_EXECUTION
):
    service = MagicMock()
    service.update_status = AsyncMock()
    service.classify_intent = AsyncMock(
        return_value=IntentDecision(
            intent=intent,
            confidence=1,
            decision_summary="test decision",
            requested_execution_mode=mode,
        )
    )
    service.answer_question = AsyncMock(return_value="질문에 대한 응답")
    service.commit_answer = AsyncMock()
    risk = RiskReview(
        level=RiskLevel.LOW,
        summary="test safe code",
        recommended_action="ALLOW",
    )
    service.review_request_risk = AsyncMock(return_value=risk)
    service.review_compiled_code_risk = AsyncMock(return_value=risk)
    service.search_workflows = AsyncMock(return_value=[])
    service.build_plan = AsyncMock(return_value=plan(mode))
    service.compile_and_persist_plan = AsyncMock(
        side_effect=lambda state, draft: PersistedPlan(
            plan_id=uuid4(),
            plan_revision_id=uuid4(),
            plan_revision_number=state.get("plan_revision_number", 0) + 1,
            public_payload_hash="a" * 64,
            compiled_bundle_id=uuid4(),
        )
    )
    service.verify_approval_and_lock = AsyncMock()
    service.submit_execution = AsyncMock(
        return_value=SubmissionReceipt(
            execution_id=uuid4(), operation_id=uuid4(), execution_version=1
        )
    )
    service.reconcile_executor = AsyncMock(
        side_effect=lambda state, signal: ExecutorReconciliation(
            outcome=ExecutorOutcome.SUCCEEDED,
            execution_id=signal.execution_id,
            execution_version=signal.event_sequence,
        )
    )
    service.adapt_multi_plan = AsyncMock(
        return_value=MultiDecision(
            action=MultiAction.FINALIZE, rationale="All requested work is done"
        )
    )
    service.append_operation = AsyncMock(
        return_value=SubmissionReceipt(
            execution_id=service.submit_execution.return_value.execution_id,
            operation_id=uuid4(),
            execution_version=3,
        )
    )
    service.finalize_execution = AsyncMock()
    service.cancel_execution = AsyncMock()
    service.build_report_evidence = AsyncMock(return_value={"result": "ok"})
    service.generate_and_materialize_report = AsyncMock(
        return_value=ReportResult(markdown="# 분석 결과", artifact_id=uuid4())
    )
    service.commit_terminal = AsyncMock()
    return service


def event_context(task, execution, *, sequence=1, kind="execution.completed"):
    event = ExecutorEvent(
        event_id=uuid4(),
        execution_id=execution,
        event_type=kind,
        event_sequence=sequence,
        schema_version="1.0",
        occurred_at="2026-08-31T00:00:00Z",
        payload={},
    )
    return EventContext(
        "test",
        task.session_id,
        task.active_task_id,
        event.execution_id,
        uuid4(),
        event,
    )


def boundary(snapshot):
    return next(i for t in snapshot.tasks for i in t.interrupts)


async def review(coordinator, task, snapshot, decision="APPROVE"):
    waiting = boundary(snapshot)
    return await coordinator.resume_user(
        turn=task,
        interrupt_id=waiting.id,
        payload={
            "type": "PLAN_REVIEW",
            "decision": decision,
            "feedback": "보완 요청" if decision == "REVISE" else None,
            **{
                key: waiting.value[key]
                for key in (
                    "plan_revision_id",
                    "plan_revision_number",
                    "public_payload_hash",
                )
            },
        },
    )
