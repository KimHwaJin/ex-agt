import os
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import delete

from ex_agent.domain.contracts import PlanDraft, PlanStepDraft
from ex_agent.domain.enums import (
    ExecutionMode,
    PlanningKind,
    TaskStatus,
)
from ex_agent.models import DeterministicHashEmbeddings
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
    transaction,
)
from ex_agent.persistence.models import Workflow, WorkflowVersion
from ex_agent.persistence.repository import (
    AgentRepository,
    ExecutorEventSequenceGapError,
    SessionLockedError,
)

pytestmark = pytest.mark.skipif(
    "TEST_DATABASE_URL" not in os.environ,
    reason="TEST_DATABASE_URL is not configured",
)


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_task_and_durable_events_round_trip() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = create_engine(database_url)
    repository = AgentRepository(create_session_factory(engine))
    task_id = uuid4()
    try:
        task = await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=f"session-{task_id}",
            content="평균을 계산해줘",
            idempotency_key=f"create-{task_id}",
        )
        await repository.update_status(task_id, TaskStatus.CLASSIFYING)
        events = await repository.events_after(task_id, 0)
    finally:
        await engine.dispose()

    assert task.status == TaskStatus.ACCEPTED.value
    assert [event.event_type for event in events] == [
        "task.accepted",
        "task.status_changed",
    ]


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_approval_command_locks_session_atomically() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = create_engine(database_url)
    repository = AgentRepository(create_session_factory(engine))
    task_id = uuid4()
    session_id = f"session-{task_id}"
    try:
        await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=session_id,
            content="분석해줘",
            idempotency_key=f"create-{task_id}",
        )
        await repository.create_resume_command(
            task_id=task_id,
            idempotency_key=f"approve-{task_id}",
            payload={"type": "PLAN_REVIEW", "decision": "APPROVE"},
            lock_session=True,
        )
        with pytest.raises(SessionLockedError):
            await repository.create_task(
                task_id=uuid4(),
                input_message_id=uuid4(),
                user_id="integration-user",
                project_id="integration-project",
                session_id=session_id,
                content="다른 요청",
                idempotency_key=f"blocked-{task_id}",
            )
        await repository.commit_message(
            task_id,
            "완료",
            status=TaskStatus.SUCCEEDED,
        )
        next_task = await repository.create_task(
            task_id=uuid4(),
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=session_id,
            content="다음 요청",
            idempotency_key=f"next-{task_id}",
        )
        assert next_task.session_id == session_id
    finally:
        await engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_failure_compensation_keeps_lock_until_executor_terminal() -> (
    None
):
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = create_engine(database_url)
    repository = AgentRepository(create_session_factory(engine))
    task_id = uuid4()
    session_id = f"session-{task_id}"
    execution_id = uuid4()
    try:
        await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=session_id,
            content="분석해줘",
            idempotency_key=f"create-{task_id}",
        )
        command_id = await repository.create_resume_command(
            task_id=task_id,
            idempotency_key=f"approve-{task_id}",
            payload={"type": "PLAN_REVIEW", "decision": "APPROVE"},
            lock_session=True,
        )
        await repository.bind_execution(
            task_id=task_id,
            execution_id=execution_id,
            operation_id=uuid4(),
            execution_version=1,
            next_step_sequence=1,
        )

        await repository.prepare_failure_compensation(
            command_id,
            task_id,
            "RuntimeError: adaptive planning failed",
        )
        task = await repository.get_task(task_id)
        command = await repository.get_command(command_id)

        assert task is not None
        assert task.status == TaskStatus.CANCEL_REQUESTED.value
        assert command is not None
        assert command.command_type == "FAILURE_COMPENSATION"
        assert command.state == "PENDING"
        with pytest.raises(SessionLockedError):
            await repository.create_task(
                task_id=uuid4(),
                input_message_id=uuid4(),
                user_id="integration-user",
                project_id="integration-project",
                session_id=session_id,
                content="잠금 중 요청",
                idempotency_key=f"blocked-{task_id}",
            )

        await repository.complete_failure_compensation(
            command_id,
            task_id,
            "실행 취소 확인 후 실패",
            failure_message="RuntimeError: adaptive planning failed",
            executor_status="CANCELLED",
        )
        completed = await repository.get_task(task_id)
        completed_command = await repository.get_command(command_id)
        assert completed is not None
        assert completed.status == TaskStatus.FAILED.value
        assert completed_command is not None
        assert completed_command.state == "FAILED"
        next_task = await repository.create_task(
            task_id=uuid4(),
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=session_id,
            content="취소 확인 후 요청",
            idempotency_key=f"next-{task_id}",
        )
        assert next_task.session_id == session_id
    finally:
        await engine.dispose()


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_dummy_embeddings_search_pgvector_consistently() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = create_engine(database_url)
    sessions = create_session_factory(engine)
    repository = AgentRepository(sessions)
    embeddings = DeterministicHashEmbeddings(1024)
    suffix = uuid4().hex
    workflow_ids = []
    plan = PlanDraft(
        objective="샘플 분석",
        strategy_summary="샘플 데이터를 분석한다.",
        execution_mode=ExecutionMode.SINGLE,
        steps=[
            PlanStepDraft(
                sequence=0,
                title="샘플 코드 실행",
                purpose="검색 테스트용 계획",
                planning_kind=PlanningKind.CUSTOM_CODE,
                custom_code=(
                    "def analyze():\n    return 1\n\nresult = analyze()"
                ),
                selection_rationale="검색 테스트",
            )
        ],
    )
    payload = plan.model_dump(mode="json")
    try:
        async with transaction(sessions) as session:
            for index, (name, description) in enumerate(
                [
                    ("월별 매출 분석", "월별 매출 추이와 매출 변화를 분석"),
                    ("고객 이탈 분석", "고객 이탈률과 이탈 원인을 분석"),
                ],
                start=1,
            ):
                workflow = Workflow(
                    name=f"{name}-{suffix}",
                    description=description,
                )
                session.add(workflow)
                await session.flush()
                workflow_ids.append(workflow.id)
                session.add(
                    WorkflowVersion(
                        workflow_id=workflow.id,
                        version=1,
                        plan_payload=payload,
                        public_payload_hash=sha256(
                            f"{suffix}-{index}".encode()
                        ).hexdigest(),
                        embedding=embeddings.embed_query(
                            f"{name} {description}"
                        ),
                        promoted_by="integration-test",
                    )
                )

        candidates = await repository.workflow_candidates(
            embeddings.embed_query(f"월별 매출 추이 분석 {suffix}"),
            limit=2,
        )
    finally:
        async with transaction(sessions) as session:
            await session.execute(
                delete(Workflow).where(Workflow.id.in_(workflow_ids))
            )
        await engine.dispose()

    assert len(candidates) == 2
    assert candidates[0].name.startswith("월별 매출 분석")
    assert candidates[0].score > candidates[1].score


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_executor_event_checkpoint_rejects_sequence_gap() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    engine = create_engine(database_url)
    repository = AgentRepository(create_session_factory(engine))
    task_id = uuid4()
    execution_id = uuid4()
    try:
        await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=f"session-{task_id}",
            content="실행 이벤트 테스트",
            idempotency_key=f"create-{task_id}",
        )
        await repository.bind_execution(
            task_id=task_id,
            execution_id=execution_id,
            operation_id=uuid4(),
            execution_version=1,
            next_step_sequence=1,
        )
        accepted = await repository.record_executor_progress(
            stream_name="executor.events",
            message_id=f"event:{execution_id}:1",
            task_id=task_id,
            event_type="execution.started",
            event_sequence=1,
            payload={"execution_id": str(execution_id)},
        )
        duplicate = await repository.record_executor_progress(
            stream_name="executor.events",
            message_id=f"event:{execution_id}:1",
            task_id=task_id,
            event_type="execution.started",
            event_sequence=1,
            payload={"execution_id": str(execution_id)},
        )

        with pytest.raises(ExecutorEventSequenceGapError):
            await repository.record_executor_progress(
                stream_name="executor.events",
                message_id=f"event:{uuid4()}",
                task_id=task_id,
                event_type="execution.step_started",
                event_sequence=3,
                payload={"execution_id": str(execution_id)},
            )

        binding = await repository.binding_for_task(task_id)
    finally:
        await engine.dispose()

    assert accepted
    assert not duplicate
    assert binding.last_event_sequence == 1
