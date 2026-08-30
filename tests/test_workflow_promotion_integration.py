from __future__ import annotations

import os
from hashlib import sha256
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import delete, select

from ex_agent.application.promotions import WorkflowPromotionService
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    CompiledStep,
    PlanDraft,
    PlanStepDraft,
    WorkflowPromotionRequest,
)
from ex_agent.domain.enums import ExecutionMode, PlanningKind, TaskStatus
from ex_agent.llm.factory import DeterministicHashEmbeddings
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
    transaction,
)
from ex_agent.persistence.models import (
    Task,
    Workflow,
    WorkflowPromotion,
    WorkflowStep,
    WorkflowVersion,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry

pytestmark = pytest.mark.skipif(
    "TEST_DATABASE_URL" not in os.environ,
    reason="Compose PostgreSQL is not configured",
)

_SKILL_ROOT = Path(__file__).parents[1] / "skills"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_postgres_promotion_is_versioned_audited_and_idempotent() -> (
    None
):
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    sessions = create_session_factory(engine)
    repository = AgentRepository(sessions)
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    embeddings = DeterministicHashEmbeddings(1024)
    service = WorkflowPromotionService(
        Settings(agent_skill_root=_SKILL_ROOT),
        repository,
        registry,
        embeddings,
    )
    task_id = uuid4()
    execution_id = uuid4()
    workflow_id = None
    manifest = registry.get_tool("fetch_dataset")
    private_query = "SELECT * FROM private_revenue_source"
    step = PlanStepDraft(
        sequence=0,
        title="Fetch source data",
        purpose="Acquire data for the analysis",
        planning_kind=PlanningKind.TOOL_PLAN,
        skill=manifest.skill,
        tool=manifest.tool,
        parameters={
            "query": private_query,
            "dataset_name": "private-revenue-export",
            "output_format": "parquet",
        },
        selection_rationale="The analysis requires source data",
        expected_outputs=["dataset"],
    )
    plan = PlanDraft(
        objective="매출 데이터를 분석한다",
        strategy_summary="승인된 데이터를 조회하고 품질을 분석한다.",
        execution_mode=ExecutionMode.MULTI,
        steps=[step],
        expected_artifacts=["report"],
    )
    source = "def fetch_dataset():\n    return {}\n\nresult = fetch_dataset()"
    compiled = CompiledStep(
        sequence=0,
        source=source,
        source_sha256=sha256(source.encode()).hexdigest(),
        skill_name=manifest.skill.name,
        tool_name=manifest.tool.name,
        parameters=step.parameters,
    )
    try:
        await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="promotion-owner",
            project_id="promotion-project",
            session_id=f"promotion-session-{task_id}",
            content="private request that must not be published",
            idempotency_key=f"create-{task_id}",
        )
        persisted = await repository.persist_plan(
            task_id,
            plan,
            [(compiled, f"{task_id}/step-0000.py")],
            registry.registry_snapshot_hash(),
            None,
        )
        await repository.bind_execution(
            task_id=task_id,
            execution_id=execution_id,
            operation_id=uuid4(),
            execution_version=1,
            next_step_sequence=1,
        )
        await repository.record_successful_execution_steps(
            task_id=task_id,
            operation_id=uuid4(),
            plan_id=persisted.plan_id,
            plan_revision_id=persisted.plan_revision_id,
            registry_snapshot_hash=registry.registry_snapshot_hash(),
            start_sequence=0,
            steps=[step],
        )
        await repository.update_status(task_id, TaskStatus.SUCCEEDED)
        request = WorkflowPromotionRequest(
            idempotency_key=f"promote-{task_id}",
            name=f"재사용 매출 분석 {task_id}",
            description="승인된 매출 데이터 조회 및 분석 Workflow",
            request_examples=["매출 데이터 분석을 실행해줘"],
            tags=["revenue", "analysis"],
            public_parameter_defaults={"step_0_output_format": "parquet"},
        )

        first = await service.promote(
            task_id,
            actor_user_id="promotion-owner",
            request=request,
        )
        second = await service.promote(
            task_id,
            actor_user_id="promotion-owner",
            request=request,
        )
        workflow_id = first.workflow_id

        assert first.created is True
        assert second.created is False
        assert second.workflow_version_id == first.workflow_version_id
        async with sessions() as session:
            version = await session.get(
                WorkflowVersion,
                first.workflow_version_id,
            )
            stored_steps = (
                await session.scalars(
                    select(WorkflowStep).where(
                        WorkflowStep.workflow_version_id
                        == first.workflow_version_id
                    )
                )
            ).all()
            promotions = (
                await session.scalars(
                    select(WorkflowPromotion).where(
                        WorkflowPromotion.task_id == task_id
                    )
                )
            ).all()
        assert version is not None
        assert version.source_task_id == task_id
        assert version.source_execution_id == execution_id
        assert version.searchable_text is not None
        assert version.input_contract.keys() == {
            "step_0_dataset_name",
            "step_0_query",
        }
        assert private_query not in version.searchable_text
        assert private_query not in str(version.plan_payload)
        assert len(stored_steps) == 1
        assert stored_steps[0].skill_ref["name"] == "data-access"
        assert len(promotions) == 1

        candidates = await repository.workflow_candidates(
            embeddings.embed_query("매출 데이터 분석"),
            limit=10,
        )
        promoted = next(
            item
            for item in candidates
            if item.workflow_version_id == first.workflow_version_id
        )
        assert promoted.input_contract == version.input_contract
    finally:
        async with transaction(sessions) as session:
            await session.execute(
                delete(WorkflowPromotion).where(
                    WorkflowPromotion.task_id == task_id
                )
            )
            if workflow_id is not None:
                await session.execute(
                    delete(Workflow).where(Workflow.id == workflow_id)
                )
            await session.execute(delete(Task).where(Task.id == task_id))
        await engine.dispose()
